from collections import defaultdict
import functools
import logging
import threading

from .api import SchedulerDriver
from .util import timed, unique_suffix
from .vendor import mesos

from compactor.context import Context
from compactor.pid import PID
from compactor.process import ProtobufProcess

log = logging.getLogger(__name__)


class SchedulerProcess(ProtobufProcess):
  def __init__(self, driver, scheduler, framework, credential=None, detector=None):
    self.driver = driver
    self.scheduler = scheduler
    self.framework = framework
    self.master = None

    # events
    self.connected = threading.Event()
    self.aborted = threading.Event()
    self.failover = threading.Event()

    # credentials
    self.credential = credential
    self.authenticating = threading.Event()
    self.authenticated = threading.Event()

    # master detection
    self.detector = detector

    # saved state
    self.saved_offers = defaultdict(dict)
    self.saved_slaves = {}

    super(SchedulerProcess, self).__init__(unique_suffix('scheduler'))

  def ignore_if_aborted(method):
    @functools.wraps(method)
    def _wrapper(self, from_pid, *args, **kwargs):
      if self.aborted.is_set():
        log.info('Ignoring message from %s because the scheduler driver is aborted.' % from_pid)
        return
      return method(self, from_pid, *args, **kwargs)
    return _wrapper

  def ignore_if_disconnected(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
      if not self.connected.is_set():
        log.info('Ignoring message from because the scheduler driver is disconnected.')
        return
      return method(self, *args, **kwargs)
    return _wrapper

  def valid_origin(self, from_pid):
    if self.master != from_pid:
      log.warning('Ignoring message from non-leading master %s' % from_pid)
      return False
    return True

  # Invoked externally when a new master is detected.
  def detected(self, master):
    pass

  def register(self):
    pass

  @ProtobufProcess.install(mesos.internal.FrameworkRegisteredMessage)
  @ignore_if_aborted
  def registered(self, from_pid, message):
    if self.connected.is_set():
      log.info('Ignoring registered message as we are already connected.')
      return
    if not self.valid_origin(from_pid):
      return
    self.framework.id = message.framework_id
    self.connected.set()
    self.failover.clear()

    with timed(log.debug, 'scheduler::registered'):
      self.scheduler.registered(self.driver, message.framework_id, message.master_info)

  @ProtobufProcess.install(mesos.internal.FrameworkReregisteredMessage)
  @ignore_if_aborted
  def reregistered(self, from_pid, message):
    if self.connected.is_set():
      log.info('Ignoring registered message as we are already connected.')
      return
    if not self.valid_origin(from_pid):
      return
    assert self.framework.id == message.framework_id
    self.connected.set()
    self.failover.clear()

    with timed(log.debug, 'scheduler::reregistered'):
      self.scheduler.reregistered(self.driver, message.master_info)

  @ProtobufProcess.install(mesos.internal.ResourceOffersMessage)
  @ignore_if_disconnected
  @ignore_if_aborted
  def resource_offers(self, from_pid, message):
    assert self.master is not None
    if not self.valid_origin(from_pid):
      return
    for offer, pid in zip(message.offers, message.pids):
      self.saved_offers[offer.id][offer.slave_id] = PID.from_string(pid)
    with timed(log.debug, 'scheduler::resource_offers'):
      self.scheduler.resource_offers(self.driver, message.offers)

  @ProtobufProcess.install(mesos.internal.RescindResourceOfferMessage)
  @ignore_if_disconnected
  @ignore_if_aborted
  def rescind_offer(self, from_pid, message):
    assert self.master is not None
    if not self.valid_origin(from_pid):
      return
    log.info('Rescinding offer %s' % message.offer_id)
    if not self.saved_offers.pop(message.offer_id, None):
      log.warning('Offer %s not found.' % message.offer_id)
    with timed(log.debug, 'scheduler::offer_rescinded'):
      self.scheduler.offer_rescinded(self.driver, message.offer_id)

  @ProtobufProcess.install(mesos.internal.StatusUpdateMessage)
  @ignore_if_aborted
  def status_update(self, from_pid, message):
    pass

  @ignore_if_aborted
  def status_update_acknowledgement(self, update, pid):
    message = mesos.internal.StatusUpdateAcknowledgementMessage(
        framework_id=self.framework.id,
        slave_id=update.slave_id,
        task_id=update.status.task_id,
        uuid=update.uuid,
    )
    self.send(pid, message)

  @ProtobufProcess.install(mesos.internal.LostSlaveMessage)
  @ignore_if_disconnected
  @ignore_if_aborted
  def lost_slave(self, from_pid, message):
    assert self.master is not None
    if not self.valid_origin(from_pid):
      return
    self.slave_pids.pop(message.slave_id)
    with timed(log.debug, 'scheduler::slave_lost'):
      self.scheduler.slave_lost(self.driver, message.slave_id)

  @ProtobufProcess.install(mesos.internal.ExecutorToFrameworkMessage)
  @ignore_if_aborted
  def framework_message(self, from_pid, message):
    with timed(log.debug, 'scheduler::framework_message'):
      self.scheduler.framework_message(self.driver,
          message.executor_id, message.slave_id, message.data)

  @ProtobufProcess.install(mesos.internal.FrameworkErrorMessage)
  @ignore_if_aborted
  def error(self, from_pid, message):
    with timed(log.debug, 'scheduler::error'):
      self.scheduler.error(self.driver, message.message)

  def stop(self, failover=False):
    pass

  def abort(self):
    pass

  @ignore_if_disconnected
  def kill_task(self, task_id):
    assert self.master is not None
    message = mesos.internal.KillTaskMessage(framework_id=self.framework.id, task_id=task_id)
    self.send(self.master, message)

  @ignore_if_disconnected
  def request_resources(self, requests):
    assert self.master is not None
    message = mesos.internal.ResourceRequestMessage(
        framework_id=self.framework.id,
        requests=requests)
    self.send(self.master, message)

  def launch_tasks(self, offer_ids, tasks, filters):
    pass

  @ignore_if_disconnected
  def revive_offers(self):
    assert self.master is not None
    message = mesos.internal.ReviveOffersMessage(framework_id=self.framework.id)
    self.send(self.master, message)

  @ignore_if_disconnected
  def send_framework_message(self, executor_id, slave_id, data):
    pass

  @ignore_if_disconnected
  def reconcile_tasks(self, statuses):
    assert self.master is not None
    message = mesos.internal.ReviveOffersMessage(framework_id=self.framework.id, statuses=statuses)
    self.send(self.master, message)

  del ignore_if_aborted


class MesosSchedulerDriver(SchedulerDriver):
  def __init__(self, scheduler, framework, master_uri, credential=None, context=None):
    self.context = context or Context()
    self.scheduler = scheduler
    self.scheduler_process = None
    self.master_uri = master_uri
    self.framework = framework
    self.lock = threading.Condition()
    self.status = mesos.DRIVER_NOT_STARTED
    self.detector = None
    self.credential = credential

  def locked(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kw):
      with self.lock:
        return method(self, *args, **kw)
    return _wrapper

  def _initialize_detector(self):
    pass

  @locked
  def start(self):
    if self.status is not mesos.DRIVER_NOT_STARTED:
      return self.status

    if self.detector is None:
      self.detector = self._initialize_detector()

    assert self.scheduler_process is None
    self.scheduler_process = SchedulerProcess(
        self,
        self.scheduler,
        self.framework,
        self.credential,
        self.detector,
    )
    self.context.spawn(self.scheduler_process)
    self.status = mesos.DRIVER_RUNNING
    return self.status

  @locked
  def stop(self, failover=False):
    if self.status not in (mesos.DRIVER_RUNNING, mesos.DRIVER_ABORTED):
      return self.status

    if self.scheduler_process is not None:
      self.context.dispatch(self.scheduler_process.pid, 'stop', failover)

    aborted = self.status == mesos.DRIVER_ABORTED
    self.status = mesos.DRIVER_STOPPED
    return mesos.DRIVER_ABORTED if aborted else self.status

  @locked
  def abort(self):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status

    assert self.scheduler_process is not None
    self.scheduler_process.aborted.set()
    self.context.dispatch(self.scheduler_process.pid, 'abort')
    self.status = mesos.DRIVER_ABORTED
    return self.status

  @locked
  def join(self):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status

    while self.status is mesos.DRIVER_RUNNING:
      self.lock.wait()

    assert self.status in (mesos.DRIVER_ABORTED, mesos.DRIVER_STOPPED)
    return self.status

  @locked
  def run(self):
    self.status = self.start()
    return self.status if self.status is not mesos.DRIVER_RUNNING else self.join()

  @locked
  def request_resources(self, requests):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'request_resources', requests)
    return self.status

  @locked
  def launch_tasks(self, offer_ids, tasks, filters=None):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'launch_tasks', offer_ids, tasks, filters)
    return self.status

  @locked
  def kill_task(self, task_id):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'kill_task', task_id)
    return self.status

  def decline_offer(self, offer_id, filters=None):
    return self.launch_tasks([offer_id], [], filters)

  @locked
  def revive_offers(self):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'revive_offers')
    return self.status

  @locked
  def send_framework_message(self, executor_id, slave_id, data):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'send_framework_message',
        executor_id, slave_id, data)
    return self.status

  @locked
  def reconcile_tasks(self, statuses):
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    assert self.scheduler_process is not None
    self.context.dispatch(self.scheduler_process.pid, 'reconcile_tasks', statuses)
    return self.status
