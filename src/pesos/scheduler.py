from .api import SchedulerDriver
from .util import unique_suffix
from .vendor import mesos

from compactor.context import Context
from compactor.process import ProtobufProcess


class SchedulerProcess(ProtobufProcess):
  def __init__(self, driver, scheduler, framework, credential=None, detector=None):
    self.driver = driver
    self.scheduler = scheduler
    self.framework = framework

    # events
    self.connected = threading.Event()
    self.aborted = threading.Event()

    # credentials
    self.credential = credential
    self.authenticating = threading.Event()
    self.authenticated = threading.Event()

    # master detection
    self.detector = detector

    super(SchedulerProcess, self).__init__(unique_suffix('scheduler'))

  @ProtobufProcess.install(mesos.internal.FrameworkRegisteredMessage)
  def registered(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.FrameworkReregisteredMessage)
  def reregistered(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.ResourceOffersMessage)
  def resource_offers(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.RescindResourceOfferMessage)
  def rescind_offer(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.StatusUpdateMessage)
  def status_update(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.LostSlaveMessage)
  def lost_slave(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.ExecutorToFrameworkMessage)
  def framework_message(self, from_pid, message):
    pass

  @ProtobufProcess.install(mesos.internal.FrameworkErrorMessage)
  def error(self, from_pid, message):
    pass


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

    aborted = status == mesos.DRIVER_ABORTED
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
    if self.status is not mesos.DRIVER_RUNNING:
      return self.status
    else:
      return self.join()

  @locked
  def request_resources(self, requests):
    pass

  @locked
  def launch_tasks(self, offer_ids, tasks, filters=None):
    pass

  @locked
  def kill_task(self, task_id):
    pass

  @locked
  def decline_offer(self, offer_id, filters=None):
    pass

  @locked
  def revive_offers(self):
    pass

  @locked
  def send_framework_message(self, executor_id, slave_id, data):
    pass

  @locked
  def reconcile_tasks(self, statuses):
    pass

