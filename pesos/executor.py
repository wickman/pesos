import functools
import logging
import os
import threading
import time
import uuid

from .vendor.mesos import mesos_pb2
from .vendor.mesos.internal import messages_pb2 as internal
from .util import camel_call, timed, unique_suffix

from compactor.context import Context
from compactor.pid import PID
from compactor.process import Process, ProtobufProcess
from mesos.interface import ExecutorDriver

log = logging.getLogger(__name__)


class ExecutorProcess(ProtobufProcess):

  class Error(Exception):
    pass

  def __init__(self,
               slave_pid,
               driver,
               executor,
               slave_id,
               framework_id,
               executor_id,
               directory,
               checkpoint,
               recovery_timeout):

    self.slave = slave_pid
    self.driver = driver
    self.executor = executor

    self.slave_id = slave_id
    self.framework_id = framework_id
    self.executor_id = executor_id

    self.aborted = threading.Event()
    self.stopped = threading.Event()
    self.connected = threading.Event()

    self.directory = directory
    self.checkpoint = checkpoint
    self.recovery_timeout = recovery_timeout

    self.updates = {}  # unacknowledged updates
    self.tasks = {}  # unacknowledged tasks

    super(ExecutorProcess, self).__init__(unique_suffix('executor'))

  def ignore_if_aborted(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kwargs):
      if self.aborted.is_set():
        log.info('Ignoring message from slave %s because the driver is aborted.' % self.slave_id)
        return
      return method(self, *args, **kwargs)
    return _wrapper

  def initialize(self):
    super(ExecutorProcess, self).initialize()

    self.link(self.slave)

    log.info('Registering executor with slave %s' % self.slave)
    message = internal.RegisterExecutorMessage()
    message.framework_id.value = self.framework_id
    message.executor_id.value = self.executor_id
    self.send(self.slave, message)

  @ProtobufProcess.install(internal.ExecutorRegisteredMessage)
  @ignore_if_aborted
  def registered(self, from_pid, message):
    log.info('Executor registered on slave %s' % self.slave_id)
    self.connected.set()
    self.connection = uuid.uuid4()

    with timed(log.debug, 'executor::registered'):
      camel_call(self.executor, 'registered',
          self.driver,
          message.executor_info,
          message.framework_info,
          message.slave_info
      )

  @ProtobufProcess.install(internal.ExecutorReregisteredMessage)
  @ignore_if_aborted
  def reregistered(self, from_pid, message):
    # TODO(wickman) Should we be validating that self.slave_id == message.slave_id?
    log.info('Executor re-registered on slave %s' % self.slave_id)
    self.connected.set()
    self.connection = uuid.uuid4()

    with timed(log.debug, 'executor::reregistered'):
      camel_call(self.executor, 'reregistered', self.driver, message.slave_info)

  @ProtobufProcess.install(internal.ReconnectExecutorMessage)
  @ignore_if_aborted
  def reconnect(self, from_pid, message):
    log.info('Received reconnect request from slave %s' % message.slave_id)
    self.slave = from_pid
    self.link(from_pid)

    reregister_message = internal.ReregisterExecutorMessage(
        executor_id=mesos_pb2.ExecutorID(value=self.executor_id),
        framework_id=mesos_pb2.FrameworkID(value=self.framework_id),
    )
    reregister_message.updates.extend(self.updates.values())
    reregister_message.tasks.extend(self.tasks.values())
    self.send(self.slave, reregister_message)

  @ProtobufProcess.install(internal.RunTaskMessage)
  @ignore_if_aborted
  def run_task(self, from_pid, message):
    task = message.task

    if task.task_id.value in self.tasks:
      raise self.Error('Unexpected duplicate task %s' % task)

    self.tasks[task.task_id.value] = task

    with timed(log.debug, 'executor::launch_task'):
      camel_call(self.executor, 'launch_task', self.driver, task)

  @ProtobufProcess.install(internal.KillTaskMessage)
  @ignore_if_aborted
  def kill_task(self, from_pid, message):
    log.info('Executor asked to kill task %s' % message.task_id)

    with timed(log.debug, 'executor::kill_task'):
      camel_call(self.executor, 'kill_task', self.driver, message.task_id)

  @ProtobufProcess.install(internal.StatusUpdateAcknowledgementMessage)
  @ignore_if_aborted
  def status_update_acknowledgement(self, from_pid, message):
    ack_uuid = uuid.UUID(bytes=message.uuid)

    log.info(
      'Executor received status update acknowledgement %s for task %s of framework %s',
      ack_uuid,
      message.task_id.value,
      message.framework_id.value
    )

    if not self.updates.pop(ack_uuid, None):
      log.error('Unknown status update %s!' % ack_uuid)

    if not self.tasks.pop(message.task_id.value, None):
      log.error('Unknown task %s!' % message.task_id.value)

  @ProtobufProcess.install(internal.FrameworkToExecutorMessage)
  @ignore_if_aborted
  def framework_message(self, from_pid, message):
    log.info('Executor received framework message')

    with timed(log.debug, 'executor::framework_message'):
      camel_call(self.executor, 'framework_message', self.driver, message.data)

  @ProtobufProcess.install(internal.ShutdownExecutorMessage)
  @ignore_if_aborted
  def shutdown(self, from_pid, message):
    with timed(log.debug, 'executor::shutdown'):
      camel_call(self.executor, 'shutdown', self.driver)

    self.stop()

  @ignore_if_aborted
  def stop(self):
    self.stopped.set()
    self.terminate()
    # Shutdown?

  @ignore_if_aborted
  def abort(self):
    self.connected.clear()
    self.aborted.set()

  @ignore_if_aborted
  def exited(self, pid):
    if self.checkpoint and self.connected.is_set():
      self.connected.clear()
      log.info('Slave exited but framework has checkpointing enabled.')
      log.info('Waiting %s to reconnect with %s' % (self.recovery_timeout, self.slave_id))
      self.context.delay(self.recovery_timeout, self.pid, '_recovery_timeout', self.connection)
      return

    self._abort()

  def _recovery_timeout(self, connection):
    if self.connected.is_set():
      return

    if self.connection == connection:
      log.info('Recovery timeout exceeded, shutting down.')
      self._abort()

  def _abort(self):
    with timed(log.debug, 'executor::shutdown'):
      camel_call(self.executor, 'shutdown', self.driver)

    log.info('Slave exited. Aborting.')
    self.abort()

  @ignore_if_aborted
  def send_status_update(self, status):
    if status.state is mesos_pb2.TASK_STAGING:
      log.error('Executor is not allowed to send TASK_STAGING, aborting!')
      self.driver.abort()
      with timed(log.debug, 'executor::error'):
        camel_call(self.executor, 'error', self.driver,
            'Attempted to send TASK_STAGING status update.')
      return

    update = internal.StatusUpdate(
        status=status,
        timestamp=time.time(),
        uuid=uuid.uuid4().get_bytes(),
    )

    update.framework_id.value = self.framework_id
    update.executor_id.value = self.executor_id
    update.slave_id.value = self.slave_id
    update.status.timestamp = update.timestamp
    update.status.slave_id.value = self.slave_id

    message = internal.StatusUpdateMessage(
        update=update,
        pid=str(self.pid)
    )

    self.updates[update.uuid] = update
    self.send(self.slave, message)

  @ignore_if_aborted
  def send_framework_message(self, data):
    message = internal.ExecutorToFrameworkMessage()
    message.slave_id.value = self.slave_id
    message.framework_id.value = self.framework_id
    message.executor_id.value = self.executor_id
    message.data = data
    self.send(self.slave, message)

  del ignore_if_aborted


class PesosExecutorDriver(ExecutorDriver):
  @classmethod
  def get_env(cls, key):
    try:
      return os.environ[key]
    except KeyError:
      raise RuntimeError('%s must be specified in order for the driver to work!' % key)

  @classmethod
  def get_bool(cls, key):
    if key not in os.environ:
      return False
    return os.environ[key] == "1"

  def __init__(self, executor, context=None):
    self.context = context or Context.singleton()
    self.executor = executor
    self.executor_process = None
    self.executor_pid = None
    self.started = threading.Event()
    self.status = mesos_pb2.DRIVER_NOT_STARTED
    self.lock = threading.Condition()

  def locked(method):
    @functools.wraps(method)
    def _wrapper(self, *args, **kw):
      with self.lock:
        return method(self, *args, **kw)
    return _wrapper

  @locked
  def start(self):
    log.info('MesosExecutorDriver.start called')

    slave_pid = PID.from_string(self.get_env('MESOS_SLAVE_PID'))
    slave_id = self.get_env('MESOS_SLAVE_ID')
    framework_id = self.get_env('MESOS_FRAMEWORK_ID')
    executor_id = self.get_env('MESOS_EXECUTOR_ID')
    directory = self.get_env('MESOS_DIRECTORY')
    checkpoint = self.get_bool('MESOS_CHECKPOINT')
    recovery_timeout_secs = 15 * 60  # 15 minutes

    if checkpoint:
      # TODO(wickman) Implement Duration. Instead take seconds for now
      try:
        recovery_timeout_secs = int(self.get_env('MESOS_RECOVERY_TIMEOUT'))
      except ValueError:
        raise RuntimeError('MESOS_RECOVERY_TIMEOUT must be in seconds.')

    assert self.executor_process is None
    self.executor_process = ExecutorProcess(
        slave_pid,
        self,
        self.executor,
        slave_id,
        framework_id,
        executor_id,
        directory,
        checkpoint,
        recovery_timeout_secs,
    )

    self.context.spawn(self.executor_process)

    log.info("Started driver")

    self.status = mesos_pb2.DRIVER_RUNNING
    self.started.set()
    return self.status

  @locked
  def stop(self):
    if self.status not in (mesos_pb2.DRIVER_RUNNING, mesos_pb2.DRIVER_ABORTED):
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_process.pid, 'stop')
    aborted = self.status == mesos_pb2.DRIVER_ABORTED
    self.status = mesos_pb2.DRIVER_STOPPED
    self.lock.notify()
    return mesos_pb2.DRIVER_ABORTED if aborted else self.status

  @locked
  def abort(self):
    if self.status is not mesos_pb2.DRIVER_RUNNING:
      return self.status
    # TODO(wickman) Why do we set this first?
    self.executor_process.aborted.set()
    self.context.dispatch(self.executor_process.pid, 'abort')
    self.status = mesos_pb2.DRIVER_ABORTED
    self.lock.notify()
    return self.status

  @locked
  def join(self):
    if self.status is not mesos_pb2.DRIVER_RUNNING:
      return self.status

    while self.status is mesos_pb2.DRIVER_RUNNING:
      self.lock.wait()  # Wait until the driver notifies us to break

    log.info("Executor driver finished with status %d", self.status)
    assert self.status in (mesos_pb2.DRIVER_ABORTED, mesos_pb2.DRIVER_STOPPED)
    return self.status

  @locked
  def run(self):
    self.status = self.start()
    return self.status if self.status is not mesos_pb2.DRIVER_RUNNING else self.join()

  @locked
  def sendStatusUpdate(self, status):
    if self.status is not mesos_pb2.DRIVER_RUNNING:
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_process.pid, 'send_status_update', status)
    return self.status

  @locked
  def sendFrameworkMessage(self, data):
    if self.status is not mesos_pb2.DRIVER_RUNNING:
      return self.status
    assert self.executor_process is not None
    self.context.dispatch(self.executor_process.pid, 'send_framework_message', data)
    return self.status

  del locked

  send_status_update = sendStatusUpdate
  send_framework_message = sendFrameworkMessage
