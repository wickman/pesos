import socket
import threading
import uuid

from .util import unique_suffix
from .vendor.mesos import mesos_pb2 as mesos
from .vendor.mesos.internal import messages_pb2 as internal

from compactor.pid import PID
from compactor.process import ProtobufProcess


def fake_id(prefix=''):
  return prefix + uuid.uuid4().get_hex()


def fake_slave_info(**kw):
  return mesos.SlaveInfo(hostname=socket.gethostname(), **kw)


class MockSlave(ProtobufProcess):
  def __init__(self, slave_id, executor_map, framework_map):
    self._executor_map = executor_map   # executor_id => executor_info
    self._framework_map = framework_map   # framework_id => framework_info

    self.slave_id = slave_id
    self.slave_info = fake_slave_info()
    self.reregister_event = threading.Event()
    self.register_event = threading.Event()
    self.status_updates = []
    self.status_update_event = threading.Event()
    self.framework_messages = []
    self.framework_message_event = threading.Event()

    super(MockSlave, self).__init__(unique_suffix('slave'))

  @ProtobufProcess.install(internal.RegisterExecutorMessage)
  def recv_register_executor(self, from_pid, message):
    framework_info = self._framework_map[message.framework_id.value]
    executor_info = self._executor_map[message.executor_id.value]
    self.send_registered(from_pid, executor_info, message.framework_id, framework_info)
    self.register_event.set()

  @ProtobufProcess.install(internal.ReregisterExecutorMessage)
  def recv_reregister_executor(self, from_pid, message):
    self.send_reregistered(from_pid)
    self.reregister_event.set()

  @ProtobufProcess.install(internal.StatusUpdateMessage)
  def recv_status_update(self, from_pid, message):
    self.status_updates.append((from_pid, message))
    self.status_update_event.set()

  @ProtobufProcess.install(internal.ExecutorToFrameworkMessage)
  def recv_framework_message(self, from_pid, message):
    self.framework_messages.append((from_pid, message))
    self.framework_message_event.set()

  def send_registered(self, to, executor_info, framework_id, framework_info):
    message = internal.ExecutorRegisteredMessage(
        executor_info=executor_info,
        framework_id=framework_id,
        framework_info=framework_info,
        slave_id=self.slave_id,
        slave_info=self.slave_info
    )
    self.send(to, message)

  def send_reregistered(self, to):
    message = internal.ExecutorReregisteredMessage(
        slave_id=self.slave_id,
        slave_info=self.slave_info,
    )
    self.send(to, message)

  def send_reconnect(self, to):
    message = internal.ReconnectExecutorMessage(slave_id=self.slave_id)
    self.send(to, message)

  def send_run_task(self, to, framework_id, task):
    message = internal.RunTaskMessage(
        framework_id=framework_id,
        framework=self._framework_map[framework_id.value],
        task=task,
        # this appears to be no longer used though it is a required field.
        pid=str(PID('127.0.0.1', 31337, 'not_used(123)')),
    )
    self.send(to, message)

  def send_kill_task(self, to, framework_id, task_id):
    message = internal.KillTaskMessage(
        framework_id=framework_id,
        task_id=task_id,
    )
    self.send(to, message)

  def send_status_update_acknowledgement(self, to, framework_id, task_id, update_uuid):
    message = internal.StatusUpdateAcknowledgementMessage(
        slave_id=self.slave_id,
        framework_id=framework_id,
        task_id=task_id,
        uuid=update_uuid,
    )
    self.send(to, message)

  def send_framework_message(self, to, framework_id, executor_id, data):
    message = internal.FrameworkToExecutorMessage(
        slave_id=self.slave_id,
        framework_id=framework_id,
        executor_id=executor_id,
        data=data,
    )
    self.send(to, message)

  def send_shutdown(self, to):
    message = internal.ShutdownExecutorMessage()
    self.send(to, message)
