import socket
import uuid

from .util import unique_suffix
from .vendor import mesos

from compactor.process import ProtobufProcess


def fake_id(prefix=''):
  return prefix + uuid.uuid4().get_hex()


def fake_slave_info(**kw):
  return mesos.SlaveInfo(hostname=socket.gethostname(), **kw)


class MockSlave(ProtobufProcess):
  def __init__(self):
    self.slave_id = mesos.SlaveID(value=fake_id('slave-'))
    self.slave_info = fake_slave_info()
    super(MockSlave, self).__init__(unique_suffix('slave'))

  def send_registered(self, to, executor_info, framework_id, framework_info):
    message = mesos.internal.ExecutorRegisteredMessage(
        executor_info=executor_info,
        framework_id=framework_id,
        framework_info=framework_info,
        slave_id=self.slave_id,
        slave_info=self.slave_info
    )
    self.send(to, message)

  def send_reregistered(self):
    pass

  def send_reconnect(self):
    pass

  def send_run_task(self, to, framework_pid, framework_id, framework_info, task):
    message = mesos.internal.RunTaskMessage(
        framework_id=framework_id,
        framework_info=framework_info,
        task=task,
        pid=framework_pid
    )
    self.send(to, message)

  def send_kill_task(self):
    pass

  def send_status_update_acknowledgement(self):
    pass

  def send_framework_message(self):
    pass

  def send_shutdown(self):
    pass
