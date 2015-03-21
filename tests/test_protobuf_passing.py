import logging
import threading

from pesos.vendor.mesos import mesos_pb2
from pesos.vendor.mesos.internal import messages_pb2 as internal

from compactor.context import Context
from compactor.process import ProtobufProcess

logging.basicConfig(level=logging.DEBUG)


class SimpleReceiver(ProtobufProcess):
  def __init__(self):
    self.receive_event = threading.Event()
    super(SimpleReceiver, self).__init__('receiver')

  @ProtobufProcess.install(internal.UpdateFrameworkMessage)
  def update_framework(self, from_pid, message):
    assert message.framework_id.value == 'simple_framework'
    assert message.pid == 'simple_pid'
    self.receive_event.set()


class SimpleSender(ProtobufProcess):
  def __init__(self):
    super(SimpleSender, self).__init__('sender')

  def send_simple(self, to, framework_id, pid):
    self.send(to, internal.UpdateFrameworkMessage(
        framework_id=mesos_pb2.FrameworkID(value=framework_id), pid=pid))


def test_simple_message_pass():
  context = Context()
  context.start()

  receiver = SimpleReceiver()
  sender = SimpleSender()
  context.spawn(receiver)
  context.spawn(sender)

  sender.send_simple(receiver.pid, 'simple_framework', 'simple_pid')

  receiver.receive_event.wait(timeout=1)
  assert receiver.receive_event.is_set()

  context.stop()
