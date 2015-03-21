import logging
import os

from pesos.executor import PesosExecutorDriver
from pesos.testing import MockSlave
from pesos.vendor.mesos import mesos_pb2

from compactor.context import Context
from mesos.interface import Executor

try:
  from unittest import mock
except ImportError:
  import mock

logging.basicConfig(level=logging.DEBUG)


FAKE_ENVIRONMENT = {
  'MESOS_SLAVE_ID': 'fake_slave_id',
  'MESOS_FRAMEWORK_ID': 'fake_framework_id',
  'MESOS_EXECUTOR_ID': 'fake_executor_id',
  'MESOS_DIRECTORY': 'fake_dir',
  'MESOS_CHECKPOINT': '0',
}


@mock.patch.dict('os.environ', FAKE_ENVIRONMENT)
def test_mesos_executor_driver_init():
  context = Context()
  context.start()

  # spawn slave
  slave = MockSlave()
  context.spawn(slave)

  os.environ['MESOS_SLAVE_PID'] = str(slave.pid)

  executor = Executor()
  driver = PesosExecutorDriver(executor, context=context)
  assert driver.start() == mesos_pb2.DRIVER_RUNNING
  assert driver.stop() == mesos_pb2.DRIVER_STOPPED
  assert driver.join() == mesos_pb2.DRIVER_STOPPED

  context.stop()


@mock.patch.dict('os.environ', FAKE_ENVIRONMENT)
def test_mesos_executor_register():
  context = Context()
  context.start()

  # spawn slave
  slave = MockSlave()
  context.spawn(slave)

  os.environ['MESOS_SLAVE_PID'] = str(slave.pid)

  executor = mock.MagicMock()
  executor.registered = mock.MagicMock()

  driver = PesosExecutorDriver(executor, context=context)
  assert driver.start() == mesos_pb2.DRIVER_RUNNING

  command_info = mesos_pb2.CommandInfo(value='wat')
  framework_id = mesos_pb2.FrameworkID(value='fake_framework_id')
  executor_id = mesos_pb2.ExecutorID(value='fake_executor_id')
  executor_info = mesos_pb2.ExecutorInfo(
    executor_id=executor_id,
    framework_id=framework_id,
    command=command_info
  )
  framework_info = mesos_pb2.FrameworkInfo(user='fake_user', name='fake_framework_name')

  slave.send_registered(
    driver.executor_process.pid,
    executor_info,
    framework_id,
    framework_info
  )

  driver.executor_process.connected.wait()
  assert driver.executor_process.connected.is_set()

  # TODO(wickman) race condition here
  executor.registered.assert_called_with(driver, executor_info, framework_info, slave.slave_info)

  assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  context.stop()
