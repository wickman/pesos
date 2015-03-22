import logging
import os
import threading
import unittest

from pesos.executor import PesosExecutorDriver
from pesos.testing import MockSlave
from pesos.vendor.mesos import mesos_pb2

from compactor.context import Context
from mesos.interface import Executor

try:
  from unittest import mock
except ImportError:
  import mock

FORMAT = "%(levelname)s:%(asctime)s.%(msecs)03d %(threadName)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')


FAKE_ENVIRONMENT = {
  'MESOS_SLAVE_ID': 'fake_slave_id',
  'MESOS_FRAMEWORK_ID': 'fake_framework_id',
  'MESOS_EXECUTOR_ID': 'fake_executor_id',
  'MESOS_DIRECTORY': 'fake_dir',
  'MESOS_CHECKPOINT': '0',
}


@mock.patch.dict('os.environ', FAKE_ENVIRONMENT)
class TestExecutor(unittest.TestCase):
  FRAMEWORK_ID = 'fake_framework_id'
  EXECUTOR_ID = 'fake_executor_id'

  def setUp(self):
    command_info = mesos_pb2.CommandInfo(value='wat')
    framework_id = mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID)
    executor_id = mesos_pb2.ExecutorID(value=self.EXECUTOR_ID)
    self.executor_info = mesos_pb2.ExecutorInfo(
        executor_id=executor_id,
        framework_id=framework_id,
        command=command_info,
    )
    self.framework_info = mesos_pb2.FrameworkInfo(
        user='fake_user',
        name='fake_framework_name',
    )
    self.context = Context()
    self.context.start()
    self.slave = MockSlave(
        {self.EXECUTOR_ID: self.executor_info},
        {self.FRAMEWORK_ID: self.framework_info},
    )
    self.context.spawn(self.slave)

  def tearDown(self):
    self.context.stop()

  def test_mesos_executor_driver_init(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING
    assert driver.stop() == mesos_pb2.DRIVER_STOPPED
    assert driver.join() == mesos_pb2.DRIVER_STOPPED

  def test_mesos_executor_register(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    event = threading.Event()
    registered_call = mock.Mock()

    def side_effect_event(*args, **kw):
      registered_call(*args, **kw)
      event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.registered = mock.Mock(side_effect=side_effect_event)

    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    event.wait()
    assert registered_call.mock_calls == [
        mock.call(driver, self.executor_info, self.framework_info, self.slave.slave_info)]

    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  def test_mesos_executor_abort_on_disconnection(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    shutdown_event = threading.Event()
    shutdown_call = mock.Mock()

    def shutdown_side_effect(*args, **kw):
      shutdown_call(*args, **kw)
      shutdown_event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.shutdown = mock.Mock(side_effect=shutdown_side_effect)

    driver_context = Context()
    driver_context.start()
    driver = PesosExecutorDriver(executor, context=driver_context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()
    self.slave.register_event.wait()

    # kill connection
    conn = driver_context._connections[self.slave.pid]
    conn.close()

    # abort event
    shutdown_event.wait()
    assert shutdown_event.is_set()

    assert shutdown_call.mock_calls == [mock.call(driver)]

  def test_mesos_executor_reregister(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)
    os.environ['MESOS_CHECKPOINT'] = '1'
    os.environ['MESOS_RECOVERY_TIMEOUT'] = '60'

    reregistered_event = threading.Event()
    reregistered_call = mock.Mock()

    def reregistered_side_effect(*args, **kw):
      reregistered_call(*args, **kw)
      reregistered_event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.reregistered = mock.Mock(side_effect=reregistered_side_effect)

    driver_context = Context()
    driver_context.start()
    driver = PesosExecutorDriver(executor, context=driver_context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    self.slave.register_event.wait()
    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    # kill connection, but expect reconnect
    conn = driver_context._connections[self.slave.pid]
    conn.close()

    # simulate slave reconnecting
    self.slave.send_reconnect(driver.executor_process.pid)

    # wait for reregister event
    self.slave.reregister_event.wait()
    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    # reregistered event
    reregistered_event.wait()
    assert reregistered_event.is_set()

    assert reregistered_call.mock_calls == [mock.call(driver, self.slave.slave_info)]

  def test_mesos_executor_run_task(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    launch_task_event = threading.Event()
    launch_task_call = mock.Mock()

    def launch_task_side_effect(*args, **kw):
      launch_task_call(*args, **kw)
      launch_task_event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.launchTask = mock.Mock(side_effect=launch_task_side_effect)

    driver_context = Context()
    driver_context.start()
    driver = PesosExecutorDriver(executor, context=driver_context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    # now launch task
    task_info = mesos_pb2.TaskInfo(
        name='task',
        task_id=mesos_pb2.TaskID(value='task-id'),
        slave_id=self.slave.slave_id,
        executor=self.executor_info,
    )
    self.slave.send_run_task(
        driver.executor_process.pid,
        mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID),
        task_info,
    )

    launch_task_event.wait()
    assert launch_task_call.mock_calls == [mock.call(driver, task_info)]
    assert driver.executor_process.tasks == {task_info.task_id.value: task_info}

  def test_mesos_executor_kill_task(self):
    pass

  def test_mesos_executor_framework_message_delivery(self):
    pass

