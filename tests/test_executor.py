import logging
import os
import threading
import unittest

from pesos.executor import PesosExecutorDriver
from pesos.testing import MockSlave
from pesos.vendor.mesos import mesos_pb2
from pesos.vendor.mesos.internal import messages_pb2 as internal

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
  SLAVE_ID = 'fake_slave_id'

  @staticmethod
  def mock_method():
    event = threading.Event()
    call = mock.Mock()

    def side_effect(*args, **kw):
      call(*args, **kw)
      event.set()

    return event, call, side_effect

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
        mesos_pb2.SlaveID(value=self.SLAVE_ID),
        {self.EXECUTOR_ID: self.executor_info},
        {self.FRAMEWORK_ID: self.framework_info},
    )
    self.context.spawn(self.slave)
    self.close_contexts = []

  def tearDown(self):
    self.context.stop()
    for context in self.close_contexts:
      context.stop()

  # ---- Executor callback tests
  def test_mesos_executor_register(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    registered_event, registered_call, registered_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.registered = mock.Mock(side_effect=registered_side_effect)

    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    registered_event.wait()
    assert registered_call.mock_calls == [
        mock.call(driver, self.executor_info, self.framework_info, self.slave.slave_info)]

    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  def test_mesos_executor_abort_on_disconnection(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    shutdown_event, shutdown_call, shutdown_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.shutdown = mock.Mock(side_effect=shutdown_side_effect)

    driver_context = Context()
    driver_context.start()
    self.close_contexts.append(driver_context)
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

    reregistered_event, reregistered_call, reregistered_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.reregistered = mock.Mock(side_effect=reregistered_side_effect)

    driver_context = Context()
    driver_context.start()
    self.close_contexts.append(driver_context)
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

    launch_task_event, launch_task_call, launch_task_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.launchTask = mock.Mock(side_effect=launch_task_side_effect)

    driver = PesosExecutorDriver(executor, context=self.context)
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
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    kill_task_event, kill_task_call, kill_task_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.killTask = mock.Mock(side_effect=kill_task_side_effect)

    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    # now launch task
    task_id = mesos_pb2.TaskID(value='task-id')
    self.slave.send_kill_task(
        driver.executor_process.pid,
        mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID),
        task_id,
    )

    kill_task_event.wait()
    assert kill_task_call.mock_calls == [mock.call(driver, task_id)]

  def test_mesos_executor_framework_message_delivery(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    framework_message_event, framework_message_call, framework_message_side_effect = (
        self.mock_method())
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.frameworkMessage = mock.Mock(side_effect=framework_message_side_effect)

    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    self.slave.send_framework_message(
        driver.executor_process.pid,
        mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID),
        mesos_pb2.ExecutorID(value=self.EXECUTOR_ID),
        data=b'beep boop beep',
    )

    framework_message_event.wait()
    assert framework_message_call.mock_calls == [mock.call(driver, b'beep boop beep')]

  def test_mesos_executor_shutdown(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    shutdown_event, shutdown_call, shutdown_side_effect = self.mock_method()
    executor = mock.create_autospec(Executor, spec_set=True)
    executor.shutdown = mock.Mock(side_effect=shutdown_side_effect)

    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    self.slave.send_shutdown(driver.executor_process.pid)

    shutdown_event.wait()
    assert shutdown_call.mock_calls == [mock.call(driver)]

  # -- Driver delivery tests
  def test_mesos_executor_driver_init(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING
    assert driver.stop() == mesos_pb2.DRIVER_STOPPED
    assert driver.join() == mesos_pb2.DRIVER_STOPPED

  def test_mesos_executor_driver_abort(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING
    assert driver.abort() == mesos_pb2.DRIVER_ABORTED
    driver.executor_process.aborted.wait()
    assert driver.join() == mesos_pb2.DRIVER_ABORTED

  def test_mesos_executor_driver_run_and_abort(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)

    join_event = threading.Event()
    def runner():
      assert driver.run() == mesos_pb2.DRIVER_ABORTED
      join_event.set()

    threading.Thread(target=runner).start()
    assert not join_event.is_set()
    driver.started.wait(timeout=10)
    assert driver.started.is_set()

    assert driver.abort() == mesos_pb2.DRIVER_ABORTED
    join_event.wait(timeout=10)
    assert join_event.is_set()

  def test_mesos_executor_driver_framework_message(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    # send and wait for framework message
    assert driver.send_framework_message(b'beep boop beep') == mesos_pb2.DRIVER_RUNNING
    self.slave.framework_message_event.wait()

    framework_message = internal.ExecutorToFrameworkMessage(
        slave_id=self.slave.slave_id,
        framework_id=mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID),
        executor_id=mesos_pb2.ExecutorID(value=self.EXECUTOR_ID),
        data=b'beep boop beep',
    )
    assert len(self.slave.framework_messages) == 1
    assert self.slave.framework_messages[0][0] == driver.executor_process.pid
    assert self.slave.framework_messages[0][1] == framework_message, (
        '%s != %s' % (self.slave.framework_messages[0][1], framework_message))

  def test_mesos_executor_driver_status_update(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    executor = Executor()
    driver = PesosExecutorDriver(executor, context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    # wait until registered
    driver.executor_process.connected.wait()

    task_status = mesos_pb2.TaskStatus(
        task_id=mesos_pb2.TaskID(value='task-id'),
        state=mesos_pb2.TASK_FAILED,
    )

    assert driver.send_status_update(task_status) == mesos_pb2.DRIVER_RUNNING
    self.slave.status_update_event.wait()

    assert len(self.slave.status_updates) == 1
    assert self.slave.status_updates[0][0] == driver.executor_process.pid

    status_update_message = self.slave.status_updates[0][1]
    status_update = status_update_message.update
    assert status_update.slave_id == self.slave.slave_id
    assert status_update.framework_id == mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID)
    assert status_update.executor_id == mesos_pb2.ExecutorID(value=self.EXECUTOR_ID)
    assert status_update.status.task_id == task_status.task_id
    assert status_update.status.state == task_status.state
    assert status_update.HasField('timestamp')
    assert status_update.HasField('uuid')
