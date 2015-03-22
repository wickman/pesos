from __future__ import print_function

import logging
import os
import sys
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
  def setUp(self):
    self.context = Context()
    self.context.start()
    self.slave = MockSlave()
    self.context.spawn(self.slave)

  def tearDown(self):
    self.context.stop()

  def fake_registration(self):
    command_info = mesos_pb2.CommandInfo(value='wat')
    framework_id = mesos_pb2.FrameworkID(value='fake_framework_id')
    executor_id = mesos_pb2.ExecutorID(value='fake_executor_id')
    executor_info = mesos_pb2.ExecutorInfo(
        executor_id=executor_id,
        framework_id=framework_id,
        command=command_info,
    )
    framework_info = mesos_pb2.FrameworkInfo(user='fake_user', name='fake_framework_name')
    return executor_info, framework_id, framework_info

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

    executor_info, framework_id, framework_info = self.fake_registration()

    self.slave.send_registered(
        driver.executor_process.pid,
        executor_info,
        framework_id,
        framework_info
    )

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    event.wait()
    assert registered_call.mock_calls == [
        mock.call(driver, executor_info, framework_info, self.slave.slave_info)]

    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  def test_mesos_executor_abort_on_disconnection(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)

    event = threading.Event()
    shutdown_call = mock.Mock()
    registered_call = mock.Mock()

    def shutdown_side_effect(*args, **kw):
      shutdown_call(*args, **kw)
      event.set()

    def registered_side_effect(*args, **kw):
      registered_call(*args, **kw)
      event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.registered = mock.Mock(side_effect=registered_side_effect)
    executor.shutdown = mock.Mock(side_effect=shutdown_side_effect)

    driver_context = Context()
    driver_context.start()
    driver = PesosExecutorDriver(executor, context=driver_context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    executor_info, framework_id, framework_info = self.fake_registration()

    self.slave.send_registered(
        driver.executor_process.pid,
        executor_info,
        framework_id,
        framework_info
    )

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    # registered event
    event.wait()
    assert event.is_set()
    event.clear()

    conn = driver_context._connections[self.slave.pid]
    conn.close()

    # abort event
    event.wait()
    assert event.is_set()

    assert shutdown_call.mock_calls == [mock.call(driver)]

  def test_mesos_executor_reregister(self):
    os.environ['MESOS_SLAVE_PID'] = str(self.slave.pid)
    os.environ['MESOS_CHECKPOINT'] = '1'
    os.environ['MESOS_RECOVERY_TIMEOUT'] = '60'

    event = threading.Event()
    reregistered_call = mock.Mock()
    registered_call = mock.Mock()

    def reregistered_side_effect(*args, **kw):
      reregistered_call(*args, **kw)
      event.set()

    def registered_side_effect(*args, **kw):
      registered_call(*args, **kw)
      event.set()

    executor = mock.create_autospec(Executor, spec_set=True)
    executor.registered = mock.Mock(side_effect=registered_side_effect)
    executor.reregistered = mock.Mock(side_effect=reregistered_side_effect)

    driver_context = Context()
    driver_context.start()
    driver = PesosExecutorDriver(executor, context=driver_context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    executor_info, framework_id, framework_info = self.fake_registration()

    self.slave.send_registered(
        driver.executor_process.pid,
        executor_info,
        framework_id,
        framework_info
    )

    driver.executor_process.connected.wait()
    assert driver.executor_process.connected.is_set()

    # registered event
    event.wait()
    assert event.is_set()
    event.clear()

    conn = driver_context._connections[self.slave.pid]
    conn.close()

    self.slave.send_reregistered(driver.executor_process.pid)

    # reregistered event
    event.wait()
    assert event.is_set()

    assert reregistered_call.mock_calls == [mock.call(driver, self.slave.slave_info)]
