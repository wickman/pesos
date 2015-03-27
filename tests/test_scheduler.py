import threading
import unittest

from pesos.detector import StandaloneMasterDetector, pid_to_master_info
from pesos.scheduler import PesosSchedulerDriver, SchedulerProcess
from pesos.testing import MockMaster
from pesos.vendor.mesos import mesos_pb2

from compactor.context import Context
from mesos.interface import Scheduler
import mock


MAX_TIMEOUT = 10


class TestScheduler(unittest.TestCase):
  FRAMEWORK_ID = 'fake_framework_id'

  @staticmethod
  def mock_method():
    event = threading.Event()
    call = mock.Mock()

    def side_effect(*args, **kw):
      call(*args, **kw)
      event.set()

    return event, call, side_effect

  def setUp(self):
    self.framework_id = mesos_pb2.FrameworkID(value=self.FRAMEWORK_ID)
    self.framework_info = mesos_pb2.FrameworkInfo(
        user='fake_user',
        name='fake_framework_name',
    )
    self.context = Context()
    self.context.start()
    self.master = MockMaster()
    self.context.spawn(self.master)
    self.close_contexts = []

  def tearDown(self):
    self.context.stop()
    for context in self.close_contexts:
      context.stop()

  def test_scheduler_registration(self):
    registered_event, registered_call, registered_side_effect = self.mock_method()
    scheduler = mock.create_autospec(Scheduler, spec_set=True)
    scheduler.registered = mock.Mock(side_effect=registered_side_effect)

    driver = PesosSchedulerDriver(
        scheduler, self.framework_info, str(self.master.pid), context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.scheduler_process.connected.wait(timeout=MAX_TIMEOUT)
    assert driver.scheduler_process.connected.is_set()

    registered_event.wait(timeout=MAX_TIMEOUT)
    assert registered_event.is_set()

    assert len(self.master.frameworks) == 1

    framework_id = next(iter(self.master.frameworks))
    assert registered_call.mock_calls == [mock.call(
        driver,
        mesos_pb2.FrameworkID(value=framework_id),
        pid_to_master_info(self.master.pid))]

    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  def test_scheduler_scheduler_failover(self):
    reregistered_event, reregistered_call, reregistered_side_effect = self.mock_method()
    scheduler = mock.create_autospec(Scheduler, spec_set=True)
    scheduler.reregistered = mock.Mock(side_effect=reregistered_side_effect)

    self.framework_info.id.MergeFrom(self.framework_id)

    driver = PesosSchedulerDriver(
        scheduler, self.framework_info, str(self.master.pid), context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.scheduler_process.connected.wait(timeout=MAX_TIMEOUT)
    assert driver.scheduler_process.connected.is_set()

    reregistered_event.wait(timeout=MAX_TIMEOUT)
    assert reregistered_event.is_set()

    assert len(self.master.frameworks) == 1

    framework_id = next(iter(self.master.frameworks))
    assert reregistered_call.mock_calls == [mock.call(driver, pid_to_master_info(self.master.pid))]

    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

  def test_scheduler_master_failover(self):
    disconnected_event, disconnected_call, disconnected_side_effect = self.mock_method()
    registered_event, registered_call, registered_side_effect = self.mock_method()
    reregistered_event, reregistered_call, reregistered_side_effect = self.mock_method()
    scheduler = mock.create_autospec(Scheduler, spec_set=True)
    scheduler.disconnected = mock.Mock(side_effect=disconnected_side_effect)
    scheduler.registered = mock.Mock(side_effect=registered_side_effect)
    scheduler.reregistered = mock.Mock(side_effect=reregistered_side_effect)

    standby_master = MockMaster()
    self.context.spawn(standby_master)

    driver = PesosSchedulerDriver(
        scheduler, self.framework_info, str(self.master.pid), context=self.context)
    assert driver.start() == mesos_pb2.DRIVER_RUNNING

    driver.scheduler_process.connected.wait(timeout=MAX_TIMEOUT)
    assert driver.scheduler_process.connected.is_set()

    registered_event.wait(timeout=MAX_TIMEOUT)
    assert registered_event.is_set()

    # now fail the current master
    driver.detector.appoint(None)
    disconnected_event.wait(timeout=MAX_TIMEOUT)
    assert disconnected_event.is_set()

    # now appoint the new master
    driver.detector.appoint(standby_master.pid)
    standby_master.reregister_event.wait(timeout=MAX_TIMEOUT)
    assert standby_master.reregister_event.is_set()
    assert standby_master.frameworks == self.master.frameworks
    reregistered_event.wait(timeout=MAX_TIMEOUT)
    assert reregistered_event.is_set()
    assert driver.stop() == mesos_pb2.DRIVER_STOPPED

