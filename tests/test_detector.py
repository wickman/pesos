import os
import posixpath
import socket
import struct
import threading
import unittest
import uuid

from pesos.detector import MasterDetector, StandaloneMasterDetector, ZookeeperMasterDetector
from pesos.vendor.mesos import mesos_pb2

import pytest
from compactor.pid import PID
from kazoo.client import KazooClient


def test_standalone_immediate_detection():
  master_pid = PID.from_string('master(1)@192.168.33.2:12345')
  detector = StandaloneMasterDetector(leader=master_pid)
  event = threading.Event()
  future = detector.detect(previous=None)
  future.add_done_callback(lambda f: event.set())
  event.wait(timeout=1.0)
  assert event.is_set()
  assert future.result() == master_pid


def test_standalone_change_detection():
  master_pid_str = 'master(1)@192.168.33.2:12345'
  master_pid = PID.from_string(master_pid_str)
  detector = StandaloneMasterDetector.from_uri(master_pid_str)
  event = threading.Event()
  future = detector.detect(previous=master_pid)
  future.add_done_callback(lambda f: event.set())
  assert future.running()
  assert not event.is_set()
  detector.appoint(PID.from_string('master(2)@192.168.33.2:12345'))
  event.wait(timeout=1.0)
  assert event.is_set()


# Sadly zake does not support enough of the KazooClient interface to mock
# out KazooGroup, so we have to rely upon a real Zookeeper if we want to
# test this.
@pytest.mark.skipif('"ZOOKEEPER_IP" not in os.environ')
class TestZookeeperMasterDetector(unittest.TestCase):
  def setUp(self):
    self.client = KazooClient('%s:2181' % os.environ['ZOOKEEPER_IP'])
    self.client.start()
    self.root = '/' + uuid.uuid4().hex
    self.client.ensure_path(self.root)
    self.uri = 'zk://%s:2181%s' % (os.environ['ZOOKEEPER_IP'], self.root)

  def tearDown(self):
    self.client.delete(self.root, recursive=True)
    self.client.stop()

  def create_root(self):
    self.client.ensure_path(self.root)

  def unregister_master(self, pid):
    for path in self.client.get_children(self.root):
      full_path = posixpath.join(self.root, path)
      data, _ = self.client.get(full_path)
      master_info = mesos_pb2.MasterInfo()
      master_info.MergeFromString(data)
      if master_info.id == pid.id and master_info.port == pid.port and (
          socket.inet_ntoa(struct.pack('<L', master_info.ip)) == pid.ip):
        self.client.delete(full_path)
        return True

    return False

  def register_master(self, pid):
    master_info = mesos_pb2.MasterInfo(
        id=pid.id,
        ip=struct.unpack('<L', socket.inet_aton(pid.ip))[0],
        port=pid.port)
    self.client.create(
        '%s/info_' % self.root,
        value=master_info.SerializeToString(),
        ephemeral=True,
        sequence=True)

  def test_zk_master_detector_creation(self):
    class WrappedZookeeperMasterDetector(ZookeeperMasterDetector):
      def __init__(self, *args, **kw):
        super(WrappedZookeeperMasterDetector, self).__init__(*args, **kw)
        self.changed = threading.Event()

      def on_change(self, membership):
        self.changed.set()
        super(WrappedZookeeperMasterDetector, self).on_change(membership)

    event = threading.Event()
    leader_queue = []

    def appointed_callback(future):
      leader_queue.append(future.result())
      event.set()

    self.create_root()

    # construct master detector and detect master
    detector = WrappedZookeeperMasterDetector.from_uri(self.uri)
    leader_future = detector.detect().add_done_callback(appointed_callback)

    # trigger detection by registering master
    master_pid = PID('10.1.2.3', 12345, 'master(1)')
    self.register_master(master_pid)
    detector.changed.wait(timeout=10)
    assert detector.changed.is_set()
    event.wait(timeout=10)
    assert event.is_set()
    assert leader_queue == [master_pid]
    leader_queue = []
    event.clear()

    # start new detection loop when existing master changes
    leader_future = detector.detect(master_pid).add_done_callback(appointed_callback)
    detector.changed.clear()

    # register new master (won't trigger detection until original master is gone.)
    new_master_pid = PID('10.2.3.4', 12345, 'master(1)')
    self.register_master(new_master_pid)
    detector.changed.wait(timeout=10)
    assert detector.changed.is_set()
    detector.changed.clear()
    assert leader_queue == []
    assert not event.is_set()

    # failover existing master
    assert self.unregister_master(master_pid)

    # make sure new master is detected.
    detector.changed.wait(timeout=10)
    assert detector.changed.is_set()
    event.wait(timeout=10)
    assert event.is_set()
    assert leader_queue == [new_master_pid]


def test_from_uri():
  detector = MasterDetector.from_uri('master(1)@192.168.33.2:5051')
  assert isinstance(detector, StandaloneMasterDetector)


@pytest.mark.skipif('"ZOOKEEPER_IP" not in os.environ')
def test_from_uri_zk():
  detector = MasterDetector.from_uri('zk://%s/blorp' % os.environ['ZOOKEEPER_IP'])
  assert isinstance(detector, ZookeeperMasterDetector)
