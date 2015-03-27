import os
import posixpath
import socket
import struct
import threading
import unittest
import uuid

from pesos.detector import StandaloneMasterDetector, ZookeeperMasterDetector
from pesos.vendor.mesos import mesos_pb2

import pytest
from compactor.pid import PID
from kazoo.client import KazooClient


def test_standalone_immediate_detection():
  detector = StandaloneMasterDetector(leader=PID.from_string('master(1)@192.168.33.2:12345'))
  event = threading.Event()
  future = detector.detect(previous=None)
  future.add_done_callback(lambda f: event.set())
  event.wait(timeout=1.0)
  assert event.is_set()
  assert future.result() == PID.from_string('master(1)@192.168.33.2:12345')


def test_standalone_change_detection():
  detector = StandaloneMasterDetector(leader=PID.from_string('master(1)@192.168.33.2:12345'))
  event = threading.Event()
  future = detector.detect(previous=PID.from_string('master(1)@192.168.33.2:12345'))
  future.add_done_callback(lambda f: event.set())
  assert future.running()
  assert not event.is_set()
  detector.appoint(PID.from_string('master(2)@192.168.33.2:12345'))
  event.wait(timeout=1.0)
  assert event.is_set()

"""
The kazoo group is too sophisticated for zake, so we need to use the real thing.

from zake.fake_client import FakeClient

def test_zk_master_detector():
  client = FakeClient()
  client.start()
  client.create('/master')

  event = threading.Event()
  leader_queue = []

  def appointed_callback(leader):
    leader_queue.append(leader)
    event.set()

  detector = ZookeeperMasterDetector('zk://glorp/master', client=client)

  leader_future = detector.detect().add_done_callback(appointed_callback)

  master_info = mesos_pb2.MasterInfo(
      id='master(1)',
      ip=struct.unpack('<L', socket.inet_aton('192.168.33.2'))[0],
      port=12345)

  client.create(
      '/master/info_',
      value=master_info.SerializeToString(),
      ephemeral=True,
      sequence=True)

  event.wait(timeout=10)
  assert event.is_set()
  assert leader_queue == [PID('192.168.33.2', 12345, 'master(1)')]
"""


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
      data = self.client.get(full_path)
      master_info = mesos_pb2.MasterInfo()
      master_info.MergeFromString(data)

      if master_info.id == pid.id and master_info.port == pid.port and (
          socket.inet_ntoa(struct.pack('<L', master.ip)) == pid.ip):
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

  def test_zk_master_detector_root_exists(self):
    event = threading.Event()
    leader_queue = []

    def appointed_callback(future):
      leader_queue.append(future.result())
      event.set()

    self.create_root()

    detector = ZookeeperMasterDetector(self.uri)
    leader_future = detector.detect().add_done_callback(appointed_callback)

    master_pid = PID('10.1.2.3', 12345, 'master(1)')
    self.register_master(master_pid)

    event.wait(timeout=10)
    assert event.is_set()
    assert leader_queue == [master_pid]

  def test_zk_master_detector_root_doesnt_exist(self):
    pass

  def test_zk_master_detector_election(self):
    pass
