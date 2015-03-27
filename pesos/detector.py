import socket
import struct
import threading

from .vendor.mesos.mesos_pb2 import MasterInfo

from compactor.pid import PID
from concurrent.futures import Future
from kazoo.client import KazooClient
from twitter.common.zookeeper.group.kazoo_group import KazooGroup

try:
  from urlparse import urlparse
except ImportError:
  from urllib.parse import urlparse


class MasterDetector(object):
  class Error(Exception): pass
  class InvalidUrl(Error): pass

  @classmethod
  def from_url(cls, url):
    raise NotImplementedError

  def detect(self, previous=None):
    """Detect a master should it change from ``previous``.

    :keyword previous: Either ``None`` or a :class:`Process` object pointing to the previously
      detected master.
    :returns: :class:`Future` that can either provide ``None`` or a :class:`Process` value.
    """
    future = Future()
    future.set_exception(NotImplementedError())
    return future


class FutureMasterDetector(MasterDetector):
  """A base class for future-based master detectors."""

  def __init__(self):
    self.__lock = threading.Lock()
    self._leader = None
    self._future_queue = []  # list of queued detection requests

  def fail(self, exception):
    with self.__lock:
      future_queue, self._future_queue = self._future_queue, []
      for future in future_queue:
        future.set_exception(exception)

  def appoint(self, leader):
    with self.__lock:
      if leader == self._leader:
        return
      self._leader = leader
      future_queue, self._future_queue = self._future_queue, []
      for future in future_queue:
        future.set_result(leader)

  def detect(self, previous=None):
    with self.__lock:
      future = Future()
      if previous != self._leader:
        future.set_result(self._leader)
        return future
      self._future_queue.append(future)
      future.set_running_or_notify_cancel()
      return future


class StandaloneMasterDetector(FutureMasterDetector):
  """A concrete implementation of a standalone master detector."""

  def __init__(self, leader=None):
    super(StandaloneMasterDetector, self).__init__()

    if leader:
      self.appoint(leader)


class MesosKazooGroup(KazooGroup):
  MEMBER_PREFIX = 'info_'


class ZookeeperMasterDetector(FutureMasterDetector):
  def __init__(self, url, client=None):
    super(ZookeeperMasterDetector, self).__init__()

    url = urlparse(url)
    if url.scheme.lower() != 'zk':
      raise self.InvalidUrl('ZookeeperMasterDetector got bad ensemble url: %s' % (url,))

    if client:  # for mocking
      self._kazoo_client = client
    else:
      self._kazoo_client = KazooClient(url.netloc)
      self._kazoo_client.start_async()

    self._group = MesosKazooGroup(self._kazoo_client, url.path)
    self._group.monitor(callback=self._on_change)

  def _on_change(self, membership):
    if membership:
      leader = sorted(membership)[0]
      self._group.info(leader, callback=self._on_appointment)
    self._group.monitor(membership, callback=self._on_change)

  def _on_appointment(self, master_data):
    master_info = MasterInfo()
    master_info.MergeFromString(master_data)
    ip, port, id_ = (
        socket.inet_ntoa(struct.pack('<L', master_info.ip)),
        master_info.port,
        master_info.id)
    master_pid = PID(ip, port, id_)
    self.appoint(master_pid)

