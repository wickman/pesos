import logging
import socket
import struct
import threading

from .vendor.mesos.mesos_pb2 import MasterInfo

from compactor.pid import PID
from concurrent.futures import Future

try:
  from kazoo.client import KazooClient
  from twitter.common.zookeeper.group.kazoo_group import KazooGroup

  HAS_ZK = True
except ImportError:
  HAS_ZK = False

try:
  from urlparse import urlparse
except ImportError:
  from urllib.parse import urlparse


log = logging.getLogger(__name__)


def master_info_to_pid(master_info):
  return PID(
      socket.inet_ntoa(struct.pack('<L', master_info.ip)),
      master_info.port,
      master_info.id,
  )


def pid_to_master_info(pid):
  return MasterInfo(
      ip=struct.unpack('<L', socket.inet_aton(pid.ip))[0],
      id=pid.id,
      port=pid.port
  )


class MasterDetector(object):
  class Error(Exception): pass
  class InvalidUri(Error): pass
  class CannotDetect(Error): pass

  _DETECTORS = []

  @classmethod
  def from_uri(cls, uri):
    for detector_cls in cls._DETECTORS:
      try:
        return detector_cls.from_uri(uri)
      except cls.InvalidUri:
        continue
    raise cls.CannotDetector('No compatible master detectors found for %r' % uri)

  @classmethod
  def register(cls, detector_cls):
    cls._DETECTORS.append(detector_cls)

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
    print('grabbing lock.')
    with self.__lock:
      future_queue, self._future_queue = self._future_queue, []
      for future in future_queue:
        future.set_exception(exception)

  def appoint(self, leader):
    print('grabbing lock.')
    with self.__lock:
      if leader == self._leader:
        log.info('FutureMasterDetector.appoint skipping identical appointment %s' % leader)
        return
      else:
        log.info('FutureMasterDetector.appoint accepting appointment %s' % leader)
      self._leader = leader
      future_queue, self._future_queue = self._future_queue, []
      print('Crunching through future queue')
      for future in future_queue:
        print('Finishing future: %s' % future)
        future.set_result(leader)
      print('Done')

  def detect(self, previous=None):
    print('grabbing lock.')
    with self.__lock:
      future = Future()
      if previous != self._leader:
        future.set_result(self._leader)
        return future
      else:
        log.info('FutureMasterDetector.detect no-op because previous same as leader: %s' % previous)
      self._future_queue.append(future)
      future.set_running_or_notify_cancel()
      return future


class StandaloneMasterDetector(FutureMasterDetector):
  """A concrete implementation of a standalone master detector."""

  @classmethod
  def from_uri(cls, uri):
    try:
      leader_pid = PID.from_string(uri)
    except ValueError:
      raise cls.InvalidUri('Not a PID: %r' % uri)
    return cls(leader=leader_pid)

  def __init__(self, leader=None):
    super(StandaloneMasterDetector, self).__init__()

    if leader:
      if not isinstance(leader, PID):
        raise TypeError('StandaloneMasterDetector takes a PID, got %s' % type(leader))
      self.appoint(leader)


MasterDetector.register(StandaloneMasterDetector)


if HAS_ZK:
  class MesosKazooGroup(KazooGroup):
    MEMBER_PREFIX = 'info_'

  class ZookeeperMasterDetector(FutureMasterDetector):
    @classmethod
    def from_uri(cls, uri):
      url = urlparse(uri)
      if url.scheme.lower() != 'zk':
        raise self.InvalidUrl('ZookeeperMasterDetector got invalid ensemble URI %s' % uri)
      return cls(url.netloc, url.path)

    def __init__(self, ensemble, path):
      super(ZookeeperMasterDetector, self).__init__()

      self._kazoo_client = KazooClient(ensemble)
      self._kazoo_client.start_async()

      self._group = MesosKazooGroup(self._kazoo_client, path)
      self._group.monitor(callback=self.on_change)

    def on_change(self, membership):
      if membership:
        leader = sorted(membership)[0]
        self._group.info(leader, callback=self.on_appointment)
      self._group.monitor(membership, callback=self.on_change)

    def on_appointment(self, master_data):
      master_info = MasterInfo()
      master_info.MergeFromString(master_data)
      self.appoint(master_info_to_pid(master_info))

  MasterDetector.register(ZookeeperMasterDetector)
