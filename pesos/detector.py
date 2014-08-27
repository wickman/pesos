import threading
from concurrent.futures import Future


class MasterDetector(object):
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
    self.appoint(leader)
