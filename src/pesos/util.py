from __future__ import print_function

from contextlib import contextmanager
import threading
import time


@contextmanager
def timed(logger=print, message=None):
  start = time.time()
  yield
  time_ms = 1000.0 * (time.time() - start)
  logger("%s: %.1fms" % (message, time_ms))


_ID_LOCK = threading.Lock()
_ID = 1


def unique_suffix(name):
  global _ID
  with _ID_LOCK:
    real_name = '%s(%d)' % (name, _ID)
    _ID = _ID + 1
    return real_name
