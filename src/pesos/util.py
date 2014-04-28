from __future__ import print_function

from contextlib import contextmanager
import threading
import time


@contextmanager
def timed(message, logger=print):
  start = time.time()
  yield
  logger('%s: %.1fms' % (message, 1000.0 * (time.time() - start)))


_ID_LOCK = threading.Lock()
_ID = 1


def unique_suffix(name):
  global _ID
  with _ID_LOCK:
    real_name = '%s(%d)' % (name, _ID)
    _ID = _ID + 1
    return real_name
