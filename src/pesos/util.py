from __future__ import print_function

from contextlib import contextmanager
import time


@contextmanager
def timed(message, logger=print):
  start = time.time()
  yield
  logger('%s: %.1fms' % (message, 1000.0 * (time.time() - start)))
  