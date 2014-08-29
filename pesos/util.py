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


# camelCase a snake_case name without capitalizing the first word
def camel(name):
  def yield_components(name):
    split_name = name.split('_')
    yield split_name[0]
    for split in split_name[1:]:
      yield split.capitalize()
  return ''.join(yield_components(name))


def camel_call(instance, method, *args, **kw):
  # TODO(wickman) assert 'method' is snake_case
  instance_method = getattr(instance, method, None)
  if instance_method:
    return instance_method(*args, **kw)

  # snake_case fell through, try camelCase -- raising AttributeError if it is missing
  instance_method = getattr(instance, camel(method))
  if instance_method:
    return instance_method(*args, **kw)
