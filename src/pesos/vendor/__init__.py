from __future__ import absolute_import

try:
  import mesos.pb as mesos
except ImportError:
  from . import mesos
