from pesos.detector import StandaloneMasterDetector

import mock
import threading


def test_standalone_immediate_detection():
  detector = StandaloneMasterDetector(leader='foo')
  event = threading.Event()
  future = detector.detect(previous=None)
  future.add_done_callback(lambda f: event.set())
  event.wait(timeout=1.0)
  assert event.is_set()
  assert future.result() == 'foo'


def test_standalone_change_detection():
  detector = StandaloneMasterDetector(leader='foo')
  event = threading.Event()
  future = detector.detect(previous='foo')
  future.add_done_callback(lambda f: event.set())
  assert future.running()
  assert not event.is_set()
  detector.appoint('bar')
  event.wait(timeout=1.0)
  assert event.is_set()
