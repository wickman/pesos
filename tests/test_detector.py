from pesos.detector import StandaloneMasterDetector

import mock
import threading


def test_standalone_immediate_detection():
  detector = StandaloneMasterDetector(leader='foo')
  event = threading.Event()
  
  def detector_callback(future):
    assert future.result() == 'foo'
    event.set()
  
  detector.detect(previous=None).add_done_callback(detector_callback)
  event.wait(timeout=1.0)
  assert event.is_set()


def test_standalone_change_detection():
  detector = StandaloneMasterDetector(leader='foo')
  event = threading.Event()

  def detector_callback(future):
    assert future.result() == 'bar'
    event.set()
  
  future = detector.detect(previous='foo')
  future.add_done_callback(detector_callback)
  assert future.running()
  
  detector.appoint('bar')
  event.wait(timeout=1.0)
  assert event.is_set()
