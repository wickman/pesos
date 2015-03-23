from pesos.util import camel, camel_call, duration_to_seconds

import pytest


def test_camel():
  assert camel('foo') == 'foo'
  assert camel('foo_bar') == 'fooBar'
  assert camel('camel_Snake') == 'camelSnake'


def test_camel_call():
  class CamelClass(object):
    def camelCase(self, foo, bar=None):  # noqa
      return foo, bar

    def snake_case(self, foo, bar=None):
      return foo, bar

  klazz = CamelClass()
  assert camel_call(klazz, 'camel_case', 1, bar=2) == (1, 2)
  assert camel_call(klazz, 'camel_case', 1) == (1, None)
  assert camel_call(klazz, 'snake_case', 2, bar=3) == (2, 3)
  assert camel_call(klazz, 'snake_case', 2) == (2, None)


def test_duration():
  assert duration_to_seconds('0secs') == 0
  assert duration_to_seconds('1') == 1
  assert duration_to_seconds('1ns') == .000000001
  assert duration_to_seconds('1us') == .000001
  assert duration_to_seconds('1ms') == .001
  assert duration_to_seconds('1secs') == 1
  assert duration_to_seconds('1mins') == 60
  assert duration_to_seconds('1hrs') == 3600
  assert duration_to_seconds('1days') == 86400
  assert duration_to_seconds('1weeks') == 86400*7
  assert duration_to_seconds('3hrs') == 3 * duration_to_seconds('1hrs')
  assert duration_to_seconds('3141592653ns') == 3.141592653
  assert duration_to_seconds('-10hrs') == -10 * duration_to_seconds('1hrs')
  assert duration_to_seconds('1.1875days') == (
      duration_to_seconds('1days') +
      duration_to_seconds('4hrs') +
      duration_to_seconds('30mins'))

  for bad_duration in ('', 'days', 23):
    with pytest.raises(ValueError):
      duration_to_seconds(bad_duration)
