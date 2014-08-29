from pesos.util import camel, camel_call


def test_camel():
  assert camel('foo') == 'foo'
  assert camel('foo_bar') == 'fooBar'
  assert camel('camel_Snake') == 'camelSnake'


def test_camel_call():
  class CamelClass(object):
    def camelCase(self, foo, bar=None):
      return foo, bar

    def snake_case(self, foo, bar=None):
      return foo, bar

  klazz = CamelClass()
  assert camel_call(klazz, 'camel_case', 1, bar=2) == (1, 2)
  assert camel_call(klazz, 'camel_case', 1) == (1, None)
  assert camel_call(klazz, 'snake_case', 2, bar=3) == (2, 3)
  assert camel_call(klazz, 'snake_case', 2) == (2, None)

