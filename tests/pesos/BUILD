python_test_suite(
  name = 'all',
  dependencies = [
    pants(':test_executor'),
  ],
)


python_tests(
  name = 'test_executor',
  sources = ['test_executor.py'],
  dependencies = [
    pants('src/pesos'),
  ],
)
