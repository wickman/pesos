python_test_suite(
  name = 'all',
  dependencies = [
    pants(':test_executor'),
    pants(':test_scheduler'),
    pants(':test_protobuf_passing'),
  ],
)


python_tests(
  name = 'test_executor',
  sources = ['test_executor.py'],
  dependencies = [
    pants('src/pesos'),
    python_requirement('mock'),
  ],
)


python_tests(
  name = 'test_scheduler',
  sources = ['test_scheduler.py'],
  dependencies = [
    pants('src/pesos'),
  ],
)


python_tests(
  name = 'test_protobuf_passing',
  sources = ['test_protobuf_passing.py'],
  dependencies = [
    pants('src/pesos'),
  ],
)
