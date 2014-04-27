python_library(
  name = 'mesos-protobuf',
  sources = rglobs('vendor/*.py'),
  dependencies = [
    # TODO(wickman) https://issues.apache.org/jira/browse/MESOS-857
    # python_requirement('mesos.pb==0.19.0'),
    python_requirement('protobuf==2.5.0'),
  ],
)

python_library(
  name = 'pesos',
  sources = globs('*.py'),
  dependencies = [
    pants(':mesos-protobuf'),
    python_requirement('compactor==0.1.0'),
  ],
)
