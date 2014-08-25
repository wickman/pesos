from setuptools import setup

__version__ = '0.1.0'


setup(
  name='pesos',
  version=__version__,
  description='Pure python implementation of the Apache Mesos Framework API',
  url='http://github.com/wickman/pesos',
  author='Brian Wickman',
  author_email='wickman@gmail.com',
  license='Apache License 2.0',
  packages=[
    'pesos',
    'pesos.vendor',
    'pesos.vendor.mesos',
    'pesos.vendor.mesos.containerizer',
    'pesos.vendor.mesos.internal',
    'pesos.vendor.mesos.internal.log',
    'pesos.vendor.mesos.internal.state',
  ],
  install_requires=[
    'compactor==0.1.2',
    'protobuf==2.5.0',
    'futures==2.1.6',
  ],
  zip_safe=True
)
