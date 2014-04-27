from setuptools import setup


__version__ = '0.1.0'


install_requires = [
  'compactor==0.1.0',
  'protobuf==2.5.0',
]


setup(
  name='pesos',
  version=__version__,
  description='a pure python implementation of the mesos framework api',
  url='http://github.com/wickman/pesos',
  author='Brian Wickman',
  author_email='wickman@gmail.com',
  license='Apache License 2.0',
  package_dir={'': 'src'},
  packages=[
     'pesos',
     'pesos.vendor',
     'pesos.vendor.mesos',
     'pesos.vendor.mesos.containerizer',
     'pesos.vendor.mesos.internal',
     'pesos.vendor.mesos.internal.log',
     'pesos.vendor.mesos.internal.state',
  ],
  install_requires=install_requires,
  zip_safe=True,
)
