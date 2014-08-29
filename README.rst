pesos
=====

pesos is a pure python implementation of the mesos framework api based upon
`compactor <https://github.com/wickman/compactor>`_.


Using
=====

pesos is intended to be a drop-in replacement for the ``mesos.native``
package.  While Python bindings are provided by the Mesos project, they
require libmesos which can be challenging to build and package.  pesos
requires no C extensions to run.

To use:

.. code-block:: python

    try:
        from pesos.executor import PesosExecutorDriver as MesosExecutorDriver
        from pesos.scheduler import PesosSchedulerDriver as MesosSchedulerDriver
    except ImportError:
        from mesos.native import MesosExecutorDriver, MesosSchedulerDriver

Then use the pesos-provided equivalents as you would the native Mesos versions.


Testing
=======

pesos uses `tox <https://tox.rtfd.org>`_ as a test harness.  To run tests,
``pip install tox`` and execute

.. code-block:: bash

    $ tox


Caveats
=======

pesos relies upon compactor, which currently requires a version of tornado
(tornado>=4.1) that has not yet been released.  To run the tests, you must
clone and generate a source distribution of tornado off master (which,
currently, will produce tornado==4.1.dev1) and copy it into the
``third_party`` directory, where it will be used by tox for testing.
