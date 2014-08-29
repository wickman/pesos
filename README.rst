Pesos
=====

pesos is a pure python implementation of the mesos framework api based upon
`compactor <https://github.com/wickman/compactor>`_.

Using
=====

pesos is intended to provide drop-in replacements for the mesos.native
MesosExecutorDriver and MesosSchedulerDriver.  Bindings are provided by the
Mesos project but require libmesos which can be challenging to build and package.

to use:

.. code-block:: python

    try:
        from pesos.executor import PesosExecutorDriver as MesosExecutorDriver
        from pesos.scheduler import PesosSchedulerDriver as MesosSchedulerDriver
    except ImportError:
        from mesos.native import MesosExecutorDriver, MesosSchedulerDriver


Testing
=======

pesos uses `tox <https://tox.rtfd.org>` for its test harness.  To run tests,
``pip install tox`` and execute

.. code-block:: bash

    $ tox


Caveats
=======

Pesos relies upon Compactor, which currently requires tornado>=4.1 which has
not yet been released.  To run the tests, you must clone and generate a
source distribution of tornado off master (which will produce
tornado==4.1.dev1) and copy it into the ``third_party`` directory, where it
will be used by tox for testing.
