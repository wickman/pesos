=======
CHANGES
=======

-----
0.2.2
-----

* Pins to ``compactor==0.2.2`` which fixes a number of race conditions.

-----
0.2.1
-----

* Add stout's ``Duration`` support to pesos so that it works correctly with
  slave checkpointing.

-----
0.2.0
-----

* Improved executor test coverage.

* Vagrant image added for integration testing

* Revendored mesos protobufs for 0.23.x

* Implement against mesos.interface instead of vendorized interface in ``pesos.api``
  (``pesos.api`` has now been removed.)

* Example executor and scheduler implementations provided.

-----
0.1.0
-----

* Basic framework implemented with vendorized API and mesos interfaces.
