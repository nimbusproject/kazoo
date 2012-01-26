#####################################################################
kazoo - Python ZooKeeper client that supports async frameworks
#####################################################################

`ZooKeeper`_ is a high-performance coordination service for distributed
applications. The official Python client `zkpython`_ is a C extension
which does not play well with asynchronous frameworks such as `gevent`_
due to its use of real threads. `kazoo` is a wrapper client which
uses IO tricks to be async compatible. It also attempts to present a
more pythonic interface than zkpython.

Initially kazoo only supports gevent as well as non-async environments.

.. _`ZooKeeper`: http://zookeeper.apache.org/
.. _`zkpython`: http://pypi.python.org/pypi/zkpython
.. _`gevent`: http://www.gevent.org/
