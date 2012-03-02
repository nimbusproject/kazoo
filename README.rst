#####################################################################
kazoo - Python ZooKeeper client that supports async frameworks
#####################################################################

`ZooKeeper`_ is a high-performance coordination service for distributed
applications. The official Python client is a C extension
which does not play well with asynchronous frameworks such as `gevent`_
due to its use of real threads. `kazoo` is a wrapper client which
uses IO tricks to be async compatible. It also attempts to present a
more pythonic interface than zkpython.

Initially kazoo only supports gevent as well as non-async environments.

Kazoo depends on the ZooKeeper python binding. Because this can be
installed in various ways, it is not a listed Python dependency of
kazoo. The ZK bindings should be installed from your distribution
if possible. Otherwise the unofficial ``zc-zookeeper-static``
package is easy to install via PyPi.

.. _`ZooKeeper`: http://zookeeper.apache.org/
.. _`gevent`: http://www.gevent.org/
