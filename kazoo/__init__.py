import os

from kazoo.zkclient import ZooKeeperClient
from kazoo.client import KazooClient

__all__ = ['ZooKeeperClient', 'KazooClient']


# ZK C client likes to spew log info to STDERR. disable that unless an
# env is present.

def disable_zookeeper_log():
    import zookeeper
    zookeeper.set_log_stream(open('/dev/null'))

if not "KAZOO_LOG_ENABLED" in os.environ:
    disable_zookeeper_log()

def patch_extras():
    # workaround for http://code.google.com/p/gevent/issues/detail?id=112
    # gevent isn't patching threading._sleep which causes problems
    # for Condition objects
    from gevent import sleep
    import threading
    threading._sleep = sleep

if "KAZOO_TEST_GEVENT_PATCH" in os.environ:
    from gevent import monkey; monkey.patch_all()
    patch_extras()
