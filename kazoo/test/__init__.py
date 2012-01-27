import os
import unittest

from kazoo.client import ZooKeeperClient

# if this env variable is set, ZK client integration tests are run
# against the specified host list
ENV_AZK_TEST_HOSTS = "AZK_TEST_HOSTS"

def get_hosts_or_skip():
    if ENV_AZK_TEST_HOSTS in os.environ:
        return os.environ[ENV_AZK_TEST_HOSTS]
    raise unittest.SkipTest("Skipping ZooKeeper test. To run, set "+
                            "%s env to a host list. (ex: localhost:2181)" %
                            ENV_AZK_TEST_HOSTS)

def get_client_or_skip(**kwargs):
    hosts = get_hosts_or_skip()
    return ZooKeeperClient(hosts, **kwargs)