import unittest
import os
import uuid

from azookeeper.client import ZooKeeperClient

# if this env variable is set, ZK client integration tests are run
# against the specified host list
ENV_AZK_TEST_HOSTS = "AZK_TEST_HOSTS"

class ZooKeeperClientTests(unittest.TestCase):
    def setUp(self):
        self.hosts = get_zk_hosts_or_skip()

        self.zk = None
        self.created = []

    def tearDown(self):
        if self.zk:
            #TODO remove any created nodes
            self.zk.close()

    def test_connect_close(self):
        self.zk = ZooKeeperClient(self.hosts)

        self.zk.connect()
        self.assertTrue(self.zk.connected)
        self.zk.close()
        self.assertFalse(self.zk.connected)

    def test_create_get_set(self):
        self.zk = ZooKeeperClient(self.hosts)
        self.zk.connect()

        nodepath = "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "sandwich", ephemeral=True)

        data, stat = self.zk.get(nodepath)
        self.assertEqual(data, "sandwich")

        self.zk.set(nodepath, "hats", stat["version"])


def get_zk_hosts_or_skip():
    if ENV_AZK_TEST_HOSTS in os.environ:
        return os.environ[ENV_AZK_TEST_HOSTS]
    raise unittest.SkipTest("Skipping ZooKeeper test. To run, set "+
                            "%s env to a host list. (ex: localhost:2181)" %
                            ENV_AZK_TEST_HOSTS)
  