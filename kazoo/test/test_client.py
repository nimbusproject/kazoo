import unittest
import os
import uuid
import threading

from kazoo.client import ZooKeeperClient

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

        newstat = self.zk.set(nodepath, "hats", stat["version"])
        self.assertTrue(newstat)
        self.assertGreater(newstat['version'], stat['version'])

    def test_create_get_sequential(self):
        self.zk = ZooKeeperClient(self.hosts)
        self.zk.connect()

        basepath = "/" + uuid.uuid4().hex
        realpath = self.zk.create(basepath, "sandwich", sequence=True,
            ephemeral=True)

        self.assertTrue(basepath != realpath and realpath.startswith(basepath))

        data, stat = self.zk.get(realpath)
        self.assertEqual(data, "sandwich")

    def test_exists(self):
        self.zk = ZooKeeperClient(self.hosts)
        self.zk.connect()

        nodepath = "/" + uuid.uuid4().hex

        exists = self.zk.exists(nodepath)
        self.assertIsNone(exists)

        self.zk.create(nodepath, "sandwich", ephemeral=True)
        exists = self.zk.exists(nodepath)
        self.assertTrue(exists)
        self.assertIn("version", exists)

    def test_exists_watch(self):
        self.zk = ZooKeeperClient(self.hosts)
        self.zk.connect()

        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        def w(type, state, path):
            self.assertEqual(path, nodepath)
            event.set()

        exists = self.zk.exists(nodepath, watch=w)
        self.assertIsNone(exists)

        self.zk.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_create_delete(self):
        self.zk = ZooKeeperClient(self.hosts)
        self.zk.connect()

        nodepath = "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "zzz")

        self.zk.delete(nodepath)

        exists = self.zk.exists(nodepath)
        self.assertIsNone(exists)

def get_zk_hosts_or_skip():
    if ENV_AZK_TEST_HOSTS in os.environ:
        return os.environ[ENV_AZK_TEST_HOSTS]
    raise unittest.SkipTest("Skipping ZooKeeper test. To run, set "+
                            "%s env to a host list. (ex: localhost:2181)" %
                            ENV_AZK_TEST_HOSTS)
  
