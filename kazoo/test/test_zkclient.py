import uuid
import threading

from kazoo.test import KazooTestCase

class ZooKeeperClientTests(KazooTestCase):

    @property
    def zk(self):
        return self.client.zk

    def test_create_get_set(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = self.namespace + "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "sandwich", ephemeral=True)

        data, stat = self.zk.get(nodepath)
        self.assertEqual(data, "sandwich")

        newstat = self.zk.set(nodepath, "hats", stat["version"])
        self.assertTrue(newstat)
        self.assertGreater(newstat['version'], stat['version'])

    def test_create_get_sequential(self):
        self.client.connect()
        self.client.ensure_path("/")

        basepath = "/" + uuid.uuid4().hex
        realpath = self.zk.create(basepath, "sandwich", sequence=True,
            ephemeral=True)

        self.assertTrue(basepath != realpath and realpath.startswith(basepath))

        data, stat = self.zk.get(realpath)
        self.assertEqual(data, "sandwich")

    def test_exists(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        exists = self.zk.exists(nodepath)
        self.assertIsNone(exists)

        self.zk.create(nodepath, "sandwich", ephemeral=True)
        exists = self.zk.exists(nodepath)
        self.assertTrue(exists)
        self.assertIn("version", exists)

        multi_node_nonexistent = "/" + uuid.uuid4().hex + "/hats"
        exists = self.zk.exists(multi_node_nonexistent)
        self.assertIsNone(exists)

    def test_exists_watch(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        def w(watch_event):
            self.assertEqual(watch_event.path, nodepath)
            event.set()

        exists = self.zk.exists(nodepath, watch=w)
        self.assertIsNone(exists)

        self.zk.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_exists_watcher_exception(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        event = threading.Event()

        # if the watcher throws an exception, all we can really do is log it
        def w(watch_event):
            self.assertEqual(watch_event.path, nodepath)
            event.set()

            raise Exception("test exception in callback")

        exists = self.zk.exists(nodepath, watch=w)
        self.assertIsNone(exists)

        self.zk.create(nodepath, "x", ephemeral=True)

        event.wait(1)
        self.assertTrue(event.is_set())

    def test_create_delete(self):
        self.client.connect()
        self.client.ensure_path("/")

        nodepath = "/" + uuid.uuid4().hex

        self.zk.create(nodepath, "zzz")

        self.zk.delete(nodepath)

        exists = self.zk.exists(nodepath)
        self.assertIsNone(exists)



