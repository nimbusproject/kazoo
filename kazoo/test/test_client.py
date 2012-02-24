import unittest
import uuid
import threading

from kazoo.client import KazooClient
from kazoo.zkclient import EventType
from kazoo.test import get_hosts_or_skip

class ZooKeeperClientTests(unittest.TestCase):
    def setUp(self):
        self.hosts = get_hosts_or_skip()

    def test_namespace(self):

        namespace = "/" + uuid.uuid4().hex
        client = KazooClient(self.hosts, namespace=namespace)

        client.connect()

        # grab internal ZK client for non-namespaced ops
        zk = client.zk

        created_path = client.create("/hi", "hello")
        self.assertEqual(created_path, "/hi")
        self.assertTrue(zk.exists(namespace + "/hi"))

        condition = threading.Condition()
        watch_events = []
        def watch(event):
            with condition:
                watch_events.append(event)
                condition.notify_all()

        data, stat = client.get("/hi", watch=watch)
        self.assertEqual(data, "hello")

        children = client.get_children("/hi", watch=watch)
        self.assertFalse(children)

        # now create a child to trigger the watch
        created_path = client.create("/hi/there", "hello")
        self.assertEqual(created_path, "/hi/there")
        self.assertTrue(zk.exists(namespace + "/hi/there"))

        with condition:
            if not watch_events:
                condition.wait(5)

        self.assertEqual(len(watch_events), 1)
        event = watch_events[0]
        self.assertEqual(event.type, EventType.CHILD)
        self.assertEqual(event.path, "/hi")

        watch_events[:] = []

        # change the value to trigger the get() watch
        client.set("/hi", "bye")

        with condition:
            if not watch_events:
                condition.wait(5)
        self.assertEqual(len(watch_events), 1)
        event = watch_events[0]
        self.assertEqual(event.type, EventType.CHANGED)
        self.assertEqual(event.path, "/hi")

        # delete the child and parent
        client.delete("/hi/there")
        client.delete("/hi")

        self.assertFalse(zk.exists(namespace + "/hi"))

    def test_ensure_path(self):
        namespace = "/" + uuid.uuid4().hex
        client = KazooClient(self.hosts, namespace=namespace)

        client.connect()
        zk = client.zk

        client.ensure_path("/1/2")
        self.assertTrue(client.exists("/1/2"))
        self.assertTrue(zk.exists(namespace + "/1/2"))

        client.ensure_path("/1/2/3/4")
        self.assertTrue(client.exists("/1/2/3/4"))
        self.assertTrue(zk.exists(namespace + "/1/2/3/4"))

