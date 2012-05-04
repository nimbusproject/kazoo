import unittest
import uuid
import threading

from kazoo.client import KazooClient, KazooState
from kazoo.zkclient import EventType
from kazoo.test import get_hosts_or_skip
from kazoo.exceptions import NoNodeException

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

    def test_state_listener(self):

        states = []
        condition = threading.Condition()

        def listener(state):
            with condition:
                states.append(state)
                condition.notify_all()

        namespace = "/" + uuid.uuid4().hex
        client = KazooClient(self.hosts, namespace=namespace)

        client.add_listener(listener)
        client.connect(5)

        with condition:
            if not states:
                condition.wait(5)

        self.assertEqual(len(states), 1)
        self.assertEqual(states[0], KazooState.CONNECTED)

    def test_create_no_makepath(self):
        namespace = "/" + uuid.uuid4().hex
        client = KazooClient(self.hosts, namespace=namespace)

        client.connect()

        self.assertRaises(NoNodeException, client.create, "/1/2", "val1")
        self.assertRaises(NoNodeException, client.create, "/1/2", "val1",
            makepath=False)

    def test_create_makepath(self):
        namespace = "/" + uuid.uuid4().hex
        client = KazooClient(self.hosts, namespace=namespace)
        client.connect()

        client.create("/1/2", "val1", makepath=True)
        data, stat = client.get("/1/2")
        self.assertEqual(data, "val1")

        client.create("/1/2/3/4/5", "val2", makepath=True)
        data, stat = client.get("/1/2/3/4/5")
        self.assertEqual(data, "val2")

