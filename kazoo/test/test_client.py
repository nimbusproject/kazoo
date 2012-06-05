import threading
import uuid

from kazoo.client import KazooState, make_digest_acl
from kazoo.zkclient import EventType
from kazoo.test import KazooTestCase
from kazoo.exceptions import NoNodeException, NoAuthException

class KazooClientTests(KazooTestCase):

    def test_namespace(self):
        namespace = self.namespace
        client = self.client

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
        namespace = self.namespace
        client = self.client

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

        self.client.add_listener(listener)
        self.client.connect(5)

        with condition:
            if not states:
                condition.wait(5)

        self.assertEqual(len(states), 1)
        self.assertEqual(states[0], KazooState.CONNECTED)

    def test_create_no_makepath(self):

        self.client.connect()

        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1")
        self.assertRaises(NoNodeException, self.client.create, "/1/2", "val1",
            makepath=False)

    def test_create_makepath(self):
        self.client.connect()

        self.client.create("/1/2", "val1", makepath=True)
        data, stat = self.client.get("/1/2")
        self.assertEqual(data, "val1")

        self.client.create("/1/2/3/4/5", "val2", makepath=True)
        data, stat = self.client.get("/1/2/3/4/5")
        self.assertEqual(data, "val2")

    def test_auth(self):

        self.client.connect()

        username = uuid.uuid4().hex
        password = uuid.uuid4().hex

        digest_auth = "%s:%s" % (username, password)
        acl = make_digest_acl(username, password, all=True)

        self.client.add_auth("digest", digest_auth)

        self.client.default_acl = (acl,)

        self.client.create("/1", "")
        self.client.create("/1/2", "")

        eve = self._get_client()
        eve.connect()

        self.assertRaises(NoAuthException, eve.get, "/1/2")

        # try again with the wrong auth token
        eve.add_auth("digest", "badbad:bad")

        self.assertRaises(NoAuthException, eve.get, "/1/2")
