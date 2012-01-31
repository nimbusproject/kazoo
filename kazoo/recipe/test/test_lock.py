import unittest
import uuid
import threading
import time

from kazoo.recipe.lock import ZooLock
from kazoo.test import get_client_or_skip

class ZooLockTests(unittest.TestCase):

    def setUp(self):
        self._c = get_client_or_skip()
        self._c.connect()
        self.lockpath = "/" + uuid.uuid4().hex
        self._c.create(self.lockpath, "")

        self.active = 0

    def tearDown(self):
        if self.lockpath:
            try:
                self._c.delete(self.lockpath)
            except Exception:
                pass

    def test_lock(self):
        clients = []
        locks = []

        for _ in range(5):
            c = get_client_or_skip()
            c.connect()

            l = ZooLock(c, self.lockpath)

            clients.append(c)
            locks.append(l)

        # these will be greenlets in a monkey patched test env.
        threads = [threading.Thread(target=self._thread_lock_acquire,
            args=(lock,)) for lock in locks]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        self.assertEqual(0, self.active)

    def _thread_lock_acquire(self, lock):
        with lock:
            self.active += 1
            self.assertEqual(self.active, 1)
            print "got lock"
            time.sleep(0)
            self.active -= 1


