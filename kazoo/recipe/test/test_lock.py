import unittest
import uuid
import threading

from kazoo.recipe.lock import ZooLock
from kazoo.test import get_client_or_skip

class ZooLockTests(unittest.TestCase):

    def setUp(self):
        self._c = get_client_or_skip()
        self._c.connect()
        self.lockpath = "/" + uuid.uuid4().hex
        self._c.create(self.lockpath, "")

        self.semaphore = threading.Semaphore()

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

        threads = [threading.Thread(target=self._thread_lock_acquire,
            args=(lock,)) for lock in locks]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        #TODO this test needs work

    def _thread_lock_acquire(self, lock):
        with lock:
            self.semaphore.release()
            print "got lock"
            self.semaphore.acquire()


