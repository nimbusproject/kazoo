import unittest
import uuid
import threading
import time

from kazoo.recipe.lock import ZooLock
from kazoo.exceptions import CancelledError
from kazoo.test import get_client_or_skip, until_timeout

class ZooLockTests(unittest.TestCase):

    def setUp(self):
        self._c = get_client_or_skip()
        self._c.connect()
        self.lockpath = "/" + uuid.uuid4().hex

        self.condition = threading.Condition()
        self.active_thread = None
        self.cancelled_threads = []

    def tearDown(self):
        if self.lockpath:
            try:
                self._c.delete(self.lockpath)
            except Exception:
                pass

    def test_lock_one(self):
        c = get_client_or_skip()
        c.connect()

        contender_name = uuid.uuid4().hex
        lock = ZooLock(c, self.lockpath, contender_name)

        event = threading.Event()

        thread = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=(contender_name, lock, event))
        thread.start()

        anotherlock = ZooLock(c, self.lockpath, contender_name)
        contenders = None
        for _ in until_timeout(5):
            contenders = anotherlock.get_contenders()
            if contenders:
                break
            time.sleep(0)

        self.assertEqual(contenders, [contender_name])

        with self.condition:
            while self.active_thread != contender_name:
                self.condition.wait()

        # release the lock
        event.set()

        with self.condition:
            while self.active_thread:
                self.condition.wait()


    def test_lock(self):
        threads = []
        names = ["contender"+str(i) for i in range(5)]

        contender_bits = {}

        for name in names:
            c = get_client_or_skip()
            c.connect()

            e = threading.Event()

            l = ZooLock(c, self.lockpath, name)
            t = threading.Thread(target=self._thread_lock_acquire_til_event,
                args=(name, l, e))
            contender_bits[name] = (t, e)
            threads.append(t)

        # acquire the lock ourselves first to make the others line up
        lock = ZooLock(self._c, self.lockpath, "test")
        lock.acquire()

        for t in threads:
            t.start()

        contenders = None
        # wait for everyone to line up on the lock
        for _ in until_timeout(5):
            contenders = lock.get_contenders()
            if len(contenders) == 6:
                break

        self.assertEqual(contenders[0], "test")
        contenders = contenders[1:]
        remaining = list(contenders)

        # release the lock and contenders should claim it in order
        lock.release()

        for contender in contenders:
            thread, event = contender_bits[contender]

            with self.condition:
                while not self.active_thread:
                    self.condition.wait()
                self.assertEqual(self.active_thread, contender)

            self.assertEqual(lock.get_contenders(), remaining)
            remaining = remaining[1:]

            event.set()

            with self.condition:
                while self.active_thread:
                    self.condition.wait()
            thread.join()


    def test_lock_cancel(self):

        client1 = get_client_or_skip()
        client1.connect()
        event1 = threading.Event()
        lock1 = ZooLock(client1, self.lockpath, "one")
        thread1 = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=("one", lock1, event1))
        thread1.start()

        # wait for this thread to acquire the lock
        with self.condition:
            if not self.active_thread:
                self.condition.wait(5)
                self.assertEqual(self.active_thread, "one")

        client2 = get_client_or_skip()
        client2.connect()
        event2 = threading.Event()
        lock2 = ZooLock(client2, self.lockpath, "two")
        thread2 = threading.Thread(target=self._thread_lock_acquire_til_event,
            args=("two", lock2, event2))
        thread2.start()

        # this one should block in acquire. check that it is a contender
        self.assertEqual(lock2.get_contenders(), ["one", "two"])

        lock2.cancel()
        with self.condition:
            if not "two" in self.cancelled_threads:
                self.condition.wait()
                self.assertIn("two", self.cancelled_threads)

        self.assertEqual(lock2.get_contenders(), ["one"])

        thread2.join()
        event1.set()
        thread1.join()

    def _thread_lock_acquire_til_event(self, name, lock, event):
        try:
            with lock:
                with self.condition:
                    self.assertIsNone(self.active_thread)
                    self.active_thread = name
                    self.condition.notify_all()

                event.wait()

                with self.condition:
                    self.assertEqual(self.active_thread, name)
                    self.active_thread = None
                    self.condition.notify_all()

        except CancelledError:
            with self.condition:
                self.cancelled_threads.append(name)
                self.condition.notify_all()

