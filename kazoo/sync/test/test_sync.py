import unittest
import time
import threading

import kazoo.sync
import kazoo.sync.util

realthread = kazoo.sync.util.get_realthread()

class SyncStrategyTests(unittest.TestCase):
    def setUp(self):
        self.sync = kazoo.sync.get_sync_strategy()
        print "Sync strategy is %s" % self.sync.name

    def tearDown(self):
        del self.sync # bad?

    def test_async_result(self):
        """Set AsyncResult value from another OS thread
        """
        async_result = self.sync.async_result()

        realthread.start_new_thread(thread_set_async_result,
            (async_result, "hats"))

        result = async_result.get()
        self.assertEqual(result, "hats")

    def test_async_result_exception(self):
        """Set AsyncResult exception from another OS thread
        """
        async_result = self.sync.async_result()

        exc = KeyError("hats")
        realthread.start_new_thread(thread_set_async_result,
            (async_result, None, exc))

        try:
            result = async_result.get()
        except KeyError, e:
            self.assertEqual(e, exc)
        else:
            self.fail("Expected exception")

    def test_dispatch_callback(self):
        """Dispatch many callbacks serially from another OS thread
        """

        # goal with this test is to ensure that a series of callbacks are
        # called in sequence and never in parallel. That is, dispatch_callback
        # doesn't return until the callback completes execution. The way the
        # test is currently written could still potentially pass even if this
        # condition does not hold, due to a race. Doing many dispatches seems
        # to flush this out, but it is not ideal.

        callbacks = 1000
        results = []
        lock = threading.Lock()
        done = threading.Event()

        def fun(i):
            self.assertTrue(lock.acquire(False), "failed to get lock")
            try:
                time.sleep(0) # try to force a thread/greenlet yield
                results.append(i)
                if len(results) == callbacks:
                    done.set()
            finally:
                lock.release()

        realthread.start_new_thread(thread_dispatch_callbacks,
            (self.sync, fun, callbacks))
        done.wait(10)
        self.assertEqual(results, range(callbacks))

def thread_set_async_result(async_result, value=None, exception=None):
    if exception:
        async_result.set_exception(exception)
    else:
        async_result.set(value)

def thread_dispatch_callbacks(sync, fun, count=1):
    for i in range(count):
        sync.dispatch_callback(fun, i)
