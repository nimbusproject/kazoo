import fcntl
import os

import gevent
import gevent.event
from gevent.timeout import Timeout

# get the unpatched thread module
import azookeeper.sync.util
realthread = azookeeper.sync.util.get_realthread()


# this is inspired by the threadpool recipe in the geventutil package:
#   https://bitbucket.org/denis/gevent-playground/src/tip/geventutil/threadpool.py

def _pipe():
    r, w = os.pipe()
    fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
    return r, w


#noinspection PyUnusedLocal
def _pipe_read_callback(event, eventtype):
    try:
        os.read(event.fd, 1)
    except EnvironmentError:
        pass


class _AsyncResult(gevent.event.AsyncResult):
    def __init__(self, pipe):
        self._pipe = pipe
        gevent.event.AsyncResult.__init__(self)

    def set_exception(self, exception):
        gevent.event.AsyncResult.set_exception(self, exception)
        os.write(self._pipe, '\0')

    def set(self, value=None):
        gevent.event.AsyncResult.set(self, value)
        os.write(self._pipe, '\0')


class _Event(gevent.event.Event):
    def __init__(self, pipe):
        self._pipe = pipe
        gevent.event.Event.__init__(self)

    def set(self):
        gevent.event.Event.set(self)
        os.write(self._pipe, '\0')


class GeventSyncStrategy(object):

    name = "gevent"
    timeout_error = Timeout

    def __init__(self):
        self._pipe_read, self._pipe_write = _pipe()

        self._event = gevent.core.event(
            gevent.core.EV_READ | gevent.core.EV_PERSIST,
        self._pipe_read, _pipe_read_callback)
        self._event.add()

        self._cb_fun = None
        self._cb_args = None

        # this Event is waited on by a greenlet and set by an OS thread.
        # it is set when a new callback is dispatched by zookeeper.
        self._cb_event = _Event(self._pipe_write)

        # this is a real (unpatched) Lock object that is waited on by the OS
        # thread after it signals a callback to the greenlet. The greenlet
        # acquires the lock and releases it when the callback is complete.
        self._cb_lock = realthread.allocate_lock()

        self._cb_greenlet = gevent.spawn(self._cb_thread)

    def __del__(self):
        if getattr(self, "_cb_greenlet", None):
            try:
                gevent.kill(self._cb_greenlet)
            except Exception:
                pass

        # attempt to clean up the FD from the gevent hub
        if getattr(self, "_event", None):
            try:
                self._event.cancel()
            except Exception:
                pass

    def _cb_thread(self):
        """Greenlet function that runs callbacks in the gevent loop

        1. Waits on a gevent-friendly Event which is set by the ZK callback thread
        2. Runs the callback
        3. Sets a condition variable which is being waited on by the ZK thread
        """

        while True:
            self._cb_event.wait()

            # this should not be able to block
            with self._cb_lock:

                fun = self._cb_fun
                args = self._cb_args


                try:
                    fun(*args)
                except Exception:
                    pass

                self._cb_fun = None
                self._cb_args = None
                self._cb_event.clear()

    def async_result(self):
        return _AsyncResult(self._pipe_write)

    def dispatch_callback(self, fun, *args):
        """Run a callback under gevent

        This is called by an OS thread (not the gevent one) and it runs the
        callback on the gevent thread. It returns after the callback finishes.

        This method itself is not thread-safe: it cannot be called by more
        than one thread at a time.

        @param fun: callable to run on gevent thread
        @param args: args to pass to function
        """

        self._cb_fun = fun
        self._cb_args = args
        self._cb_event.set()
        while self._cb_event.is_set():
            with self._cb_lock:
                pass

