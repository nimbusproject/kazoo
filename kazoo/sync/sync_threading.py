import threading

# sentinal object
_NONE = object()


class TimeoutError(Exception):
    pass


class _AsyncResult(object):
    """A one-time event that stores a value or an exception.

    Uses gevent's AsyncResult API

    """
    def __init__(self):
        self.value = None
        self._exception = _NONE
        self._condition = threading.Condition()

    def ready(self):
        """Return true if and only if it holds a value or an exception"""
        return self._exception is not _NONE

    def successful(self):
        """Return true if and only if it is ready and holds a value"""
        return self._exception is None

    @property
    def exception(self):
        if self._exception is not _NONE:
            return self._exception

    def set(self, value=None):
        """Store the value. Wake up the waiters.
        """
        with self._condition:
            self.value = value
            self._exception = None

            self._condition.notify_all()

    def set_exception(self, exception):
        """Store the exception. Wake up the waiters.
        """
        with self._condition:
            self._exception = exception

            self._condition.notify_all()

    def get(self, block=True, timeout=None):
        """Return the stored value or raise the exception.

        If there is no value raises Timeout
        """
        with self._condition:
            if self._exception is not _NONE:
                if self._exception is None:
                    return self.value
                raise self._exception
            elif block:
                self._condition.wait(timeout)
                if self._exception is not _NONE:
                    if self._exception is None:
                        return self.value
                    raise self._exception

            # if we get to this point we timeout
            raise TimeoutError()

    def get_nowait(self):
        """Return the value or raise the exception without blocking.

        If nothing is available, raises TimeoutError
        """
        return self.get(block=False)


class ThreadingSyncStrategy(object):
    name = "threading"
    timeout_error = TimeoutError

    def async_result(self):
        return _AsyncResult()

    def dispatch_callback(self, fun, *args):

        #directly run method in the current thread
        fun(*args)
  