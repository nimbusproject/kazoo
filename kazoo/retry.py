from zookeeper import ConnectionLossException, OperationTimeoutException, \
    SessionExpiredException, SessionMovedException


class ForceRetryError(Exception):
    """Raised when some recipe logic wants to force a retry
    """

class KazooRetry(object):
    """Helper for retrying a method in the face of specific exceptions
    """

    ALLOWED_EX = (ConnectionLossException, OperationTimeoutException,
        SessionMovedException, SessionExpiredException, ForceRetryError)

    def __init__(self, max_tries=None):
        self.max_tries = max_tries

    def run(self, func, *args, **kwargs):
        self(func, *args, **kwargs)

    def __call__(self, func, *args, **kwargs):
        tries = 1

        while True:
            try:
                return func(*args, **kwargs)

            except self.ALLOWED_EX:
                if self.max_tries and tries == self.max_tries:
                    raise
                tries += 1

