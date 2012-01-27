from kazoo.recipe.lock import ZooLock


class LeaderElection(object):
    def __init__(self, client, path):
        self.lock = ZooLock(client, path)

    def run(self, func, *args, **kwargs):
        if not callable(func):
            raise ValueError("leader function is not callable")

        with self.lock:
            func(*args, **kwargs)

