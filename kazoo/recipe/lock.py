import threading
import uuid

from kazoo.retry import ForceRetryError

#noinspection PyArgumentList
class ZooLock(object):
    _LOCK_NAME = '_lock_'

    def __init__(self, client, path):
        """
        @type client ZooKeeperClient
        """
        self.client = client
        self.path = path

        self.condition = threading.Condition()

        # props to Netflix Curator for this trick. It is possible for our
        # create request to succeed on the server, but for a failure to
        # prevent us from getting back the full path name. We prefix our
        # lock name with a uuid and can check for its presence on retry.
        self.prefix = uuid.uuid4().hex + self._LOCK_NAME
        self.create_path = self.path + "/" + self.prefix

        self.create_tried = False

        self.is_acquired = False

    def acquire(self):
        """Acquire the mutex, blocking until it is obtained
        """

        try:
            self.client.retry(self._inner_acquire)

            self.is_acquired = True

        except Exception:
            # if we did ultimately fail, attempt to clean up
            self._best_effort_cleanup()

    def _inner_acquire(self):
        node = None
        if self.create_tried:
            node = self._find_node()
        else:
            self.create_tried = True

        if not node:
            node = self.client.create(self.create_path, "",
                ephemeral=True, sequence=True)

        self.node = node

        while True:
            children = self._get_sorted_children()

            our_index = children.index(node)

            if our_index == -1:
                # somehow we aren't in the election -- probably we are
                # recovering from a session failure and our ephemeral
                # node was removed
                raise ForceRetryError()

            #noinspection PySimplifyBooleanCheck
            if our_index == 0:
                # we are leader
                return True

            # otherwise we are in the mix. watch predecessor and bide our time
            predecessor = self.path + "/" + children[our_index-1]
            with self.condition:
                if self.client.exists(predecessor, self._watch_predecessor):
                    self.condition.wait()

    def _watch_predecessor(self, type, state, path):
        with self.condition:
            self.condition.notify_all()

    def _get_sorted_children(self):
        children = self.client.get_children(self.path)

        # can't just sort directly: the node names are prefixed by uuids
        lockname = self._LOCK_NAME
        children.sort(key=lambda c: c[c.find(lockname) + len(lockname):])
        return children

    def _find_node(self):
        children = self.client.get_children(self.path)
        for child in children:
            if child.startswith(self.prefix):
                return child
        return None

    def _best_effort_cleanup(self):
        try:

            node = self._find_node()
            if node:
                self.client.delete(self.path + "/" + node)

        except Exception:
            pass

    def release(self):
        """Release the mutex immediately
        """
        return self.client.retry(self._inner_release)

    def _inner_release(self):
        if not self.is_acquired:
            return False

        self.client.delete(self.path + "/" + self.node)

        self.is_acquired = False
        self.node = None

        return True

    def __enter__(self):
        self.acquire()

    def __exit__(self,exc_type, exc_value, traceback):
        self.release()


  