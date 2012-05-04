import logging
from os.path import split

from kazoo.zkclient import ZooKeeperClient, WatchedEvent, KeeperState,\
    EventType, NodeExistsException, NoNodeException
from kazoo.retry import KazooRetry

log = logging.getLogger(__name__)


class KazooState(object):
    """High level connection state values

    States inspired by Netflix Curator
    """

    # The connection has been lost but may be recovered.
    # We should operate in a "safe mode" until then.
    SUSPENDED = "SUSPENDED"

    # The connection is alive and well.
    CONNECTED = "CONNECTED"

    # The connection has been confirmed dead.
    LOST = "LOST"


class KazooClient(object):
    """Higher-level ZooKeeper client.

    Supports retries, namespacing, easier state monitoring; saves kittens.
    """

    def __init__(self, hosts, namespace=None, timeout=10.0, max_retries=None):
        # remove any trailing slashes
        if namespace:
            namespace = namespace.rstrip('/')
        if namespace:
            validate_path(namespace)
        self.namespace = namespace

        self._needs_ensure_path = bool(namespace)

        self.zk = ZooKeeperClient(hosts, watcher=self._session_watcher,
            timeout=timeout)
        self.retry = KazooRetry(max_retries)

        self.state = KazooState.LOST
        self.state_listeners = set()

    def _session_watcher(self, event):
        """called by the underlying ZK client when the connection state changes
        """

        if event.type != EventType.SESSION:
            return

        if event.state == KeeperState.CONNECTED:
            self._make_state_change(KazooState.CONNECTED)
        elif event.state in (KeeperState.AUTH_FAILED,
                             KeeperState.EXPIRED_SESSION):
            self._make_state_change(KazooState.LOST)
        elif event.state == KeeperState.CONNECTING:
            self._make_state_change(KazooState.SUSPENDED)

    def _make_state_change(self, state):
        # skip if state is current
        if self.state == state:
            return

        self.state = state

        listeners = list(self.state_listeners)
        for listener in listeners:
            try:
                listener(state)
            except Exception:
                log.exception("Error in connection state listener")

    def _assure_namespace(self):
        if self._needs_ensure_path:
            self.ensure_path('/')
            self._needs_ensure_path = False

    def add_listener(self, listener):
        """Add a function to be called for connection state changes
        """
        if not (listener and callable(listener)):
            raise ValueError("listener must be callable")
        self.state_listeners.add(listener)

    def remove_listener(self, listener):
        """Remove a listener function
        """
        self.state_listeners.discard(listener)

    @property
    def client_id(self):
        return self.zk.client_id

    def connect(self, timeout=None):
        """Initiate connection to ZK

        @param timeout: time in seconds to wait for connection to succeed
        """
        self.zk.connect(timeout=timeout)

    def close(self):
        """Disconnect from ZooKeeper
        """
        self.zk.close()

    def create(self, path, value, acl=None, ephemeral=False, sequence=False,
            makepath=False):
        """Create a ZNode

        @param path: path of node
        @param value: initial value of node
        @param acl: permissions for node
        @param ephemeral: boolean indicating whether node is ephemeral (tied to this session)
        @param sequence: boolean indicating whether path is suffixed with a unique index
        @param makepath: boolean indicating whether to create path if it doesn't exist
        @return: real path of the new node
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        try:
            realpath = self.zk.create(path, value, acl=acl,
                ephemeral=ephemeral, sequence=sequence)

        except NoNodeException:
            # some or all of the parent path doesn't exist. if makepath is set
            # we will create it and retry. If it fails again, someone must be
            # actively deleting ZNodes and we'd best bail out.
            if not makepath:
                raise

            parent, _ = split(path)

            # using the inner call directly because path is already namespaced
            self._inner_ensure_path(parent)

            # now retry
            realpath = self.zk.create(path, value, acl=acl,
                ephemeral=ephemeral, sequence=sequence)

        return self.unnamespace_path(realpath)

    def exists(self, path, watch=None):
        """Check if a node exists

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return stat of the node if it exists, else None
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        if watch:
            watch = self.unnamespace_watch(watch)
        return self.zk.exists(path, watch)

    def get(self, path, watch=None):
        """Get the value of a node

        @param path: path of node
        @param watch: optional watch callback to set for future changes to this path
        @return tuple (value, stat) of node
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        if watch:
            watch = self.unnamespace_watch(watch)
        return self.zk.get(path, watch)

    def get_children(self, path, watch=None):
        """Get a list of child nodes of a path

        @param path: path of node to list
        @param watch: optional watch callback to set for future changes to this path
        @return: list of child node names
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        if watch:
            watch = self.unnamespace_watch(watch)
        return self.zk.get_children(path, watch)

    def set(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the supplied
        version (and the supplied version is not -1), a BadVersionException
        will be raised.

        @param path: path of node to set
        @param data: new data value
        @param version: version of node being updated, or -1
        @return: updated node stat
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        return self.zk.set(path, data, version)

    def delete(self, path, version=-1):
        """Delete a node

        @param path: path of node to delete
        @param version: version of node to delete, or -1 for any
        """
        self._assure_namespace()

        path = self.namespace_path(path)
        return self.zk.delete(path, version)

    def ensure_path(self, path):
        """Recursively create a path if it doesn't exist
        """
        path = self.namespace_path(path)
        self._inner_ensure_path(path)

    def _inner_ensure_path(self, path):
        if self.zk.exists(path):
            return

        parent, node = split(path)

        if parent != "/":
            self._inner_ensure_path(parent)
        try:
            self.zk.create(path, "")
        except NodeExistsException:
            # someone else created the node. how sweet!
            pass

    def recursive_delete(self, path):
        """Recursively delete a ZNode and all of its children
        """
        children = self.get_children(path)
        if children:
            for child in children:
                self.recursive_delete(path + "/" + child)
        try:
            self.delete(path)
        except NoNodeException:
            pass

    def with_retry(self, func, *args, **kwargs):
        """Run a method repeatedly in the face of transient ZK errors
        """
        return self.retry(func, *args, **kwargs)

    def namespace_path(self, path):
        if not self.namespace:
            return path
        validate_path(path)
        path = self.namespace + path
        path = path.rstrip('/')
        return path

    def unnamespace_path(self, path):
        if not self.namespace:
            return path
        if path.startswith(self.namespace):
            return path[len(self.namespace):]

    def unnamespace_watch(self, watch):
        def fixed_watch(event):
            if event.path:
                # make a new event with the fixed path
                path = self.unnamespace_path(event.path)
                event = WatchedEvent(event.type, event.state, path)

            watch(event)

        return fixed_watch

def validate_path(path):
    if not path.startswith('/'):
        raise ValueError("invalid path '%s'. must start with /" % path)
