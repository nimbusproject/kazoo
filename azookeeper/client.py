#!/usr/bin/env python

from functools import partial

import zookeeper

from azookeeper.sync import get_sync_strategy

ZK_OPEN_ACL_UNSAFE = {"perms": zookeeper.PERM_ALL, "scheme": "world",
                       "id": "anyone"}

class ZookeeperClient(object):
    """A gevent-friendly wrapper of the Apache Zookeeper zkpython client

    TODO lots to do:
    * handling of ZK client session events
    * disconnected state handling
    * tests
    * the rest of the operations
    """
    def __init__(self, hosts, timeout=10000):
        self._hosts = hosts
        self._timeout = timeout

        self._sync = get_sync_strategy()

        self._connected = False
        self._connected_async_result = self._sync.async_result()
        self._connection_timed_out = False

    @property
    def connected(self):
        return self._connected

    def _wrap_callback(self, func):
        return partial(self._sync.dispatch_callback, func)

    def _session_callback(self, handle, type, state, path):
        if state == zookeeper.CONNECTED_STATE:
            self._connected = True
        elif state == zookeeper.CONNECTING_STATE:
            self._connected = False

        if not self._connected_async_result.ready():
            #close the connection if we already timed out
            if self._connection_timed_out and self._connected:
                self.close()
            else:
                self._connected_async_result.set()

    def connect_async(self):
        """Asynchronously initiate connection to ZK

        @return: AsyncResult object set on connection success
        @rtype AsyncResult
        """

        cb = self._wrap_callback(self._session_callback)
        self._handle = zookeeper.init(self._hosts, cb, self._timeout)
        return self._connected_async_result

    def connect(self, timeout=None):
        """Initiate connection to ZK

        @param timeout: time in seconds to wait for connection to succeed
        """
        async_result = self.connect_async()
        try:
            async_result.wait(timeout=timeout)
        except self._sync.timeout_error:
            self._connection_timed_out = True
            raise

    def close(self):
        """Disconnect from ZooKeeper
        """
        if self._connected:
            code = zookeeper.close(self._handle)
            self._handle = None
            self._connected = False
            if code != zookeeper.OK:
                raise err_to_exception(code)

    def add_auth_async(self, scheme, credential):
        """Asynchronously send credentials to server

        @param scheme: authentication scheme (default supported: "digest")
        @param credential: the credential -- value depends on scheme
        @return: AsyncResult object set on completion
        @rtype AsyncResult
        """
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.add_auth(self._handle, scheme, credential, callback)
        return async_result

    def add_auth(self, scheme, credential):
        """Send credentials to server

        @param scheme: authentication scheme (default supported: "digest")
        @param credential: the credential -- value depends on scheme
        """
        return self.add_auth_async(scheme, credential).get()

    def create_async(self, path, value, acl=(ZK_OPEN_ACL_UNSAFE,),
                     ephemeral=False, sequence=False):
        """Asynchronously create a ZNode

        @param path: path of node
        @param value: initial value of node
        @param acl: permissions for node
        @param ephemeral: boolean indicating whether node is ephemeral (tied to this session)
        @param sequence: boolean indicating whether path is suffixed with a unique index
        @return: AsyncResult object set on completion with the real path of the new node
        @rtype AsyncResult
        """
        flags = 0
        if ephemeral:
            flags |= zookeeper.EPHEMERAL
        if sequence:
            flags |= zookeeper.SEQUENCE

        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.acreate(self._handle, path, value, acl, flags, callback)
        return async_result

    def create(self, path, value, acl, ephemeral=False, sequence=False):
        """Create a ZNode

        @param path: path of node
        @param value: initial value of node
        @param acl: permissions for node
        @param ephemeral: boolean indicating whether node is ephemeral (tied to this session)
        @param sequence: boolean indicating whether path is suffixed with a unique index
        @return: real path of the new node
        """

        return self.create_async(path, value, acl, ephemeral, sequence).get()

    def get_async(self, path, watcher=None):
        """Asynchronously get the value of a node

        @param path: path of node
        @param watcher: optional watch callback to set for future changes to this path
        @return AsyncResult set with tuple (value, stat) of node on success
        @rtype AsyncResult
        """
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)
        watcher_callback = self._wrap_callback(watcher)

        zookeeper.aget(self._handle, path, watcher_callback, callback)
        return async_result

    def get(self, path, watcher=None):
        """Get the value of a node

        @param path: path of node
        @param watcher: optional watch callback to set for future changes to this path
        @return tuple (value, stat) of node
        """
        return self.get_async(path, watcher).get()

    def get_children_async(self, path, watcher=None):
        """Asynchronously get a list of child nodes of a path

        @param path: path of node to list
        @param watcher: optional watch callback to set for future changes to this path
        @return: AsyncResult set with list of child node names on success
        @rtype: AsyncResult
        """
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)
        watcher_callback = self._wrap_callback(watcher)

        zookeeper.aget_children(self._handle, path, watcher_callback, callback)
        return async_result

    def get_children(self, path, watcher=None):
        """Get a list of child nodes of a path

        @param path: path of node to list
        @param watcher: optional watch callback to set for future changes to this path
        @return: list of child node names
        """
        return self.get_children_async(path, watcher).get()

    def set_async(self, path, data, version=-1):
        """Set the value of a node

        If the version of the node being updated is newer than the supplied
        version (and the supplied version is not -1), a BadVersionException
        will be raised.

        @param path: path of node to set
        @param data: new data value
        @param version: version of node being updated, or -1
        @return: AsyncResult set with new node stat on success
        @rtype AsyncResult
        """
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.aset(self._handle, path, data, version, callback)
        return async_result

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
        return self.set_async(path, data, version).get()


def _generic_callback(async_result, handle, code, *args):
    if code != zookeeper.OK:
        exc = err_to_exception(code)
        async_result.set_exception(exc)
    else:
        if not args:
            result = None
        elif len(args) == 1:
            result = args[0]
        else:
            result = tuple(args)

        async_result.set(result)


# this dictionary is a port of err_to_exception() from zkpython zookeeper.c
_ERR_TO_EXCEPTION = {
    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.NONODE: zookeeper.NoNodeException,
    zookeeper.NOAUTH: zookeeper.NoAuthException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.NOCHILDRENFOREPHEMERALS: zookeeper.NoChildrenForEphemeralsException,
    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
    zookeeper.SESSIONMOVED: zookeeper.SESSIONMOVED,
}

def err_to_exception(error_code, msg=None):
    """Return an exception object for a Zookeeper error code
    """
    try:
        zkmsg = zookeeper.zerror(error_code)
    except Exception:
        zkmsg = ""

    if msg:
        if zkmsg:
            msg = "%s: %s" % (zkmsg, msg)
    else:
        msg = zkmsg

    exc = _ERR_TO_EXCEPTION.get(error_code)
    if exc is None:

        # double check that it isn't an ok resonse
        if error_code == zookeeper.OK:
            return None

        # otherwise generic exception
        exc = Exception
    return exc(msg)
