#!/usr/bin/env python

from functools import partial

import zookeeper

from azookeeper.sync import get_sync_strategy


class ZookeeperClient(object):
    """A gevent-friendly wrapper of the Apache Zookeeper zkpython client

    TODO lots to do:
    * handling of ZK client session events
    * disconnected state handling
    * tests
    * the rest of the operations
    """
    def __init__(self, hosts, timeout):
        self._hosts = hosts
        self._timeout = timeout

        self._sync = get_sync_strategy()

        self._connected = False
        self._connected_async_result = self._sync.async_result()

    @property
    def connected(self):
        return self._connected

    def _wrap_callback(self, func):
        return partial(self._sync.dispatch_callback, func)

    def _session_watcher(self, handle, type, state, path):
        #TODO fo real
        self._connected = True
        if not self._connected_async_result.ready():
            self._connected_async_result.set()

    def connect(self):
        #TODO connect timeout? async version?

        cb = self._wrap_callback(self._session_watcher)
        self._handle = zookeeper.init(self._hosts, cb, self._timeout)
        self._connected_async_result.wait()

    def close(self):
        if self._connected:
            code = zookeeper.close(self._handle)
            self._handle = None
            self._connected = False
            if code != zookeeper.OK:
                raise err_to_exception(code)

    def add_auth_async(self, scheme, credential):
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.add_auth(self._handle, scheme, credential, callback)
        return async_result

    def add_auth(self, scheme, credential):
        return self.add_auth_async(scheme, credential).get()

    def create_async(self, path, value, acl, ephemeral=False, sequence=False):
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

        return self.create_async(path, value, acl, ephemeral, sequence).get()

    def get_async(self, path, watcher=None):
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)
        watcher_callback = self._wrap_callback(watcher)

        zookeeper.aget(self._handle, path, watcher_callback, callback)
        return async_result

    def get(self, path, watcher=None):
        return self.get_async(path, watcher).get()

    def get_children_async(self, path, watcher=None):
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)
        watcher_callback = self._wrap_callback(watcher)

        zookeeper.aget_children(self._handle, path, watcher_callback, callback)
        return async_result

    def get_children(self, path, watcher=None):
        return self.get_children_async(path, watcher).get()

    def set_async(self, path, data, version=-1):
        async_result = self._sync.async_result()
        callback = partial(_generic_callback, async_result)

        zookeeper.aset(self._handle, path, data, version, callback)
        return async_result

    def set(self, path, data, version=-1):
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
