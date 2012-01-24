def get_sync_strategy():
    """Detects the current async environment and returns a sync helper object
    """
    import sys

    if "gevent" in sys.modules:
        from azookeeper.sync import sync_gevent
        return sync_gevent.GeventSyncStrategy()

    else:
        from azookeeper.sync import sync_threading
        return sync_threading.ThreadingSyncStrategy()


