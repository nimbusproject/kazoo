
_realthread = None


def get_realthread():
    """Get the real Python thread module, regardless of any monkeypatching
    """
    global _realthread
    if _realthread:
        return _realthread

    import imp
    fp, pathname, description = imp.find_module('thread')
    try:
        return imp.load_module('realthread', fp, pathname, description)
    finally:
        if fp:
            fp.close()
