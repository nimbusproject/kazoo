from zookeeper import SystemErrorException, RuntimeInconsistencyException,\
    DataInconsistencyException, ConnectionLossException,\
    MarshallingErrorException,UnimplementedException,OperationTimeoutException,\
    BadArgumentsException,ApiErrorException, NoNodeException, NoAuthException,\
    BadVersionException,NoChildrenForEphemeralsException,NodeExistsException,\
    InvalidACLException, AuthFailedException, NotEmptyException,\
    SessionExpiredException, InvalidCallbackException

class CancelledError(Exception):
    """Raised when a process is cancelled by another thread
    """
