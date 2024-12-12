

class FlowException(Exception):
    pass

class ErrorThresholdExceeded(FlowException):
    pass

class FlowAborted(FlowException):
    pass

class TaskTimeout(FlowException):
    pass

class TaskDependencyError(FlowException):
    pass