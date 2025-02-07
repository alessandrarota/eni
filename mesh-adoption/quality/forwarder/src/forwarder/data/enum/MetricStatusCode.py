from enum import Enum

class MetricStatusCode(Enum):
    NEW = "NEW"
    LOCKED = "LOCKED"
    SUCCESS = "SUCCESS"
    ERR_CHECK_NOT_FOUND = "ERR_CHECK_NOT_FOUND"
    ERR_BLINDATA = "ERR_BLINDATA"

    def __init__(self, code):
        self.code = code