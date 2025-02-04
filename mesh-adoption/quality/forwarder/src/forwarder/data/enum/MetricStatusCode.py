from enum import Enum

class MetricStatusCode(Enum):
    NEW = "NEW"
    LOCKED = "LOCKED"
    SUCCESS = "SUCCESS"
    ERR_EMPTY_BUSINESS_DOMAIN = "ERR_EMPTY_BUSINESS_DOMAIN"
    ERR_WRONG_BUSINESS_DOMAIN = "ERR_WRONG_BUSINESS_DOMAIN"
    ERR_BLINDATA_SUITE_NOT_FOUND = "ERR_BLINDATA_SUITE_NOT_FOUND"
    ERR_FAILED_BLINDATA_CHECK_CREATION = "ERR_FAILED_BLINDATA_CHECK_CREATION"
    ERR_BLINDATA = "ERR_BLINDATA"

    def __init__(self, code):
        self.code = code