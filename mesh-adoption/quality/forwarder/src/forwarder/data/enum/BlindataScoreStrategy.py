from enum import Enum

class BlindataScoreStrategy(Enum):
    PERCENTAGE = "PERCENTAGE"
    DISTANCE = "DISTANCE"

    def __init__(self, code):
        self.code = code