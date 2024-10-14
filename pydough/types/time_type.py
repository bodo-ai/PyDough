from .pydough_type import PyDoughType
from .errors import PyDoughTypeException


class TimeType(PyDoughType):
    def __init__(self, precision):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(
                f"Invalid precision for TimeType: {repr(precision)}"
            )
        self.precision = precision

    def __repr__(self):
        return f"TimeType({self.precision})"

    def as_json_string(self):
        return f"time[{self.precision}]"
