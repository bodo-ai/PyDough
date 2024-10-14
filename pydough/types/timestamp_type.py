"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import pytz


class TimestampType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, precision, tz=None):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(
                f"Invalid precision for TimestampType: {repr(precision)}"
            )
        if not (tz is None or (isinstance(tz, str) and tz in pytz.all_timezones_set)):
            raise PyDoughTypeException(
                f"Invalid precision for TimestampType: {repr(precision)}"
            )
        self.precision = precision
        self.tz = tz

    def __repr__(self):
        return f"TimestampType({self.precision},{repr(self.tz)})"

    def as_json_string(self):
        if self.tz is None:
            return f"timestamp[{self.precision}]"
        else:
            return f"timestamp[{self.precision},{self.tz}]"
