"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import pytz
import re


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

    def as_json_string(self) -> str:
        if self.tz is None:
            return f"timestamp[{self.precision}]"
        else:
            return f"timestamp[{self.precision},{self.tz}]"

    type_string_pattern_no_tz: re.Pattern = re.compile("timestamp[(\d)]")
    type_string_pattern_with_tz: re.Pattern = re.compile("timestamp[(\d),(.*)]")

    def parse_from_string(type_string: str) -> PyDoughType:
        match_no_tz = TimestampType.type_string_pattern.fullmatch(type_string)
        match_with_tz = TimestampType.type_string_pattern.fullmatch(type_string)
        if match_no_tz is not None:
            precision = int(match_no_tz.groups[0])
            tz = None
        elif match_with_tz is None:
            precision = int(match_with_tz.groups[0])
            tz = match_with_tz.groups[1]
        else:
            return None
        return TimestampType(precision, tz)
