"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import pytz
import re
from typing import Optional


class TimestampType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, precision: int, tz: Optional[str] = None):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(
                f"Invalid precision for TimestampType: {precision!r}"
            )
        if not (tz is None or (isinstance(tz, str) and tz in pytz.all_timezones_set)):
            raise PyDoughTypeException(f"Invalid timezone for TimestampType: {tz!r}")
        self._precision: int = precision
        self._tz: Optional[str] = tz

    @property
    def precision(self) -> int:
        return self._precision

    @property
    def tz(self) -> Optional[str]:
        return self._tz

    def __repr__(self):
        return f"TimestampType({self.precision!r},{self.tz!r})"

    @property
    def json_string(self) -> str:
        if self.tz is None:
            return f"timestamp[{self.precision}]"
        else:
            return f"timestamp[{self.precision},{self.tz}]"

    # The string patterns that timestamp types must adhere to. Each timestamp
    # type string must match one of these patterns.
    type_string_pattern_no_tz: re.Pattern = re.compile("timestamp\[(\d)\]")
    type_string_pattern_with_tz: re.Pattern = re.compile("timestamp\[(\d),(.*)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        # Verify that the string matches one of the timestamp type regex
        # patterns, extracting the precision and timezone (if present).
        match_no_tz: Optional[re.match] = (
            TimestampType.type_string_pattern_no_tz.fullmatch(type_string)
        )
        match_with_tz: Optional[re.match] = (
            TimestampType.type_string_pattern_with_tz.fullmatch(type_string)
        )
        if match_no_tz is not None:
            precision: int = int(match_no_tz.groups(0)[0])
            tz: Optional[str] = None
        elif match_with_tz is not None:
            precision: int = int(match_with_tz.groups(0)[0])
            tz: Optional[str] = match_with_tz.groups(0)[1]
        else:
            return None
        return TimestampType(precision, tz)
