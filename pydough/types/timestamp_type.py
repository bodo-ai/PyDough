"""
Definition of the PyDough type for timestamp types.
"""

__all__ = ["TimestampType"]

import re

import pytz

from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class TimestampType(PyDoughType):
    """
    The PyDough type for timestamps with a precision and (optional) time zone.
    """

    def __init__(self, precision: int, tz: str | None = None):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(
                f"Invalid precision for TimestampType: {precision!r}"
            )
        if not (tz is None or (isinstance(tz, str) and tz in pytz.all_timezones_set)):
            raise PyDoughTypeException(f"Invalid timezone for TimestampType: {tz!r}")
        self._precision: int = precision
        self._tz: str | None = tz

    @property
    def precision(self) -> int:
        """
        The precision of the timestamp type, which should be an integer between
        0 and 9. The value indicates how many sub-second decimal places are
        supported in values of the timestamp type.
        """
        return self._precision

    @property
    def tz(self) -> str | None:
        """
        The timezone of the timestamp type, if one exists.
        """
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
    type_string_pattern_no_tz: re.Pattern = re.compile(r"timestamp\[(\d)\]")
    type_string_pattern_with_tz: re.Pattern = re.compile(r"timestamp\[(\d),(.*)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        # Verify that the string matches one of the timestamp type regex
        # patterns, extracting the precision and timezone (if present).
        match_no_tz: re.Match | None = (
            TimestampType.type_string_pattern_no_tz.fullmatch(type_string)
        )
        match_with_tz: re.Match | None = (
            TimestampType.type_string_pattern_with_tz.fullmatch(type_string)
        )
        tz: str | None = None
        precision: int
        if match_no_tz is not None:
            precision = int(match_no_tz.groups(0)[0])
        elif match_with_tz is not None:
            precision = int(match_with_tz.groups(0)[0])
            tz = str(match_with_tz.groups(0)[1])
        else:
            return None
        return TimestampType(precision, tz)
