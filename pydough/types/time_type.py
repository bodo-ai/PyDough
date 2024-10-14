"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re


class TimeType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, precision):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(
                f"Invalid precision for TimeType: {repr(precision)}"
            )
        self.precision = precision

    def __repr__(self):
        return f"TimeType({self.precision})"

    def as_json_string(self) -> str:
        return f"time[{self.precision}]"

    type_string_pattern: re.Pattern = re.compile("time[(\d)]")

    def parse_from_string(type_string: str) -> PyDoughType:
        match = TimeType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        precision = int(match.groups[0])
        return TimeType(precision)
