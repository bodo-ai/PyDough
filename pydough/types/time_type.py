"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re
from typing import Optional


class TimeType(PyDoughType):
    """
    The PyDough type representing time values.
    """

    def __init__(self, precision: int):
        if not isinstance(precision, int) or precision not in range(10):
            raise PyDoughTypeException(f"Invalid precision for TimeType: {precision!r}")
        self._precision: int = precision

    @property
    def precision(self) -> int:
        """
        The precision of the time type, which should be an integer between 0
        and 9. The value indicates how many sub-second decimal places are
        supported in values of the type.
        """
        return self._precision

    def __repr__(self):
        return f"TimeType({self.precision})"

    @property
    def json_string(self) -> str:
        return f"time[{self.precision}]"

    # The string pattern that time types must adhere to.
    type_string_pattern: re.Pattern = re.compile("time\[(\d)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        # Verify that the string matches the time type regex pattern, and
        # extract the precision.
        match: Optional[re.match] = TimeType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        precision: int = int(match.groups(0)[0])
        return TimeType(precision)
