"""
Definition of the PyDough type for fixed point decimals.
"""

__all__ = ["DecimalType"]

import re

from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class DecimalType(PyDoughType):
    """
    The PyDough type representing fixed point numbers with a precision and
    scale.
    """

    def __init__(self, precision, scale):
        if not isinstance(precision, int) or precision not in range(1, 39):
            raise PyDoughTypeException(
                f"Invalid precision for DecimalType: {precision!r}"
            )
        if not isinstance(scale, int) or scale not in range(precision):
            raise PyDoughTypeException(
                f"Invalid scale for DecimalType with precision {precision}: {scale!r}"
            )
        self._precision: int = precision
        self._scale: int = scale

    @property
    def precision(self) -> int:
        """
        The number of digits that can be stored in the type. Must be an
        integer between 1 and 38 (inclusive).
        """
        return self._precision

    @property
    def scale(self) -> int:
        """
        The number of digits that are to the right hand side of the decimal
        point. Must be between 0 and precision (exclusive of the precision).
        """
        return self._scale

    def __repr__(self):
        return f"DecimalType({self.precision},{self.scale})"

    @property
    def json_string(self) -> str:
        return f"decimal[{self.precision},{self.scale}]"

    # The string pattern that all decimal types must adhere to.
    type_string_pattern: re.Pattern = re.compile(r"decimal\[(\d{1,2}),(\d{1,2})\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        # Verify that the string matches the time type regex pattern, and
        # extract the precision and scale.
        match = DecimalType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        precision = int(match.groups(0)[0])
        scale = int(match.groups(0)[1])
        return DecimalType(precision, scale)
