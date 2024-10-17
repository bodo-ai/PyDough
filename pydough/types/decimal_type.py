"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re


class DecimalType(PyDoughType):
    """
    TODO: add class docstring
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
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return f"DecimalType({self.precision},{self.scale})"

    @property
    def json_string(self) -> str:
        return f"decimal[{self.precision},{self.scale}]"

    type_string_pattern: re.Pattern = re.compile("decimal\[(\d{1,2}),(\d{1,2})\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType:
        match = DecimalType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        precision = int(match.groups(0)[0])
        scale = int(match.groups(0)[1])
        return DecimalType(precision, scale)
