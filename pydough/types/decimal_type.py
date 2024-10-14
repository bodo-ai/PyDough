"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException


class DecimalType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, precision, scale):
        if not isinstance(precision, int) or precision not in range(39):
            raise PyDoughTypeException(
                f"Invalid precision for DecimalType: {repr(precision)}"
            )
        if not isinstance(scale, int) or scale not in range(precision):
            raise PyDoughTypeException(
                f"Invalid scale for DecimalType with precision {precision}: {repr(scale)}"
            )
        self.precision = precision
        self.scale = scale

    def __repr__(self):
        return f"DecimalType({self.precision},{repr(self.scale)})"

    def as_json_string(self):
        return f"decimal[{self.precision},{repr(self.scale)}]"
