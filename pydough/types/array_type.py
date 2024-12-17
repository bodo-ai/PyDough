"""
Definition of the PyDough type for arrays.
"""

__all__ = ["ArrayType"]

import re

from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class ArrayType(PyDoughType):
    """
    The PyDough type representing an array of data.
    """

    def __init__(self, elem_type: PyDoughType):
        if not isinstance(elem_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid component type for ArrayType. Expected a PyDoughType, received: {elem_type!r}"
            )
        self._elem_type = elem_type

    @property
    def elem_type(self) -> PyDoughType:
        """
        The PyDough type of the elements inside the array.
        """
        return self._elem_type

    def __repr__(self):
        return f"ArrayType({self.elem_type!r})"

    @property
    def json_string(self) -> str:
        return f"array[{self.elem_type.json_string}]"

    # The string pattern that array types must adhere to.
    type_string_pattern: re.Pattern = re.compile(r"array\[(.+)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        from pydough.types import parse_type_from_string

        # Verify that the string matches the array type regex pattern, extracting
        # the element type string.
        match: re.Match | None = ArrayType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None

        # Attempt to parse the element type string as a PyDough type. If the attempt
        # fails, then the parsing fails.
        try:
            elem_type: PyDoughType = parse_type_from_string(str(match.groups(0)[0]))
        except PyDoughTypeException:
            return None
        return ArrayType(elem_type)
