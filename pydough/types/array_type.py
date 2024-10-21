"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re
from typing import Optional


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
    type_string_pattern: re.Pattern = re.compile("array\[(.+)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        from pydough.types import parse_type_from_string

        # Verify that the string matches the array type regex pattern, extracting
        # the element type string.
        match: Optional[re.match] = ArrayType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None

        # Attempt to parse the element type string as a PyDough type. If the attempt
        # fails, then the parsing fails.
        try:
            elem_type: PyDoughType = parse_type_from_string(match.groups(0)[0])
        except PyDoughTypeException:
            return None
        return ArrayType(elem_type)
