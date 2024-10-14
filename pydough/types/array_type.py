"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re


class ArrayType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, elem_type: PyDoughType):
        if not isinstance(elem_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid component type for ArrayType. Expected a PyDoughType, received: {repr(elem_type)}"
            )
        self.elem_type = elem_type

    def __repr__(self):
        return f"ArrayType({repr(self.elem_type)})"

    def as_json_string(self) -> str:
        return f"array[{self.elem_type.as_json_string()}]"

    type_string_pattern: re.Pattern = re.compile("array\[(.+)\]")

    def parse_from_string(type_string: str) -> PyDoughType:
        from pydough.types import parse_type_from_string

        match = ArrayType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        try:
            elem_type = parse_type_from_string(match.groups(0)[0])
        except PyDoughTypeException:
            return None
        return ArrayType(elem_type)
