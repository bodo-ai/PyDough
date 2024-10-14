"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re


class MapType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, key_type: PyDoughType, val_type: PyDoughType):
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid key type for ArrayType. Expected a PyDoughType, received: {repr(key_type)}"
            )
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid value type for ArrayType. Expected a PyDoughType, received: {repr(val_type)}"
            )
        self.key_type = key_type
        self.val_type = val_type

    def __repr__(self):
        return f"MapType({repr(self.key_type)},{repr(self.val_type)})"

    def as_json_string(self) -> str:
        return f"map[{self.key_type.as_json_string()},{self.val_type.as_json_string()}]"

    type_string_pattern: re.Pattern = re.compile("map[(.+),(.+)]")

    def parse_from_string(type_string: str) -> PyDoughType:
        from pydough.types import parse_type_from_string

        match = MapType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        try:
            key_type = parse_type_from_string(match.groups[0])
            val_type = parse_type_from_string(match.groups[1])
        except PyDoughTypeException:
            return None
        return MapType(key_type, val_type)
