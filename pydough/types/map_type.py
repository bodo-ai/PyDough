"""
Definition of the PyDough type for map types.
"""

__all__ = ["MapType"]

import re

from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class MapType(PyDoughType):
    """
    The PyDough type for a map of keys to values.
    """

    def __init__(self, key_type: PyDoughType, val_type: PyDoughType):
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid key type for ArrayType. Expected a PyDoughType, received: {key_type!r}"
            )
        if not isinstance(key_type, PyDoughType):
            raise PyDoughTypeException(
                f"Invalid value type for ArrayType. Expected a PyDoughType, received: {val_type!r}"
            )
        self._key_type: PyDoughType = key_type
        self._val_type: PyDoughType = val_type

    @property
    def key_type(self) -> PyDoughType:
        """
        The PyDough type of the keys in a map.
        """
        return self._key_type

    @property
    def val_type(self) -> PyDoughType:
        """
        The PyDough type of the values in a map.
        """
        return self._val_type

    def __repr__(self):
        return f"MapType({self.key_type!r},{self.val_type!r})"

    @property
    def json_string(self) -> str:
        return f"map[{self.key_type.json_string},{self.val_type.json_string}]"

    # The string pattern that map types must adhere to. The delineation
    # between the key and value is handled later.
    type_string_pattern: re.Pattern = re.compile(r"map\[(.+,.+)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        from pydough.types import parse_type_from_string

        # Verify that the string matches the map type regex pattern, extracting
        # the body string.
        match: re.Match | None = MapType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        map_body: str = str(match.groups(0)[0])

        # Using each location of a comma as a candidate splitting location for
        # key,value in the body. Identify which one is valid by attempting
        # to parse both sides of the comma as a type. Whichever split succeeds
        # is the correct split.
        key_type: PyDoughType | None = None
        val_type: PyDoughType | None = None
        for i in range(len(map_body)):
            if map_body[i] == ",":
                try:
                    key_type = parse_type_from_string(map_body[:i])
                    val_type = parse_type_from_string(map_body[i + 1 :])
                    break
                except PyDoughTypeException:
                    key_type = None
                    val_type = None

        # If none of the candidate splits succeeded, the parsing fails.
        if key_type is None or val_type is None:
            return None
        return MapType(key_type, val_type)
