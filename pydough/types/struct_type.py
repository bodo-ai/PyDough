"""
TODO: add file-level docstring
"""

from types import List, Tuple
from .pydough_type import PyDoughType
from .errors import PyDoughTypeException
import re


class StructType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self, fields: List[Tuple[str, PyDoughType]]):
        if not (
            isinstance(fields, list)
            and len(fields) > 0
            and all(
                isinstance(field, tuple)
                and len(field) == 2
                and isinstance(field[0], str)
                and isinstance(field[1], PyDoughType)
                for field in fields
            )
        ):
            raise PyDoughTypeException(
                f"Invalid fields type for StructType: {repr(fields)}"
            )
        self.fields = fields

    def __repr__(self):
        return f"StructType({repr(self.fields)})"

    def as_json_string(self) -> str:
        field_strings = [f"{name}:{typ}" for name, typ in self.fields]
        return f"struct[{','.join(field_strings)}]"

    type_string_pattern: re.Pattern = re.compile("struct[((.+):(.+))+]")

    def parse_from_string(type_string: str) -> PyDoughType:
        match = StructType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        fields = []
        try:
            pass  # TODO
            # key_type = parse_type_from_string(match.groups[0])
            # val_type = parse_type_from_string(match.groups[1])
        except PyDoughTypeException:
            return None
        return StructType(fields)
