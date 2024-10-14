"""
TODO: add file-level docstring
"""

from typing import List, Tuple
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
        field_strings = [f"{name}:{typ.as_json_string()}" for name, typ in self.fields]
        return f"struct[{','.join(field_strings)}]"

    type_string_pattern: re.Pattern = re.compile("struct\[(.*:.*)+\]")

    def parse_from_string(type_string: str) -> PyDoughType:
        match = StructType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None
        fields = StructType.parse_struct_body(match.groups(0)[0])
        if fields is None or len(fields) == 0:
            return None
        return StructType(fields)

    def parse_struct_body(struct_body_string):
        """
        TODO: add function docstring
        """
        from pydough.types import parse_type_from_string

        fields = []
        for i in range(len(struct_body_string)):
            if struct_body_string[i] == ":":
                field_name = struct_body_string[:i]
                if not field_name.isidentifier():
                    continue
                field_type = None
                suffix_fields = None
                try:
                    field_type = parse_type_from_string(struct_body_string[i + 1 :])
                    fields.append((field_name, field_type))
                    return fields
                except PyDoughTypeException:
                    pass
                if field_type is None:
                    for j in range(i + 1, len(struct_body_string)):
                        if struct_body_string[j] == ",":
                            try:
                                field_type = parse_type_from_string(
                                    struct_body_string[i + 1 : j]
                                )
                                suffix_fields = StructType.parse_struct_body(
                                    struct_body_string[j + 1 :]
                                )
                                if suffix_fields is not None and len(suffix_fields) > 0:
                                    break
                                else:
                                    field_type = None
                                    suffix_fields = None
                            except PyDoughTypeException:
                                field_type = None
                                suffix_fields = None
                if field_type is None or suffix_fields is None:
                    return None
                fields.append((field_name, field_type))
                fields.extend(suffix_fields)
                return fields
