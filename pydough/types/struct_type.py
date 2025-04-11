"""
Definition of the PyDough type for struct types.
"""

__all__ = ["StructType"]

import re

from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class StructType(PyDoughType):
    """
    The PyDough type representing a collection of named fields.
    """

    def __init__(self, fields: list[tuple[str, PyDoughType]]):
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
                f"Invalid fields type for StructType: {fields!r}"
            )
        self._fields: list[tuple[str, PyDoughType]] = fields

    @property
    def fields(self) -> list[tuple[str, PyDoughType]]:
        """
        The list of fields of the struct in the form (field_name, field_type).
        """
        return self._fields

    def __repr__(self):
        return f"StructType({self.fields!r})"

    @property
    def json_string(self) -> str:
        field_strings = [f"{name}:{typ.json_string}" for name, typ in self.fields]
        return f"struct[{','.join(field_strings)}]"

    # The string pattern that struct types must adhere to. The delineation
    # between the various field names/types is handled later.
    type_string_pattern: re.Pattern = re.compile(r"struct\[(.+:.+)\]")

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        # Verify that the string matches the struct type regex pattern, extracting
        # the body string.
        match = StructType.type_string_pattern.fullmatch(type_string)
        if match is None:
            return None

        # Extract the list of fields from the body string. If the attempt fails,
        # then the parsing fails.
        fields: list[tuple[str, PyDoughType]] | None = StructType.parse_struct_body(
            str(match.groups(0)[0])
        )
        if fields is None or len(fields) == 0:
            return None
        return StructType(fields)

    @staticmethod
    def parse_struct_body(
        struct_body_string: str,
    ) -> list[tuple[str, PyDoughType]] | None:
        """
        Attempts to parse and extract a list of (field_name, field_type) tuples
        from a string which can contain 1 or more fields in the form
        field_name:field_type, separated by commas. Each field_name must be
        a valid Python identifier and each field_type must be a PyDough type
        string.
        """
        from pydough.types import parse_type_from_string

        # Keep track of all fields extracted so far.
        fields: list[tuple[str, PyDoughType]] = []

        # Iterate across the string to identify all colons that are candidate
        # splitting locations, where the left hand side is the name of a field
        # and the right hand side must be either entirely a PyDough type string,
        # or a PyDough type string followed by a comma followed by more fields.
        for i in range(len(struct_body_string)):
            if struct_body_string[i] == ":":
                # Reject the candidate name-type split location if the
                # left hand side is not a Python identifier.
                field_name: str = struct_body_string[:i]
                if not field_name.isidentifier():
                    continue

                # Special case: if the entire right hand side string is a
                # PyDough type, then the parsing has succeed in finding a
                # single (field_name, field_type) pair from the entire string.
                field_type: PyDoughType | None = None
                suffix_fields: list[tuple[str, PyDoughType]] | None = None
                try:
                    field_type = parse_type_from_string(struct_body_string[i + 1 :])
                    fields.append((field_name, field_type))
                    return fields
                except PyDoughTypeException:
                    pass

                # Otherwise, iterate across all commas in the right hand side
                # that are candidate splitting locations between a PyDough
                # type and a suffix that is a valid list of fields.
                if field_type is None:
                    for j in range(i + 1, len(struct_body_string)):
                        if struct_body_string[j] == ",":
                            # The candidate split is valid if the left hand
                            # side is a type string and the right hand side
                            # is another valid list of fields.
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

                # If all comma candidates failed, then the colon candidate
                # has also failed.
                if field_type is None or suffix_fields is None:
                    continue

                # Otherwise, add the newly identified (field_name, field_type)
                # pair to the fields followed by all the fields parsed after
                # the comma splitting location.
                fields.append((field_name, field_type))
                fields.extend(suffix_fields)
                return fields

        # If we reach this far, then the parsing attempt has failed
        return None
