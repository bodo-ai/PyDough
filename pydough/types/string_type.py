"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class StringType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "StringType()"

    def as_json_string(self) -> str:
        return "string"

    def parse_from_string(type_string: str) -> PyDoughType:
        return StringType() if type_string == "string" else None
