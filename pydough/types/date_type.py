"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class DateType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "DateType()"

    def as_json_string(self) -> str:
        return "date"

    def parse_from_string(type_string: str) -> PyDoughType:
        return DateType() if type_string == "date" else None
