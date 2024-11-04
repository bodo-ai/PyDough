"""
TODO: add file-level docstring
"""

__all__ = ["DateType"]

from .pydough_type import PyDoughType
from typing import Optional


class DateType(PyDoughType):
    """
    The PyDough type representing dates with a year/month/day.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "DateType()"

    @property
    def json_string(self) -> str:
        return "date"

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        return DateType() if type_string == "date" else None
