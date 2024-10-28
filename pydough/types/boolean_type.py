"""
TODO: add file-level docstring
"""

__all__ = ["BooleanType"]

from .pydough_type import PyDoughType
from typing import Optional


class BooleanType(PyDoughType):
    """
    The PyDough type representing true/false data.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "BooleanType()"

    @property
    def json_string(self) -> str:
        return "bool"

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        return BooleanType() if type_string == "bool" else None
