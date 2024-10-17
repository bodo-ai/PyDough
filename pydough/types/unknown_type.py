"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType
from typing import Optional


class UnknownType(PyDoughType):
    """
    The PyDough type representing an unknown type.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "UnknownType()"

    @property
    def json_string(self) -> str:
        return "unknown"

    @staticmethod
    def parse_from_string(type_string: str) -> Optional[PyDoughType]:
        return UnknownType() if type_string == "unknown" else None
