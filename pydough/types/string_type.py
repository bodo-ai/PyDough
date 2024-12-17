"""
Definition of the PyDough type for a string type.
"""

__all__ = ["StringType"]


from .pydough_type import PyDoughType


class StringType(PyDoughType):
    """
    The PyDough type representing strings.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "StringType()"

    @property
    def json_string(self) -> str:
        return "string"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return StringType() if type_string == "string" else None
