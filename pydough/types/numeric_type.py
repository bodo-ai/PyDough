"""
Definition of the PyDough types for numeric types.
"""

__all__ = ["NumericType"]


from .pydough_type import PyDoughType


class NumericType(PyDoughType):
    """
    The PyDough type superclass for integers.
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "NumericType()"

    @property
    def json_string(self) -> str:
        return "numeric"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return NumericType() if type_string == "numeric" else None
