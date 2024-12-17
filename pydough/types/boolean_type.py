"""
Definition of the PyDough type for booleans.
"""

__all__ = ["BooleanType"]


from .pydough_type import PyDoughType


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
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return BooleanType() if type_string == "bool" else None
