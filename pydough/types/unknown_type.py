"""
Definition of the PyDough type for an unknown type.
"""

__all__ = ["UnknownType"]


from .pydough_type import PyDoughType


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
    def parse_from_string(type_string: str) -> PyDoughType | None:
        return UnknownType() if type_string == "unknown" else None
