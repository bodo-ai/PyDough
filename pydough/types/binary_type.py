"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class BinaryType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "BinaryType()"

    @property
    def json_string(self) -> str:
        return "binary"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType:
        return BinaryType() if type_string == "binary" else None
