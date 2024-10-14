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

    def as_json_string(self):
        return "binary"
