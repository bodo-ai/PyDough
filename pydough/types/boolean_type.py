"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class BooleanType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        pass

    def __repr__(self):
        return "BooleanType()"

    def as_json_string(self):
        return "bool"
