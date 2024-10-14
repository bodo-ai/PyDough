"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class FloatType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __repr__(self):
        return f"Float{self.bit_width}Type()"

    def as_json_string(self):
        return f"float{self.bit_width}"


class Float32Type(FloatType):
    def __init__(self):
        self.bit_width = 32


class Float64Type(FloatType):
    def __init__(self):
        self.bit_width = 64
