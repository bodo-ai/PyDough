"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class IntegerType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __repr__(self):
        return f"Integer{self.bit_width}Type()"

    def as_json_string(self):
        return f"int{self.bit_width}"


class Int8Type(IntegerType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        self.bit_width = 8


class Int16Type(IntegerType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        self.bit_width = 16


class Int32Type(IntegerType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        self.bit_width = 32


class Int64Type(IntegerType):
    """
    TODO: add class docstring
    """

    def __init__(self):
        self.bit_width = 64
