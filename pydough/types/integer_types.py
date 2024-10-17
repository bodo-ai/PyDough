"""
TODO: add file-level docstring
"""

from .pydough_type import PyDoughType


class IntegerType(PyDoughType):
    """
    TODO: add class docstring
    """

    def __repr__(self):
        return f"Int{self.bit_width}Type()"

    @property
    def json_string(self) -> str:
        return f"int{self.bit_width}"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType:
        match type_string:
            case "int8":
                return Int8Type()
            case "int16":
                return Int16Type()
            case "int32":
                return Int32Type()
            case "int64":
                return Int64Type()
            case _:
                return None


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
