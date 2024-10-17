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

    @property
    def json_string(self) -> str:
        return f"float{self.bit_width}"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType:
        match type_string:
            case "float32":
                return Float32Type()
            case "float64":
                return Float64Type()
            case _:
                return None


class Float32Type(FloatType):
    def __init__(self):
        self.bit_width = 32


class Float64Type(FloatType):
    def __init__(self):
        self.bit_width = 64
