"""
Definition of the PyDough types for floating point types.
"""

__all__ = ["FloatType"]


from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class FloatType(PyDoughType):
    """
    The PyDough type superclass for floating point numbers.
    """

    def __init__(self, bit_width: int):
        if not isinstance(bit_width, int) or bit_width not in (32, 64):
            raise PyDoughTypeException(
                f"Invalid bit width for FloatType: {bit_width!r}"
            )
        self._bit_width: int = bit_width

    @property
    def bit_width(self) -> int:
        return self._bit_width

    def __repr__(self):
        return f"Float{self.bit_width}Type()"

    @property
    def json_string(self) -> str:
        return f"float{self.bit_width}"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
        match type_string:
            case "float32":
                return Float32Type()
            case "float64":
                return Float64Type()
            case _:
                return None


class Float32Type(FloatType):
    """
    The PyDough type for 32-bit floating point numbers.
    """

    def __init__(self):
        super().__init__(32)


class Float64Type(FloatType):
    """
    The PyDough type for 64-bit floating point numbers.
    """

    def __init__(self):
        super().__init__(64)
