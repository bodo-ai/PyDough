"""
Definition of the PyDough types for integer types.
"""

__all__ = ["Int16Type", "Int32Type", "Int64Type", "Int8Type", "IntegerType"]


from .errors import PyDoughTypeException
from .pydough_type import PyDoughType


class IntegerType(PyDoughType):
    """
    The PyDough type superclass for integers.
    """

    def __init__(self, bit_width: int):
        if not isinstance(bit_width, int) or bit_width not in (8, 16, 32, 64):
            raise PyDoughTypeException(
                f"Invalid bit width for IntegerType: {bit_width!r}"
            )
        self._bit_width: int = bit_width

    @property
    def bit_width(self) -> int:
        return self._bit_width

    def __repr__(self):
        return f"Int{self.bit_width}Type()"

    @property
    def json_string(self) -> str:
        return f"int{self.bit_width}"

    @staticmethod
    def parse_from_string(type_string: str) -> PyDoughType | None:
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
    The PyDough type for 8-bit integers.
    """

    def __init__(self):
        super().__init__(8)


class Int16Type(IntegerType):
    """
    The PyDough type for 16-bit integers.
    """

    def __init__(self):
        super().__init__(16)


class Int32Type(IntegerType):
    """
    The PyDough type for 32-bit integers.
    """

    def __init__(self):
        super().__init__(32)


class Int64Type(IntegerType):
    """
    The PyDough type for 64-bit integers.
    """

    def __init__(self):
        super().__init__(64)
