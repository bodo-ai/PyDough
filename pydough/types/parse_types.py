"""
Logic for converting JSON strings to PyDough types.
"""

__all__ = ["parse_type_from_string"]


from .array_type import ArrayType
from .binary_type import BinaryType
from .boolean_type import BooleanType
from .date_type import DateType
from .decimal_type import DecimalType
from .errors import PyDoughTypeException
from .float_types import Float32Type, Float64Type
from .integer_types import Int8Type, Int16Type, Int32Type, Int64Type
from .map_type import MapType
from .pydough_type import PyDoughType
from .string_type import StringType
from .struct_type import StructType
from .time_type import TimeType
from .timestamp_type import TimestampType
from .unknown_type import UnknownType


def parse_type_from_string(type_string: str) -> PyDoughType:
    """
    Converts a string from a JSON file representing a PyDough type and
    converts it to that PyDough type.

    Args:
        `type_string`: the string to be converted.

    Returns:
        The PyDough type object.

    Raises:
        `PyDoughTypeException` if the string does not correspond to any
        PyDough type.
    """
    type_classes: list[type[PyDoughType]] = [
        BinaryType,
        BooleanType,
        DecimalType,
        DateType,
        Float32Type,
        Float64Type,
        Int8Type,
        Int16Type,
        Int32Type,
        Int64Type,
        StringType,
        TimeType,
        TimestampType,
        UnknownType,
        ArrayType,
        MapType,
        StructType,
    ]
    for type_class in type_classes:
        parsed_type = type_class.parse_from_string(type_string)
        if parsed_type is not None:
            return parsed_type
    raise PyDoughTypeException(f"Unrecognized type string {type_string!r}")
