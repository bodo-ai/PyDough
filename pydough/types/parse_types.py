"""
TODO: add file-level docstring
"""

__all__ = ["parse_type_from_string"]

from typing import Type, MutableSequence

from .pydough_type import PyDoughType
from .integer_types import Int8Type, Int16Type, Int32Type, Int64Type
from .float_types import Float32Type, Float64Type
from .boolean_type import BooleanType
from .decimal_type import DecimalType
from .string_type import StringType
from .binary_type import BinaryType
from .date_type import DateType
from .time_type import TimeType
from .timestamp_type import TimestampType
from .array_type import ArrayType
from .map_type import MapType
from .struct_type import StructType
from .unknown_type import UnknownType
from .errors import PyDoughTypeException


def parse_type_from_string(type_string: str) -> PyDoughType:
    """
    TODO: add function docstring
    """
    type_classes: MutableSequence[Type[PyDoughType]] = [
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
