"""
Utilities used for PyDough return type inference.
"""

__all__ = [
    "ConstantType",
    "ExpressionTypeDeducer",
    "SelectArgumentType",
    "build_deducer_from_json",
]

from abc import ABC, abstractmethod
from typing import Any

from pydough.errors.error_utils import (
    NoExtraKeys,
    PyDoughMetadataException,
    extract_integer,
    extract_string,
)
from pydough.types import PyDoughType, UnknownType, parse_type_from_string


class ExpressionTypeDeducer(ABC):
    """
    Abstract base class for type-inferring classes that take in a list of
    PyDough expression QDAGs and returns a PyDough type. Each implementation
    class must implement the `infer_return_type` API.
    """

    @abstractmethod
    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        """
        Returns the inferred expression type based on the input arguments.

        Raises:
            `PyDoughQDAGException` if the arguments are invalid.
        """


class SelectArgumentType(ExpressionTypeDeducer):
    """
    Type deduction implementation class that always selects the type of a
    specifc argument from the inputs based on an ordinal position.
    """

    def __init__(self, index: int):
        self._index: int = index

    @property
    def index(self) -> int:
        """
        The ordinal position of the argument that is always selected.
        """
        return self._index

    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        from pydough.qdag import PyDoughExpressionQDAG, PyDoughQDAGException

        msg: str = f"Cannot select type of argument {self.index!r} out of {args!r}"
        if self.index not in range(len(args)):
            raise PyDoughQDAGException(msg)
        arg = args[self.index]
        if isinstance(arg, PyDoughExpressionQDAG):
            return arg.pydough_type
        else:
            raise PyDoughQDAGException(msg)


class ConstantType(ExpressionTypeDeducer):
    """
    Type deduction implementation class that always returns a specific
    PyDough type.
    """

    def __init__(self, data_type: PyDoughType):
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        """
        The type always inferred by this deducer.
        """
        return self._data_type

    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        return self.data_type


def build_deducer_from_json(json_data: dict[str, Any] | None) -> ExpressionTypeDeducer:
    """
    Builds a type deducer from a JSON object.

    Args:
        `json_data`: the JSON object containing the deducer configuration, or
        None if not provided.

    Returns:
        An instance of a `ExpressionTypeDeducer` subclass based on the JSON data.
    """
    # If no JSON data is provided, return a deducer that always returns the
    # unknown type.
    if json_data is None:
        return ConstantType(UnknownType())

    data_type: PyDoughType | None

    # Extract and switch on the deducer type string field.
    deducer_type: str = extract_string(json_data, "type", "deducer JSON metadata")
    match deducer_type:
        # Constant deducer type.
        case "constant":
            NoExtraKeys({"type", "value"}).verify(
                json_data, "constant deducer JSON metadata"
            )
            type_string: str = extract_string(
                json_data, "value", "constant deducer JSON data"
            )
            data_type = parse_type_from_string(type_string)
            if data_type is None:
                raise PyDoughMetadataException(
                    f"Invalid type value in constant deducer JSON data: {json_data['value']!r}"
                )
            return ConstantType(data_type)

        # Select argument deducer type.
        case "select argument":
            NoExtraKeys({"type", "value"}).verify(
                json_data, "select argument deducer JSON metadata"
            )
            arg_idx: int = extract_integer(
                json_data, "value", "select argument deducer JSON data"
            )
            if arg_idx < 0:
                raise PyDoughMetadataException(
                    f"Invalid argument index in select argument deducer JSON data: {arg_idx!r}"
                )
            return SelectArgumentType(arg_idx)

        case _:
            raise PyDoughMetadataException(f"Unknown deducer type: {deducer_type!r}")
