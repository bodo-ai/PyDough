"""
Utilities used for PyDough type checking.
"""

__all__ = [
    "AllowAny",
    "RequireArgRange",
    "RequireCollection",
    "RequireMinArgs",
    "RequireNumArgs",
    "TypeVerifier",
    "build_verifier_from_json",
]

from abc import ABC, abstractmethod
from typing import Any

from pydough.errors import PyDoughMetadataException, PyDoughQDAGException
from pydough.errors.error_utils import (
    NoExtraKeys,
    extract_array,
    extract_integer,
    extract_string,
)
from pydough.types import parse_type_from_string


class TypeVerifier(ABC):
    """
    Base class for verifiers that take in a list of PyDough QDAG objects and
    either silently accepts them or rejects them by raising an exception.

    Each implementation class is expected to implement the `accepts` method.
    """

    @abstractmethod
    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        """
        Verifies whether the type verifier accepts/rejects a list
        of arguments.

        Args:
            `args`: the list of arguments that are being checked.
            `error_on_fail`: whether an exception be raised if the verifier
            rejects the arguments.

        Returns:
            Whether the verifier accepts or rejects the arguments.

        Raises:
            `PyDoughQDAGException`: if the arguments are rejected and
            `error_on_fail` is True.
        """


class AllowAny(TypeVerifier):
    """
    Type verifier implementation class that always accepts, no matter the
    arguments.
    """

    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        return True


class RequireNumArgs(TypeVerifier):
    """
    Type verifier implementation class that requires an exact
    number of arguments
    """

    def __init__(self, num_args: int):
        self._num_args: int = num_args

    @property
    def num_args(self) -> int:
        """
        The number of arguments that the verifier expects to be
        provided.
        """
        return self._num_args

    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        if len(args) != self.num_args:
            if error_on_fail:
                suffix = "argument" if self._num_args == 1 else "arguments"
                raise PyDoughQDAGException(
                    f"Expected {self.num_args} {suffix}, received {len(args)}"
                )
            return False
        return True


class RequireMinArgs(TypeVerifier):
    """
    Type verifier implementation class that requires a minimum number of arguments
    """

    def __init__(self, min_args: int):
        self._min_args: int = min_args

    @property
    def min_args(self) -> int:
        """
        The minimum number of arguments that the verifier expects to be
        provided.
        """
        return self._min_args

    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        from pydough.qdag import PyDoughQDAGException

        if len(args) < self.min_args:
            if error_on_fail:
                suffix = "argument" if self._min_args == 1 else "arguments"
                raise PyDoughQDAGException(
                    f"Expected at least {self.min_args} {suffix}, received {len(args)}"
                )
            return False
        return True


class RequireArgRange(TypeVerifier):
    """
    Type verifier implementation class that requires the
    number of arguments to be within a range, both ends inclusive.
    """

    def __init__(self, low_range: int, high_range: int):
        self._low_range: int = low_range
        self._high_range: int = high_range

    @property
    def low_range(self) -> int:
        """
        The lower end of the range.
        """
        return self._low_range

    @property
    def high_range(self) -> int:
        """
        The higher end of the range.
        """
        return self._high_range

    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        if not (self.low_range <= len(args) <= self.high_range):
            if error_on_fail:
                raise PyDoughQDAGException(
                    f"Expected between {self.low_range} and {self.high_range} arguments inclusive, "
                    f"received {len(args)}."
                )
            return False
        return True


class RequireCollection(TypeVerifier):
    """
    Type verifier implementation class that requires a single argument to be a
    collection.
    """

    def accepts(self, args: list[Any], error_on_fail: bool = True) -> bool:
        from pydough.qdag.collections import PyDoughCollectionQDAG

        if len(args) != 1:
            if error_on_fail:
                raise PyDoughQDAGException(
                    f"Expected 1 collection argument, received {len(args)}."
                )
            else:
                return False

        if not isinstance(args[0], PyDoughCollectionQDAG):
            if error_on_fail:
                raise PyDoughQDAGException(
                    "Expected a collection as an argument, received an expression"
                )
            else:
                return False
        return True


def build_verifier_from_json(json_data: dict[str, Any] | None) -> TypeVerifier:
    """
    Builds a type verifier from a JSON object. Note: verifiers currently only
    deal with the number of types, rather than the actual types of the
    arguments, since the PyDough type system is not yet fully implemented.

    Args:
        `json_data`: the JSON object containing the verifier configuration, or
        None if not provided.

    Returns:
        An instance of a `TypeVerifier` subclass based on the JSON data.
    """
    # If no JSON data is provided, return a verifier that accepts any arguments
    if json_data is None:
        return AllowAny()

    type_args: list[str]

    # Extract and switch on the verifier type string field.
    deducer_type: str = extract_string(json_data, "type", "verifier JSON metadata")
    match deducer_type:
        case "fixed arguments":
            NoExtraKeys({"type", "value"}).verify(
                json_data, "fixed arguments verifier JSON metadata"
            )
            type_args = extract_array(
                json_data, "value", "fixed arguments verifier JSON data"
            )
            for arg in type_args:
                if not isinstance(arg, str) or parse_type_from_string(arg) is None:
                    raise PyDoughMetadataException(
                        f"Invalid type value in fixed arguments verifier JSON data: {arg!r}"
                    )
            return RequireNumArgs(len(type_args))

        case "argument range":
            NoExtraKeys({"type", "value", "min"}).verify(
                json_data, "argument range verifier JSON metadata"
            )
            type_args = extract_array(
                json_data, "value", "argument range verifier JSON data"
            )
            type_args = extract_array(
                json_data, "value", "fixed arguments verifier JSON data"
            )
            for arg in type_args:
                if not isinstance(arg, str) or parse_type_from_string(arg) is None:
                    raise PyDoughMetadataException(
                        f"Invalid type value in argument range verifier JSON data: {arg!r}"
                    )
            min_args: int = extract_integer(
                json_data, "min", "argument range verifier JSON data"
            )
            if min_args < 0:
                raise PyDoughMetadataException(
                    f"Invalid minimum argument count in argument range verifier JSON data: {min_args!r}"
                )
            if len(type_args) < min_args:
                raise PyDoughMetadataException(
                    "Invalid argument range verifier JSON data: "
                    f"minimum {min_args} is greater than the number of types provided: {len(type_args)}"
                )
            return RequireArgRange(min_args, len(type_args))

        case _:
            raise PyDoughMetadataException(
                f"Unknown verifier type string: {deducer_type!r}"
            )
