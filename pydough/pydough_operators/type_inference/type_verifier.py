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

from pydough.metadata import PyDoughMetadataException


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
        from pydough.qdag.errors import PyDoughQDAGException

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
        from pydough.qdag.errors import PyDoughQDAGException

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
        from pydough.qdag.errors import PyDoughQDAGException

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
    Builds a type verifier from a JSON object.

    Args:
        `json_data`: the JSON object containing the verifier configuration, or
        None if not provided.

    Returns:
        An instance of a `TypeVerifier` subclass based on the JSON data.
    """
    # If no JSON data is provided, return a verifier that accepts any arguments
    if json_data is None:
        return AllowAny()

    if "type" not in json_data:
        raise PyDoughMetadataException("Missing 'type' field in verifier JSON data")

    match json_data["type"]:
        case "fixed arguments":
            if "value" not in json_data:
                raise PyDoughMetadataException(
                    "Missing 'value' field in fixed arguments verifier JSON data"
                )
            return RequireNumArgs(len(json_data["value"]))
        case "argument range":
            if "value" not in json_data:
                raise PyDoughMetadataException(
                    "Missing 'value' field in fixed arguments verifier JSON data"
                )
            if "min" not in json_data or "max" not in json_data:
                raise PyDoughMetadataException(
                    "Missing 'min' or 'max' field in argument range verifier JSON data"
                )
            return RequireArgRange(json_data["min"], len(json_data["value"]))
        case other:
            raise PyDoughMetadataException(f"Unknown verifier type string: {other!r}")
