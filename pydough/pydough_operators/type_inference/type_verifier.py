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
]

from abc import ABC, abstractmethod
from typing import Any


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
