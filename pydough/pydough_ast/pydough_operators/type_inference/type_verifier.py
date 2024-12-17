"""
Utilities used for PyDough type checking.
"""

__all__ = ["TypeVerifier", "RequireNumArgs", "AllowAny", "RequireMinArgs"]

from abc import ABC, abstractmethod
from collections.abc import MutableSequence

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException


class TypeVerifier(ABC):
    """
    Base class for verifiers that take in a list of PyDough AST objects and
    either silently accepts them or rejects them by raising an exception.

    Each implementation class is expected to implement the `accepts` method.
    """

    @abstractmethod
    def accepts(
        self, args: MutableSequence[PyDoughAST], error_on_fail: bool = True
    ) -> bool:
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
            `PyDoughASTException`: if the arguments are rejected and
            `error_on_fail` is True.
        """


class AllowAny(TypeVerifier):
    """
    Type verifier implementation class that always accepts, no matter the
    arguments.
    """

    def accepts(
        self, args: MutableSequence[PyDoughAST], error_on_fail: bool = True
    ) -> bool:
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

    def accepts(
        self, args: MutableSequence[PyDoughAST], error_on_fail: bool = True
    ) -> bool:
        if len(args) != self.num_args:
            if error_on_fail:
                suffix = "argument" if self._num_args == 1 else "arguments"
                raise PyDoughASTException(
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

    def accepts(
        self, args: MutableSequence[PyDoughAST], error_on_fail: bool = True
    ) -> bool:
        if len(args) < self.min_args:
            if error_on_fail:
                suffix = "argument" if self._min_args == 1 else "arguments"
                raise PyDoughASTException(
                    f"Expected at least {self.min_args} {suffix}, received {len(args)}"
                )
            return False
        return True
