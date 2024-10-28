"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod

from typing import List

from pydough.pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException


class TypeVerifier(ABC):
    """
    Base class for verifiers that take in a list of PyDough AST objects and
    either silently accepts them or rejects them by raising an exception.

    Each implementation class is expected to implement the `accepts` method.
    """

    @abstractmethod
    def accepts(self, args: List[PyDoughAST]) -> None:
        """
        Verifies whether the type verifier accepts/rejects a list
        of arguments.

        Raises:
            `PyDoughASTException` if the arguments are rejected.
        """


class AllowAny(TypeVerifier):
    """
    Type verifier implementation class that always accepts, no matter the
    arguments.
    """

    def accepts(self, args: List[PyDoughAST]) -> None:
        pass


class NumArgs(TypeVerifier):
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

    def accepts(self, args: List[PyDoughAST]) -> None:
        if len(args) != self.num_args:
            args_string = "argument" if self.num_args == 1 else "arguments"
            raise PyDoughASTException(
                f"Expected {self.num_args} {args_string}, received {len(args)}"
            )
