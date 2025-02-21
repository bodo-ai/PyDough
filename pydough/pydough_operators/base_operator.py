"""
Base definition of PyDough operators.
"""

__all__ = ["PyDoughOperator"]

from abc import abstractmethod
from typing import Any

from .type_inference import TypeVerifier


class PyDoughOperator:
    """
    The abstract base class for all PyDough operators used to describe
    operations such as arithmetic or function calls, whether they return
    an expression or a collection.
    """

    def __init__(self, verifier: TypeVerifier):
        self._verifier: TypeVerifier = verifier

    @property
    def verifier(self) -> TypeVerifier:
        """
        The type verification function used by the operator
        """
        return self._verifier

    def __repr__(self):
        return self.standalone_string

    @property
    @abstractmethod
    def is_aggregation(self) -> bool:
        """
        Returns whether the operator corresponds to an aggregation that
        can collapse multiple records into a scalar value.
        """

    @property
    @abstractmethod
    def standalone_string(self) -> str:
        """
        The string representation used to identify the operator, devoid
        of any arguments.
        """

    def verify_allows_args(self, args: list[Any]) -> None:
        """
        Verifies that an operator is allowed to be called with a certain
        set of arguments.

        Raises:
            `PyDoughQDAGException` if the operator does not accept the
            provided arguments.
        """
        from pydough.qdag.errors import PyDoughQDAGException

        try:
            self.verifier.accepts(args)
        except PyDoughQDAGException as e:
            # If the verifier failed, raise the error with the same traceback
            # but prepend it with information about the operator and args
            # that caused the failure.
            arg_strings: list[str] = [str(arg) for arg in args]
            msg = f"Invalid operator invocation {self.to_string(arg_strings)!r}: {e}"
            raise PyDoughQDAGException(msg).with_traceback(e.__traceback__)

    @abstractmethod
    def to_string(self, arg_strings: list[str]) -> str:
        """
        Returns the string representation of the operator when called on
        its arguments, which have already been converted to a string.

        Args:
            `arg_strings`: the string representations of the arguments to the
            operator.

        Returns:
            The string representation of the operator called on its arguments.
        """

    @abstractmethod
    def equals(self, other: object) -> bool:
        """
        Returns whether this operator is equal to another operator.
        """

    def __eq__(self, other: object) -> bool:
        return self.equals(other)

    def __hash__(self) -> int:
        return hash(repr(self))
