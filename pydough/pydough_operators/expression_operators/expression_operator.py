"""
Base definition of PyDough operators that return an expression.
"""

__all__ = ["PyDoughExpressionOperator"]

from abc import abstractmethod
from typing import Any

from pydough.pydough_operators.base_operator import PyDoughOperator
from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)
from pydough.types import PyDoughType


class PyDoughExpressionOperator(PyDoughOperator):
    """
    The base class for PyDough operators that return an expression. In addition
    to having a verifier, all such classes have a deducer to infer the type
    of the returned expression.
    """

    def __init__(
        self,
        verifier: TypeVerifier,
        deducer: ExpressionTypeDeducer,
        public: bool = True,
    ):
        super().__init__(verifier)
        self._deducer: ExpressionTypeDeducer = deducer
        self._public: bool = public

    @property
    def deducer(self) -> ExpressionTypeDeducer:
        """
        The return type inferrence function used by the operator
        """
        return self._deducer

    @property
    def public(self) -> bool:
        """
        Whether the operator is public.
        """
        return self._public

    @property
    @abstractmethod
    def function_name(self) -> str:
        """
        The name of the function that this operator represents. This will
        be used for other components that are function dependent.

        Returns:
            str: The name used for the function.
        """

    @abstractmethod
    def requires_enclosing_parens(self, parent) -> bool:
        """
        Identifies whether an invocation of an operator converted to a string
        must be wrapped  in parenthesis before being inserted into it's parent's
        string representation. This depends on what exactly the parent is.

        Args:
            `parent`: the parent expression QDAG that contains this expression
            QDAG as a child.

        Returns:
            True if the string representation of `parent` should enclose
            parenthesis around the string representation of an invocation of
            `self`.
        """

    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        """
        Returns the expected PyDough type of the operator when called on
        the provided arguments.

        Args:
            `args`: the inputs to the operator.

        Returns:
            The type of the returned expression as a PyDoughType.

        Raises:
            `PyDoughQDAGException` if `args` is invalid for this operator.
        """
        return self.deducer.infer_return_type(args)
