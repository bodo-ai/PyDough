"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughExpressionOperatorAST"]

from abc import abstractmethod
from collections.abc import MutableSequence

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.expressions import PyDoughExpressionAST
from pydough.pydough_ast.pydough_operators.operator_ast import PyDoughOperatorAST
from pydough.pydough_ast.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)
from pydough.types import PyDoughType


class PyDoughExpressionOperatorAST(PyDoughOperatorAST):
    """
    The base class for PyDough operators that return an expression. In addition
    to having a verifier, all such classes have a deducer to infer the type
    of the returned expression.
    """

    def __init__(self, verifier: TypeVerifier, deducer: ExpressionTypeDeducer):
        super().__init__(verifier)
        self._deducer: ExpressionTypeDeducer = deducer

    @property
    def deducer(self) -> ExpressionTypeDeducer:
        """
        The return type inferrence function used by the operator
        """
        return self._deducer

    @abstractmethod
    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        """
        Identifies whether an invocation of an operator converted to a string
        must be wrapped  in parenthesis before being inserted into it's parent's
        string representation. This depends on what exactly the parent is.

        Args:
            `parent`: the parent expression AST that contains this expression
            AST as a child.

        Returns:
            True if the string representation of `parent` should enclose
            parenthesis around the string representation of an invocation of
            `self`.
        """

    def infer_return_type(self, args: MutableSequence[PyDoughAST]) -> PyDoughType:
        """
        Returns the expected PyDough type of the operator when called on
        the provided arguments.

        Args:
            `args`: the inputs to the operator.

        Returns:
            The type of the returned expression as a PyDoughType.

        Raises:
            `PyDoughASTException` if `args` is invalid for this operator.
        """
        return self.deducer.infer_return_type(args)
