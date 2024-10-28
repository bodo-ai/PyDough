"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughExpressionOperatorAST"]

from typing import List

from pydough.pydough_ast.pydough_operators import PyDoughOperatorAST
from pydough.types import PyDoughType
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
)


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

    def infer_return_type(self, args: List[PyDoughAST]) -> PyDoughType:
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
