"""
TODO: add file-level docstring
"""

__all__ = ["ExpressionFunctionCall"]

from typing import List

from pydough.pydough_ast.pydough_operators import PyDoughExpressionOperatorAST
from . import PyDoughExpressionAST
from pydough.types import PyDoughType


class ExpressionFunctionCall(PyDoughExpressionAST):
    """
    The AST node implementation class representing a call to a function
    that returns an expression.
    """

    def __init__(
        self, operator: PyDoughExpressionOperatorAST, args: List[PyDoughExpressionAST]
    ):
        operator.verify_allows_args(args)
        self._operator: PyDoughExpressionOperatorAST = operator
        self._args: List[PyDoughExpressionAST] = args
        self._data_type: PyDoughType = operator.infer_return_type(args)

    @property
    def operator(self) -> PyDoughExpressionOperatorAST:
        """
        The expression-returning PyDough operator corresponding to the
        function call.
        """
        return self._operator

    @property
    def args(self) -> List[PyDoughExpressionAST]:
        """
        The list of arguments to the function call.
        """
        return self._args

    @property
    def pydough_type(self) -> PyDoughType:
        return self._data_type

    @property
    def is_aggregation(self) -> bool:
        return self.operator.is_aggregation

    def requires_enclosing_parens(self, parent: "PyDoughExpressionAST") -> bool:
        return self.operator.requires_enclosing_parens(parent)

    def to_string(self) -> str:
        return self.operator.to_string(self.args)

    def equals(self, other: "ExpressionFunctionCall") -> bool:
        return (
            super().equals(other)
            and (self.operator == other.operator)
            and (self.args == other.args)
        )
