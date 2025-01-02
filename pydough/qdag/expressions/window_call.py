"""
Definition of PyDough QDAG nodes for window function calls that return
expressions.
"""

__all__ = ["WindowCall"]

from functools import cache

from pydough.pydough_operators.expression_operators import (
    ExpressionWindowOperator,
)
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.types import PyDoughType

from .collation_expression import CollationExpression
from .expression_qdag import PyDoughExpressionQDAG


class WindowCall(PyDoughExpressionQDAG):
    """
    The QDAG node implementation class representing window operations that
    return expressions.
    """

    def __init__(
        self,
        window_operator: ExpressionWindowOperator,
        collation_args: list[CollationExpression],
        levels: int | None,
        allow_ties: bool,
        dense: bool,
    ):
        self._window_operator: ExpressionWindowOperator = window_operator
        self._collation_args: list[CollationExpression] = collation_args
        self._levels: int | None = levels
        self._allow_ties: bool = allow_ties
        self._dense: bool = dense

    @property
    def window_operator(self) -> ExpressionWindowOperator:
        """
        The window operator that is being applied.
        """
        return self._window_operator

    @property
    def collation_args(self) -> list[CollationExpression]:
        """
        The list of collation arguments used to order to the window function.
        """
        return self._collation_args

    @property
    def levels(self) -> int | None:
        """
        The number of ancestor levels the window function is being computed
        relative to. None indicates that the computation is global.
        """
        return self._levels

    @property
    def allow_ties(self) -> bool:
        """
        Whether to allow ties in the ranking (True) or not (False).
        """
        return self._allow_ties

    @property
    def dense(self) -> bool:
        """
        Whether to compute dense ranks (True) or standard ranks (False).
        """
        return self._dense

    @property
    def pydough_type(self) -> PyDoughType:
        return self.window_operator.infer_return_type([])

    @property
    def is_aggregation(self) -> bool:
        return False

    @cache
    def is_singular(self, context: PyDoughQDAG) -> bool:
        # Window function calls are singular if all of their collation
        # arguments are singular
        for arg in self.collation_args:
            if not arg.expr.is_singular(context):
                return False
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        arg_strings: list[str] = [
            arg.to_string(tree_form) for arg in self.collation_args
        ]
        suffix: str = ""
        if self.levels is not None:
            suffix += f", levels={self.levels}"
        if self.allow_ties:
            suffix += ", allow_ties=True"
            if self.dense:
                suffix += ", dense=True"
        return f"{self.window_operator.function_name}(by=({', '.join(arg_strings)}){suffix})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, WindowCall)
            and (self.window_operator == other.window_operator)
            and (self.collation_args == other.collation_args)
            and (self.levels == other.levels)
            and (self.allow_ties == other.allow_ties)
            and (self.dense == other.dense)
        )
