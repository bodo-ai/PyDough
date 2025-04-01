"""
Definition of PyDough QDAG nodes for window function calls that return
expressions.
"""

__all__ = ["WindowCall"]


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
        args: list[PyDoughExpressionQDAG],
        collation_args: list[CollationExpression],
        levels: int | None,
        kwargs: dict[str, object],
    ):
        window_operator.verify_allows_args(args)
        self._window_operator: ExpressionWindowOperator = window_operator
        self._args: list[PyDoughExpressionQDAG] = args
        self._collation_args: list[CollationExpression] = collation_args
        self._levels: int | None = levels
        self._kwargs: dict[str, object] = kwargs

    @property
    def window_operator(self) -> ExpressionWindowOperator:
        """
        The window operator that is being applied.
        """
        return self._window_operator

    @property
    def args(self) -> list[PyDoughExpressionQDAG]:
        """
        The list of arguments used to as inputs to the window function.
        """
        return self._args

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
    def kwargs(self) -> dict[str, object]:
        """
        Any additional keyword arguments to window functions. For example, for
        `RANKING`, whether to allow ties in the ranking (True) or not (False).
        """
        return self._kwargs

    @property
    def pydough_type(self) -> PyDoughType:
        return self.window_operator.infer_return_type(self.args)

    @property
    def is_aggregation(self) -> bool:
        return False

    def is_singular(self, context: PyDoughQDAG) -> bool:
        # Window function calls are singular if all of their arguments and
        # collation arguments are singular
        for arg in self.args:
            if not arg.is_singular(context):
                return False
        for order_arg in self.collation_args:
            if not order_arg.expr.is_singular(context):
                return False
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        arg_strings: list[str] = [f"{arg.to_string(tree_form)}, " for arg in self.args]
        collation_arg_strings: list[str] = [
            arg.to_string(tree_form) for arg in self.collation_args
        ]
        suffix: str = ""
        if self.levels is not None:
            suffix += f", levels={self.levels}"
        for kwarg in self.kwargs:
            suffix += f", {kwarg}={self.kwargs.get(kwarg)!r}"
        return f"{self.window_operator.function_name}({''.join(arg_strings)}by=({', '.join(collation_arg_strings)}){suffix})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, WindowCall)
            and (self.window_operator == other.window_operator)
            and (self.args == other.args)
            and (self.collation_args == other.collation_args)
            and (self.levels == other.levels)
            and (repr(self.kwargs) == repr(other.kwargs))
        )
