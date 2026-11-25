"""
Find the type and representation of every literal in a relational expression.
"""

from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor
from .window_call_expression import WindowCallExpression

__all__ = ["LiteralTypeFinder"]


class LiteralTypeFinder(RelationalExpressionVisitor):
    """
    Collects the Python type and `repr` of every literal value in a relational
    expression, in visiting order. `RelationalExpression.equals` compares
    literal values with `==`, so `1`, `1.0` and `-0.0`/`0.0` compare equal even
    though expressions containing them can produce different results (e.g.
    `x + 1` versus `x + 1.0` for large integers). Comparing the collected
    types and representations tells such expressions apart.
    """

    def __init__(self) -> None:
        self._literal_types: list[tuple[type, str]] = []

    def reset(self) -> None:
        self._literal_types = []

    def get_literal_types(self) -> list[tuple[type, str]]:
        return self._literal_types

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        for arg in call_expression.inputs:
            arg.accept(self)

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        # Keyword arguments are already compared by type in
        # `WindowCallExpression.equals`.
        for arg in window_expression.inputs:
            arg.accept(self)
        for partition_arg in window_expression.partition_inputs:
            partition_arg.accept(self)
        for order_arg in window_expression.order_inputs:
            order_arg.expr.accept(self)

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        value: object = literal_expression.value
        self._literal_types.append((type(value), repr(value)))

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        pass

    def visit_correlated_reference(self, correlated_reference) -> None:
        pass
