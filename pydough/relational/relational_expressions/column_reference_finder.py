"""
Find all unique column references in a relational expression.
"""

from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor
from .window_call_expression import WindowCallExpression

__all__ = ["ColumnReferenceFinder"]


class ColumnReferenceFinder(RelationalExpressionVisitor):
    """
    Find all unique column references in a relational expression.
    """

    def __init__(self) -> None:
        self._column_references: set[ColumnReference] = set()

    def reset(self) -> None:
        self._column_references = set()

    def get_column_references(self) -> set[ColumnReference]:
        return self._column_references

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        for arg in call_expression.inputs:
            arg.accept(self)

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        for arg in window_expression.inputs:
            arg.accept(self)
        for partition_arg in window_expression.partition_inputs:
            partition_arg.accept(self)
        for order_arg in window_expression.order_inputs:
            order_arg.expr.accept(self)

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        pass

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        self._column_references.add(column_reference)

    def visit_correlated_reference(self, correlated_reference) -> None:
        pass
