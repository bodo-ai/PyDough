"""
Find all unique column references in a relational expression.
"""

from .call_expression import CallExpression
from .column_reference import ColumnReference
from .correlated_reference import CorrelatedReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor
from .window_call_expression import WindowCallExpression

__all__ = ["CorrelatedReferenceFinder"]


class CorrelatedReferenceFinder(RelationalExpressionVisitor):
    """
    Find all unique correlated references in a relational expression.
    """

    def __init__(self) -> None:
        self._correlated_references: set[CorrelatedReference] = set()

    def reset(self) -> None:
        self._correlated_references = set()

    def get_correlated_references(self) -> set[CorrelatedReference]:
        return self._correlated_references

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
        pass

    def visit_correlated_reference(self, correlated_reference) -> None:
        self._correlated_references.add(correlated_reference)
