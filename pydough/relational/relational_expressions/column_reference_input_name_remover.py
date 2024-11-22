"""
Shuttle implementation designed to remove the input name from
any column reference that doesn't fall within a given set.
"""

from .abstract_expression import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_shuttle import RelationalExpressionShuttle

__all__ = ["ColumnReferenceInputNameRemover"]


class ColumnReferenceInputNameRemover(RelationalExpressionShuttle):
    """
    Shuttle implementation designed to remove the input name from
    any column reference whose name is not found in the given set.
    """

    def __init__(self, kept_names: set[str] | None = None) -> None:
        self._kept_names: set[str] = set() if kept_names is None else kept_names

    def set_kept_names(self, kept_names: set[str]) -> None:
        self._kept_names = kept_names

    def visit_call_expression(
        self, call_expression: CallExpression
    ) -> RelationalExpression:
        args = [args.accept_shuttle(self) for args in call_expression.inputs]
        return CallExpression(call_expression.op, call_expression.data_type, args)

    def visit_literal_expression(
        self, literal_expression: LiteralExpression
    ) -> RelationalExpression:
        return literal_expression

    def visit_column_reference(self, column_reference) -> RelationalExpression:
        if column_reference.name in self._kept_names:
            return column_reference
        else:
            return ColumnReference(
                column_reference.name,
                column_reference.data_type,
                None,
            )
