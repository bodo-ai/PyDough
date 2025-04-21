"""
Shuttle implementation designed to update all uses of a column reference's
input name to a new input name based on a dictionary.
"""

from .abstract_expression import RelationalExpression
from .column_reference import ColumnReference
from .correlated_reference import CorrelatedReference
from .literal_expression import LiteralExpression
from .relational_expression_shuttle import RelationalExpressionShuttle

__all__ = ["ColumnReferenceInputNameModifier"]


class ColumnReferenceInputNameModifier(RelationalExpressionShuttle):
    """
    Shuttle implementation designed to update all uses of a column reference's
    input name to a new input name based on a dictionary.
    """

    def __init__(self, input_name_map: dict[str | None, str] | None = None) -> None:
        self._input_name_map: dict[str | None, str] = (
            {} if input_name_map is None else input_name_map
        )

    def set_map(self, input_name_map: dict[str | None, str]) -> None:
        self._input_name_map = input_name_map

    def visit_literal_expression(
        self, literal_expression: LiteralExpression
    ) -> RelationalExpression:
        return literal_expression

    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        if column_reference.input_name is None:
            # We ignore remapping any references without input names.
            # This is useful for handling unique names.
            return column_reference
        elif column_reference.input_name in self._input_name_map:
            return ColumnReference(
                column_reference.name,
                column_reference.data_type,
                self._input_name_map[column_reference.input_name],
            )
        else:
            raise ValueError(
                f"Input name {column_reference.input_name} not found in the input name map."
            )

    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> RelationalExpression:
        return correlated_reference
