"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

from sqlglot.expressions import Expression as SQLGlotExpression

from .abstract import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor

__all__ = ["SQLGlotRelationalExpressionVisitor"]


class SQLGlotRelationalExpressionVisitor(RelationalExpressionVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def reset(self) -> None:
        raise NotImplementedError("SQLGlotRelationalExpressionVisitor.reset")

    def visit(self, expr: RelationalExpression) -> None:
        raise NotImplementedError("SQLGlotRelationalExpressionVisitor.visit")

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        raise NotImplementedError(
            "SQLGlotRelationalExpressionVisitor.visit_call_expression"
        )

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        raise NotImplementedError(
            "SQLGlotRelationalExpressionVisitor.visit_literal_expression"
        )

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        raise NotImplementedError(
            "SQLGlotRelationalExpressionVisitor.visit_column_reference"
        )

    def relational_to_sqlglot(
        self, expr: RelationalExpression, output_name: str
    ) -> SQLGlotExpression:
        """
        Interface to convert an entire relational expression to a SQLGlot expression
        and assign it the given alias.

        Args:
            expr (RelationalExpression): The relational expression to convert.
            output_name (str): The name to assign to the final SQLGlot expression.

        Returns:
            SQLGlotExpression: The final SQLGlot expression representing the entire
                relational tree.
        """
        self.reset()
        expr.accept(self)
        result = self.get_sqlglot_result()
        result.set("alias", output_name)
        return result

    def get_sqlglot_result(self) -> SQLGlotExpression:
        """
        Interface to get the current SQLGlot expression result based on the current state.

        Returns:
            SQLGlotExpression: The SQLGlot expression representing the tree we have already
                visited.
        """
        raise NotImplementedError(
            "SQLGlotRelationalExpressionVisitor.get_sqlglot_result"
        )
