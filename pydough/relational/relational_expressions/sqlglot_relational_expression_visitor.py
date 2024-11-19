"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier
from sqlglot.expressions import Literal as SQLGlotLiteral

from .abstract_expression import RelationalExpression
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

    def __init__(self) -> None:
        # Keep a stack of SQLGlot expressions so we can build up
        # intermediate results.
        self._stack: list[SQLGlotExpression] = []

    def reset(self) -> None:
        """
        Reset just clears our stack.
        """
        self._stack = []

    def visit(self, expr: RelationalExpression) -> None:
        raise NotImplementedError("SQLGlotRelationalExpressionVisitor.visit")

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        raise NotImplementedError(
            "SQLGlotRelationalExpressionVisitor.visit_call_expression"
        )

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # TODO: Handle data types.
        self._stack.append(SQLGlotLiteral(value=literal_expression.value))

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        if column_reference.input_name is not None:
            name = f"{column_reference.input_name}.{column_reference.name}"
        else:
            name = column_reference.name
        self._stack.append(Identifier(this=name))

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
        assert len(self._stack) == 1, "Expected exactly one expression on the stack"
        return self._stack[0]
