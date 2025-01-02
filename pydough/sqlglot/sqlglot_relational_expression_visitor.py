"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier

from pydough.relational import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionVisitor,
    WindowCallExpression,
)

from .sqlglot_helpers import set_glot_alias
from .transform_bindings import SqlGlotTransformBindings

__all__ = ["SQLGlotRelationalExpressionVisitor"]


class SQLGlotRelationalExpressionVisitor(RelationalExpressionVisitor):
    """
    The visitor pattern for creating SQLGlot expressions from
    the relational tree 1 node at a time.
    """

    def __init__(
        self, dialect: SQLGlotDialect, bindings: SqlGlotTransformBindings
    ) -> None:
        # Keep a stack of SQLGlot expressions so we can build up
        # intermediate results.
        self._stack: list[SQLGlotExpression] = []
        self._dialect: SQLGlotDialect = dialect
        self._bindings: SqlGlotTransformBindings = bindings

    def reset(self) -> None:
        """
        Reset just clears our stack.
        """
        self._stack = []

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        # Visit the inputs in reverse order so we can pop them off in order.
        for arg in reversed(call_expression.inputs):
            arg.accept(self)
        input_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(call_expression.inputs))
        ]
        output_expr: SQLGlotExpression = self._bindings.call(
            call_expression.op, call_expression.inputs, input_exprs
        )
        self._stack.append(output_expr)

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        raise NotImplementedError()

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # Note: This assumes each literal has an associated type that can be parsed
        # and types do not represent implicit casts.
        literal: SQLGlotExpression = sqlglot_expressions.convert(
            literal_expression.value
        )
        self._stack.append(literal)

    @staticmethod
    def generate_column_reference_identifier(
        column_reference: ColumnReference,
    ) -> Identifier:
        """
        Generate an identifier for a column reference. This is split into a
        separate static method to ensure consistency across multiple visitors.

        Args:
            column_reference (ColumnReference): The column reference to generate
                an identifier for.

        Returns:
            Identifier: The output identifier.
        """
        if column_reference.input_name is not None:
            full_name = f"{column_reference.input_name}.{column_reference.name}"
        else:
            full_name = column_reference.name
        return Identifier(this=full_name)

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        self._stack.append(self.generate_column_reference_identifier(column_reference))

    def relational_to_sqlglot(
        self, expr: RelationalExpression, output_name: str | None = None
    ) -> SQLGlotExpression:
        """
        Interface to convert an entire relational expression to a SQLGlot expression
        and assign it the given alias.

        Args:
            expr (RelationalExpression): The relational expression to convert.
            output_name (str | None): The name to assign to the final SQLGlot expression
                or None if we should omit any alias.

        Returns:
            SQLGlotExpression: The final SQLGlot expression representing the entire
                relational tree.
        """
        self.reset()
        expr.accept(self)
        result = self.get_sqlglot_result()
        return set_glot_alias(result, output_name)

    def get_sqlglot_result(self) -> SQLGlotExpression:
        """
        Interface to get the current SQLGlot expression result based on the current state.

        Returns:
            SQLGlotExpression: The SQLGlot expression representing the tree we have already
                visited.
        """
        assert len(self._stack) == 1, "Expected exactly one expression on the stack"
        return self._stack[0]
