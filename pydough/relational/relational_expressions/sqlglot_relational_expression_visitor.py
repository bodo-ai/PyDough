"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier
from sqlglot.expressions import Literal as SQLGlotLiteral

from .abstract_expression import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor

__all__ = ["SQLGlotRelationalExpressionVisitor"]

# SQLGlot doesn't have a clean interface for functional calls without
# going through the parser. As a result, we generate our own map for now.
func_map: dict[str, SQLGlotExpression] = {
    "LOWER": sqlglot_expressions.Lower,
    "LENGTH": sqlglot_expressions.Length,
}


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

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        # Visit the inputs in reverse order so we can pop them off in order.
        for arg in call_expression.inputs[::-1]:
            arg.accept(self)
        input_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(call_expression.inputs))
        ]
        key: str = call_expression.op.function_name.upper()
        if key in func_map:
            # Create the function.
            self._stack.append(func_map[key].from_arg_list(input_exprs))
        else:
            raise ValueError(f"Unsupported function {key}")

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        # TODO: Handle data types.
        self._stack.append(SQLGlotLiteral(value=literal_expression.value))

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
