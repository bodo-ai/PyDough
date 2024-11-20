"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier
from sqlglot.expressions import Literal as SQLGlotLiteral

from pydough.relational.relational_expressions import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionVisitor,
)
from pydough.types import DecimalType, PyDoughType
from pydough.types.integer_types import IntegerType

from .sqlglot_helpers import set_glot_alias

__all__ = ["SQLGlotRelationalExpressionVisitor"]

# SQLGlot doesn't have a clean interface for functional calls without
# going through the parser. As a result, we generate our own map for now.
# These functions can be generated with from_arg_list.
generic_func_map: dict[str, SQLGlotExpression] = {
    "LOWER": sqlglot_expressions.Lower,
    "LENGTH": sqlglot_expressions.Length,
    "SUM": sqlglot_expressions.Sum,
}
# These functions need an explicit constructor for binary.
binary_func_map: dict[str, SQLGlotExpression] = {
    "==": sqlglot_expressions.EQ,
    ">=": sqlglot_expressions.GTE,
    ">": sqlglot_expressions.GT,
    "<=": sqlglot_expressions.LTE,
    "<": sqlglot_expressions.LT,
    "!=": sqlglot_expressions.NEQ,
    "+": sqlglot_expressions.Add,
    "-": sqlglot_expressions.Sub,
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
        for arg in reversed(call_expression.inputs):
            arg.accept(self)
        input_exprs: list[SQLGlotExpression] = [
            self._stack.pop() for _ in range(len(call_expression.inputs))
        ]
        key: str = call_expression.op.function_name.upper()
        output_expr: SQLGlotExpression
        if key in generic_func_map:
            output_expr = generic_func_map[key].from_arg_list(input_exprs)
        elif key in binary_func_map:
            assert (
                len(input_exprs) == 2
            ), "Expected exactly two inputs for binary function"
            output_expr = binary_func_map[key](
                this=input_exprs[0], expression=input_exprs[1]
            )
        else:
            raise ValueError(f"Unsupported function {key}")
        self._stack.append(output_expr)

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        pydough_type: PyDoughType = literal_expression.data_type
        is_string: bool
        if isinstance(pydough_type, (IntegerType, DecimalType)):
            is_string = False
        else:
            # TODO: Handle casting for non-string literal types.
            is_string = True
        self._stack.append(
            SQLGlotLiteral(this=str(literal_expression.value), is_string=is_string)
        )

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
        if output_name is not None:
            result = set_glot_alias(result, output_name)
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
