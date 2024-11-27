"""
Handle the conversion from the Relation Expressions inside
the relation Tree to a single SQLGlot query component.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Binary, Identifier, Paren
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.relational import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionVisitor,
)

from .sqlglot_helpers import set_glot_alias

__all__ = ["SQLGlotRelationalExpressionVisitor"]

# SQLGlot doesn't have a clean interface for functional calls without
# going through the parser. As a result, we generate our own map for now.
# These functions can be generated with from_arg_list.
generic_func_map: dict[str, SQLGlotExpression] = {
    "ABS": sqlglot_expressions.Abs,
    "AVG": sqlglot_expressions.Avg,
    "LOWER": sqlglot_expressions.Lower,
    "LENGTH": sqlglot_expressions.Length,
    "MAX": sqlglot_expressions.Max,
    "SUM": sqlglot_expressions.Sum,
    "COUNT": sqlglot_expressions.Count,
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
    "*": sqlglot_expressions.Mul,
    "/": sqlglot_expressions.Div,
    "&": sqlglot_expressions.And,
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
            assert len(input_exprs) >= 2, "Need at least 2 binary inputs"
            # Note: SQLGlot explicit inserts parentheses for binary operations
            # during parsing.
            if isinstance(input_exprs[0], Binary):
                output_expr = Paren(this=input_exprs[0])
            else:
                output_expr = input_exprs[0]
            for expr in input_exprs[1:]:
                other_expr: SQLGlotExpression
                if isinstance(expr, Binary):
                    other_expr = Paren(this=expr)
                else:
                    other_expr = expr
                # Build the expressions on the left since the operator is left-associative.
                output_expr = binary_func_map[key](
                    this=output_expr, expression=other_expr
                )
        else:
            raise ValueError(f"Unsupported function {key}")
        self._stack.append(output_expr)

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
