"""
Visitor implementation designed to update all uses of a column reference's
input name to a new input name based on a dictionary.

We may eventually want to replace this implementation with a shuttle pattern
that returns a new expression to avoid the stack manipulation, but this can
be done as a followup.
"""

from .abstract_expression import RelationalExpression
from .call_expression import CallExpression
from .column_reference import ColumnReference
from .literal_expression import LiteralExpression
from .relational_expression_visitor import RelationalExpressionVisitor

__all__ = ["ColumnReferenceInputNameModifier"]


class ColumnReferenceInputNameModifier(RelationalExpressionVisitor):
    """
    Visitor implementation designed to update all uses of a column reference's
    input name to a new input name based on a dictionary.
    """

    def __init__(self, input_name_map: dict[str, str] | None = None) -> None:
        self._input_name_map: dict[str, str] = (
            {} if input_name_map is None else input_name_map
        )
        self._stack: list[RelationalExpression] = []

    def reset(self) -> None:
        self._stack = []

    def set_map(self, input_name_map: dict[str, str]) -> None:
        self._input_name_map = input_name_map

    def visit_call_expression(self, call_expression: CallExpression) -> None:
        # Visit the inputs in reverse order so we can pop them off in order.
        for arg in call_expression.inputs[::-1]:
            arg.accept(self)
        updated_arguments: list[RelationalExpression] = [
            self._stack.pop() for _ in range(len(call_expression.inputs))
        ]
        new_call = CallExpression(
            call_expression.op,
            call_expression.data_type,
            updated_arguments,
        )
        self._stack.append(new_call)

    def visit_literal_expression(self, literal_expression: LiteralExpression) -> None:
        self._stack.append(literal_expression)

    def visit_column_reference(self, column_reference) -> None:
        if column_reference.input_name in self._input_name_map:
            self._stack.append(
                ColumnReference(
                    column_reference.name,
                    column_reference.data_type,
                    self._input_name_map[column_reference.input_name],
                )
            )
        else:
            raise ValueError(
                f"Input name {column_reference.input_name} not found in the input name map."
            )

    def modify_expression_names(
        self, expression: RelationalExpression
    ) -> RelationalExpression:
        """
        Modify all column references in the expression to use the new
        input names.

        Args:
            expression (RelationalExpression): The original expression to
                modify.

        Returns:
            RelationalExpression: The new expression with the updated
                input names.
        """
        self.reset()
        expression.accept(self)
        assert len(self._stack) == 1, "Expected a single expression on the stack."
        return self._stack.pop()
