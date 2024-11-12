"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression, Select

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
from .single_relational import SingleRelational


class Project(SingleRelational):
    """
    The Project node in the relational tree. This node represents a "calc" in
    relational algebra, which should involve some "compute" functions and may
    involve adding, removing, or reordering columns.
    """

    def __init__(
        self,
        input: Relational,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None = None,
    ) -> None:
        super().__init__(input, columns, orderings)

    def input_modifying_to_sqlglot(self, input_expr: Expression) -> Expression:
        # TODO: Should we change the return type to always be Select?
        assert isinstance(input_expr, Select)
        new_columns: list[Expression] = [col.to_sqlglot() for col in self.columns]
        old_columns: list[Expression] = input_expr.expressions
        modified_new_columns: list[Expression] | None
        modified_old_columns: list[Expression]
        modified_new_columns, modified_old_columns = self.merge_sqlglot_columns(
            new_columns, old_columns, set()
        )
        input_expr.set("expressions", modified_old_columns)
        if modified_new_columns is None:
            return input_expr
        else:
            return Select(**{"expression": modified_new_columns, "from": input_expr})

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, Project) and super().node_equals(other)

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"PROJECT(columns={self.columns}, orderings={self.orderings})"

    def node_can_merge(self, other: Relational) -> bool:
        return isinstance(other, Project) and super().node_can_merge(other)

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        assert isinstance(other, Project)
        input = self.input
        cols = self.merge_columns(other.columns)
        orderings = self.orderings
        return Project(input, cols, orderings)
