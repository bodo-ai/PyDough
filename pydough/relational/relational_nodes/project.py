"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calculate" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from pydough.relational.relational_expressions import (
    ColumnReference,
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .single_relational import SingleRelational


class Project(SingleRelational):
    """
    The Project node in the relational tree. This node represents a "calculate"
    in relational algebra, which should involve some "compute" functions and
    may involve adding, removing, or reordering columns.
    """

    def __init__(
        self,
        input: RelationalNode,
        columns: dict[str, RelationalExpression],
    ) -> None:
        super().__init__(input, columns)

    def node_equals(self, other: RelationalNode) -> bool:
        return isinstance(other, Project) and super().node_equals(other)

    def to_string(self, compact: bool = False) -> str:
        return f"PROJECT(columns={self.make_column_string(self.columns, compact)})"

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        return visitor.visit_project(self)

    def is_identity(self) -> bool:
        """
        Checks if a project is an identity project. This means that
        every column is just a mapping to a column of the same name.
        """
        return all(
            isinstance(val, ColumnReference)
            and key == val.name
            and val.input_name is None
            for key, val in self.columns.items()
        )

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Project node should have exactly one input"
        return Project(inputs[0], columns)
