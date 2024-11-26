"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import RelationalExpression

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor
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
        columns: MutableMapping[str, RelationalExpression],
    ) -> None:
        super().__init__(input, columns)

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, Project) and super().node_equals(other)

    def to_string(self) -> str:
        return f"PROJECT(columns={self.columns})"

    def accept(self, visitor: RelationalVisitor) -> None:
        return visitor.visit_project(self)

    def node_copy(
        self,
        columns: MutableMapping[str, RelationalExpression],
        inputs: MutableSequence[Relational],
    ) -> Relational:
        assert len(inputs) == 1, "Project node should have exactly one input"
        return Project(inputs[0], columns)
