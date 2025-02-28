"""
This file contains the relational implementation for an dummy relational node
with 1 row and 0 columns.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .relational_visitor import RelationalVisitor


class EmptySingleton(RelationalNode):
    """
    A node in the relational tree representing a constant table with 1 row and
    0 columns, for use in cases such as `SELECT 42 as A from (VALUES())`
    """

    def __init__(self) -> None:
        super().__init__({})

    @property
    def inputs(self) -> MutableSequence[RelationalNode]:
        return []

    def node_equals(self, other: RelationalNode) -> bool:
        return isinstance(other, EmptySingleton)

    def to_string(self, compact: bool = False) -> str:
        return "EMPTYSINGLETON()"

    def accept(self, visitor: RelationalVisitor) -> None:
        return visitor.visit_empty_singleton(self)

    def node_copy(
        self,
        columns: MutableMapping[str, RelationalExpression],
        inputs: MutableSequence[RelationalNode],
    ) -> RelationalNode:
        assert len(columns) == 0, "EmptySingleton has no columns"
        assert len(inputs) == 0, "EmptySingleton has no inputs"
        return EmptySingleton()
