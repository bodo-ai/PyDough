"""
This file contains the relational implementation for a "project". This is our
relational representation for a "calc" that involves any compute steps and can include
adding or removing columns (as well as technically reordering). In general, we seek to
avoid introducing extra nodes just to reorder or prune columns, so ideally their use
should be sparse.
"""

from collections.abc import MutableMapping, MutableSequence

from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .abstract_node import Relational
from .relational_visitor import RelationalVisitor


class EmptyValues(Relational):
    """
    The Values node in the relational tree. This node represents a constant
    table with 1 row and 0 columns.
    """

    def __init__(self) -> None:
        super().__init__({})

    @property
    def inputs(self) -> MutableSequence[Relational]:
        return []

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, EmptyValues)

    def to_string(self, compact: bool = False) -> str:
        return "EmptyValues()"

    def accept(self, visitor: RelationalVisitor) -> None:
        return visitor.visit_empty_values(self)

    def node_copy(
        self,
        columns: MutableMapping[str, RelationalExpression],
        inputs: MutableSequence[Relational],
    ) -> Relational:
        assert len(columns) == 0, "EmptyValues has no columns"
        assert len(inputs) == 0, "EmptyValues has no inputs"
        return EmptyValues()
