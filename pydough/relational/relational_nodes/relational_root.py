"""
Representation of the root node for the final output of a relational tree.
This node is responsible for enforcing the final orderings and columns as well
as any other traits that impact the shape/display of the final output.
"""

from collections.abc import MutableSequence

from pydough.relational.relational_expressions import (
    ColumnSortInfo,
    RelationalExpression,
)

from .abstract_node import Relational
from .single_relational import SingleRelational


class RelationalRoot(SingleRelational):
    """
    The Root node in any relational tree. At the SQL conversion step it
    needs to ensure that columns are in the correct order and any
    orderings/traits are enforced.
    """

    def __init__(
        self,
        input: Relational,
        ordered_columns: MutableSequence[tuple[str, RelationalExpression]],
        orderings: MutableSequence[ColumnSortInfo] | None = None,
    ) -> None:
        columns = dict(ordered_columns)
        assert len(columns) == len(
            ordered_columns
        ), "Duplicate column names found in root."
        super().__init__(input, columns)
        self._ordered_columns: MutableSequence[tuple[str, RelationalExpression]] = (
            ordered_columns
        )
        self._orderings: MutableSequence[ColumnSortInfo] = (
            [] if orderings is None else orderings
        )

    @property
    def ordered_columns(self) -> MutableSequence[tuple[str, RelationalExpression]]:
        return self._ordered_columns

    @property
    def orderings(self) -> MutableSequence[ColumnSortInfo]:
        return self._orderings

    def node_equals(self, other: Relational) -> bool:
        return (
            isinstance(other, RelationalRoot)
            and self.ordered_columns == other.ordered_columns
            and self.orderings == other.orderings
            and super().node_equals(other)
        )

    def to_string(self) -> str:
        return f"ROOT(columns={self.ordered_columns}, orderings={self.orderings})"
