"""
Representation of the root node for the final output of a relational tree.
This node is responsible for enforcing the final orderings and columns as well
as any other traits that impact the shape/display of the final output.
"""

from typing import TYPE_CHECKING

from pydough.relational.relational_expressions import (
    ExpressionSortInfo,
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .single_relational import SingleRelational

if TYPE_CHECKING:
    from .relational_shuttle import RelationalShuttle
    from .relational_visitor import RelationalVisitor


class RelationalRoot(SingleRelational):
    """
    The Root node in any relational tree. At the SQL conversion step it
    needs to ensure that columns are in the correct order and any
    orderings/traits are enforced.
    """

    def __init__(
        self,
        input: RelationalNode,
        ordered_columns: list[tuple[str, RelationalExpression]],
        orderings: list[ExpressionSortInfo] | None = None,
        limit: RelationalExpression | None = None,
    ) -> None:
        columns = dict(ordered_columns)
        assert len(columns) == len(ordered_columns), (
            "Duplicate column names found in root."
        )
        super().__init__(input, columns)
        self._ordered_columns: list[tuple[str, RelationalExpression]] = ordered_columns
        self._orderings: list[ExpressionSortInfo] = (
            [] if orderings is None else orderings
        )
        self._limit: RelationalExpression | None = limit

    @property
    def ordered_columns(self) -> list[tuple[str, RelationalExpression]]:
        """
        The columns in the final order that the output should be in.
        """
        return self._ordered_columns

    @property
    def orderings(self) -> list[ExpressionSortInfo]:
        """
        The orderings that are used to determine the final output order if
        any.
        """
        return self._orderings

    @property
    def limit(self) -> RelationalExpression | None:
        """
        The limit on the number of rows in the final output, if any.
        """
        return self._limit

    def node_equals(self, other: RelationalNode) -> bool:
        return (
            isinstance(other, RelationalRoot)
            and self.ordered_columns == other.ordered_columns
            and self.orderings == other.orderings
            and super().node_equals(other)
        )

    def to_string(self, compact: bool = False) -> str:
        columns: list[str] = [
            f"({name!r}, {col.to_string(compact)})"
            for name, col in self.ordered_columns
        ]
        orderings: list[str] = [
            ordering.to_string(compact) for ordering in self.orderings
        ]
        kwargs: list[tuple[str, str]] = [
            ("columns", f"[{', '.join(columns)}]"),
            ("orderings", f"[{', '.join(orderings)}]"),
        ]
        if self.limit is not None:
            kwargs.append(("limit", self.limit.to_string(compact)))
        return f"ROOT({', '.join(f'{k}={v}' for k, v in kwargs)})"

    def accept(self, visitor: "RelationalVisitor") -> None:
        visitor.visit_root(self)

    def accept_shuttle(self, shuttle: "RelationalShuttle") -> RelationalNode:
        return shuttle.visit_root(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Root node should have exactly one input"
        assert columns == self.columns, "Root columns should not be modified"
        return RelationalRoot(
            inputs[0], self.ordered_columns, self.orderings, self.limit
        )
