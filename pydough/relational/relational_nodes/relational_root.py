"""
Representation of the root node for the final output of a relational tree.
This node is responsible for enforcing the final orderings and columns as well
as any other traits that impact the shape/display of the final output.
"""

from pydough.relational.relational_expressions import (
    ExpressionSortInfo,
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .single_relational import SingleRelational


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
        return (
            f"ROOT(columns=[{', '.join(columns)}], orderings=[{', '.join(orderings)}])"
        )

    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        visitor.visit_root(self)

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert len(inputs) == 1, "Root node should have exactly one input"
        assert columns == self.columns, "Root columns should not be modified"
        return RelationalRoot(inputs[0], self.ordered_columns, self.orderings)
