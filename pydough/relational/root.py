"""
Representation of the root node for the final output of a relational tree.
This node is responsible for enforcing the final orderings and columns as well
as any other traits that impact the shape/display of the final output.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational
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
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None = None,
    ) -> None:
        super().__init__(input, columns, orderings)

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def node_equals(self, other: Relational) -> bool:
        return isinstance(other, RelationalRoot) and super().node_equals(other)

    def to_string(self) -> str:
        # TODO: Should we visit the input?
        return f"ROOT(columns={self.columns}, orderings={self.orderings})"

    def node_can_merge(self, other: Relational) -> bool:
        return False

    def merge(self, other: Relational) -> Relational:
        raise NotImplementedError("Cannot merge root nodes.")
