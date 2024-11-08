"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from collections.abc import MutableSequence

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST

from .abstract import Column, Relational


class Scan(Relational):
    """
    The Scan node in the relational tree. Right now these refer to tables
    stored within a provided database connection with is assumed to be singular
    and always available.
    """

    def __init__(
        self,
        table_name: str,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        self.table_name: str = table_name
        self._orderings: MutableSequence[PyDoughExpressionAST] = (
            orderings if orderings else []
        )
        self._columns = columns

    @property
    def inputs(self):
        # A scan is required to be the leaf node of the relational tree.
        return []

    @property
    def orderings(self) -> MutableSequence["PyDoughExpressionAST"]:
        return self._orderings

    @property
    def columns(self) -> MutableSequence["Column"]:
        return self._columns

    def equals(self, other: "Relational") -> bool:
        if not isinstance(other, Scan):
            return False
        return (
            self.table_name == other.table_name
            and self.columns == other.columns
            and self.orderings == other.orderings
        )

    def to_sqlglot(self) -> "Expression":
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"SCAN(table={self.table_name}, columns={self.columns}, orderings={self.orderings})"

    def can_merge(self, other: Relational) -> bool:
        if isinstance(other, Scan):
            # TODO: Allow merging compatible orderings.
            return (
                self.table_name == other.table_name
                and not self.orderings
                and not other.orderings
            )
        else:
            return False

    def merge(self, other: Relational) -> Relational:
        if not self.can_merge(other):
            raise ValueError(
                f"Cannot merge nodes {self.to_string()} and {other.to_string()}"
            )
        table_name = self.table_name
        # Note: Right now we assume that we aren't doing a "TREE" merge, so we can just
        # return a new node as though this node is the root of the tree. In the future
        # we may need to provide a mapping.
        col_set = set(self.columns)
        cols = list(self.columns)
        # Note: Since this is a scan we assume that column names must match the column
        # names exactly.
        for col in other.columns:
            if col not in col_set:
                cols.append(col)
        orderings = self.orderings
        return Scan(table_name, cols, orderings)
