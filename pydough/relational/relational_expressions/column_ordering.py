"""
The representation of ordering for a column
within a relational node. This is not a proper
RelationalExpression because it cannot be used as
a component of other expressions, but it is heavily
tied to this definition.
"""

__all__ = ["ColumnOrdering"]

from dataclasses import dataclass

from .column_reference import ColumnReference


@dataclass
class ColumnOrdering:
    """Representation of a column ordering."""

    column: ColumnReference
    ascending: bool
    nulls_first: bool

    def to_string(self, compact: bool = False) -> str:
        if compact:
            suffix: str = f"{'asc' if self.ascending else 'desc'}_{'first' if self.nulls_first else 'last'}"
            return f"({self.column.to_string(compact)}):{suffix}"
        else:
            return f"ColumnSortInfo(column={self.column.to_string(compact)}, ascending={self.ascending}, nulls_first={self.nulls_first})"
