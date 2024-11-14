"""
The representation of ordering for a column within a relational node.
This is not a proper RelationalExpression because it cannot be used as
a component of other expressions, but it is heavily tied to this definition.
"""

__all__ = ["ColumnSortInfo"]

from dataclasses import dataclass

from .column_reference import ColumnReference


@dataclass
class ColumnSortInfo:
    """Representation of a column ordering."""

    column: ColumnReference
    ascending: bool
    nulls_first: bool

    def to_string(self) -> str:
        return f"ColumnSortInfo(column={self.column}, ascending={self.ascending}, nulls_first={self.nulls_first})"
