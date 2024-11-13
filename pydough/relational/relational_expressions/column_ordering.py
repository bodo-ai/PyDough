"""
The representation of ordering for a column
within a relational node. This is not a proper
RelationalExpression because it cannot be used as
a component of other expressions, but it is heavily
tied to this definition.
"""

__all__ = ["ColumnOrdering"]

from dataclasses import dataclass

from sqlglot.expressions import Expression as SQLGlotExpression

from .column_reference import ColumnReference


@dataclass
class ColumnOrdering:
    """Representation of a column ordering."""

    column: ColumnReference
    ascending: bool
    nulls_first: bool

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"ColumnOrdering(column={self.column}, ascending={self.ascending}, nulls_first={self.nulls_first})"
