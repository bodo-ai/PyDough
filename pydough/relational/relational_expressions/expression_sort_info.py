"""
The representation of ordering for an expression within a relational node.
This is not a proper RelationalExpression because it cannot be used as
a component of other expressions, but it is heavily tied to this definition.
"""

__all__ = ["ExpressionSortInfo"]

from dataclasses import dataclass

from .abstract_expression import RelationalExpression


@dataclass
class ExpressionSortInfo:
    """Representation of an expression ordering."""

    expr: RelationalExpression
    ascending: bool
    nulls_first: bool

    def to_string(self) -> str:
        return f"ExpressionSortInfo(expression={self.expr}, ascending={self.ascending}, nulls_first={self.nulls_first})"

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return self.to_string()
