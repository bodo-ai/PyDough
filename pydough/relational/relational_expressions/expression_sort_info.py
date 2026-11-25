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

    @staticmethod
    def deduplicate(
        orderings: list["ExpressionSortInfo"],
    ) -> list["ExpressionSortInfo"]:
        """
        Returns a new list with repeated sort keys removed, keeping the first
        occurrence of each sort expression (with its direction and NULL
        placement) and the original order.

        Keys are compared by expression only. Rows that are still tied when a
        later key is reached already have equal values (or NULL) for every
        earlier key, so a later key on an expression that appeared before can
        never break a tie, whatever its direction or NULL placement.
        Expressions are compared with `RelationalExpression.is_same_key`, so
        keys such as `x + 1` and `x + 1.0` are both kept.

        Args:
            `orderings`: The sort keys to deduplicate.

        Returns:
            The sort keys without repeated expressions.
        """
        result: list[ExpressionSortInfo] = []
        seen: list[RelationalExpression] = []
        for ordering in orderings:
            if not any(ordering.expr.is_same_key(expr) for expr in seen):
                seen.append(ordering.expr)
                result.append(ordering)
        return result

    @staticmethod
    def same_orderings(
        first: list["ExpressionSortInfo"], second: list["ExpressionSortInfo"]
    ) -> bool:
        """
        Returns whether two lists of sort keys are interchangeable: same
        length, and each pair of keys has the same direction, the same NULL
        placement and expressions that are the same key according to
        `RelationalExpression.is_same_key`.

        Args:
            `first`: The first list of sort keys.
            `second`: The second list of sort keys.

        Returns:
            Whether the two lists of sort keys are interchangeable.
        """
        return len(first) == len(second) and all(
            first_ordering.expr.is_same_key(second_ordering.expr)
            and first_ordering.ascending == second_ordering.ascending
            and first_ordering.nulls_first == second_ordering.nulls_first
            for first_ordering, second_ordering in zip(first, second)
        )

    def to_string(self, compact: bool = False) -> str:
        if compact:
            suffix: str = f"{'asc' if self.ascending else 'desc'}_{'first' if self.nulls_first else 'last'}"
            return f"({self.expr.to_string(compact)}):{suffix}"
        else:
            return f"ExpressionSortInfo(expression={self.expr.to_string(compact)}, ascending={self.ascending}, nulls_first={self.nulls_first})"

    def __str__(self) -> str:
        return self.to_string()

    def __repr__(self) -> str:
        return self.to_string()
