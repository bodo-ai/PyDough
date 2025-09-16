"""
Definition of PyDough QDAG collection type for ordering the current collection
by certain collation keys.
"""

__all__ = ["OrderBy"]


from pydough.qdag.expressions import CollationExpression
from pydough.qdag.has_hasnot_rewrite import has_hasnot_rewrite

from .augmenting_child_operator import AugmentingChildOperator
from .collection_qdag import PyDoughCollectionQDAG


class OrderBy(AugmentingChildOperator):
    """
    The QDAG node implementation class representing an ORDER BY clause.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: list[PyDoughCollectionQDAG],
        collation: list[CollationExpression],
    ):
        super().__init__(predecessor, children)
        self._collation = [
            CollationExpression(
                has_hasnot_rewrite(col.expr, False), col.asc, col.na_last
            )
            for col in collation
        ]
        self.verify_singular_terms(self._collation)

    @property
    def collation(self) -> list[CollationExpression]:
        """
        The ordering keys for the ORDERBY clause.
        """
        return self._collation

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.ORDERBY"

    @property
    def ordering(self) -> list[CollationExpression]:
        return self.collation

    @property
    def standalone_string(self) -> str:
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"ORDER_BY({collation_str})"

    @property
    def tree_item_string(self) -> str:
        collation_str: str = ", ".join(
            [expr.to_string(True) for expr in self.collation]
        )
        return f"OrderBy[{collation_str}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, OrderBy)
            and self._collation == other._collation
            and super().equals(other)
        )
