"""
Definition of PyDough QDAG collection type for ordering the current collection
by certain collation keys.
"""

__all__ = ["OrderBy"]


from functools import cache

from pydough.qdag.errors import PyDoughQDAGException
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
    ):
        super().__init__(predecessor, children)
        self._collation: list[CollationExpression] | None = None

    def with_collation(self, collation: list[CollationExpression]) -> "OrderBy":
        """
        Specifies the expressions that are used to do the ordering in an
        ORDERBY node returning the mutated ORDERBY node afterwards. This is
        called after the ORDERBY node is created so that the terms can be
        expressions that reference child nodes of the ORDERBY. However, this
        must be called on the ORDERBY node before any properties are accessed
        by `calc_terms`, `all_terms`, `to_string`, etc.

        Args:
            `collation`: the list of collation nodes to order by.

        Returns:
            The mutated ORDERBY node (which has also been modified in-place).

        Raises:
            `PyDoughQDAGException` if the condition has already been added to
            the WHERE node.
        """
        if self._collation is not None:
            raise PyDoughQDAGException(
                "Cannot call `with_collation` more than once per ORDERBY node"
            )
        self._collation = [
            CollationExpression(
                has_hasnot_rewrite(col.expr, False), col.asc, col.na_last
            )
            for col in collation
        ]
        self.verify_singular_terms(self._collation)
        return self

    @property
    def collation(self) -> list[CollationExpression]:
        """
        The ordering keys for the ORDERBY clause.
        """
        if self._collation is None:
            raise PyDoughQDAGException(
                "Cannot access `collation` of an ORDERBY node before calling `with_collation`"
            )
        return self._collation

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.ORDERBY"

    @property
    def ordering(self) -> list[CollationExpression]:
        return self.collation

    @property
    @cache
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
