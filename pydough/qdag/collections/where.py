"""
Definition of PyDough QDAG collection type for filtering the current collection
by certain expression criteria.
"""

__all__ = ["Where"]


from pydough.qdag.expressions import PyDoughExpressionQDAG
from pydough.qdag.has_hasnot_rewrite import has_hasnot_rewrite

from .augmenting_child_operator import AugmentingChildOperator
from .collection_qdag import PyDoughCollectionQDAG


class Where(AugmentingChildOperator):
    """
    The QDAG node implementation class representing a WHERE filter.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: list[PyDoughCollectionQDAG],
        condition: PyDoughExpressionQDAG,
    ):
        super().__init__(predecessor, children)
        self._condition = has_hasnot_rewrite(condition, True)
        self.verify_singular_terms([self._condition])

    @property
    def condition(self) -> PyDoughExpressionQDAG:
        """
        The predicate expression for the WHERE clause.
        """
        return self._condition

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.WHERE"

    @property
    def standalone_string(self) -> str:
        return f"WHERE({self.condition.to_string()})"

    @property
    def tree_item_string(self) -> str:
        return f"Where[{self.condition.to_string(True)}]"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Where)
            and self._condition == other._condition
            and super().equals(other)
        )
