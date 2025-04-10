"""
Definition of PyDough QDAG collection type for filtering the current collection
by certain expression criteria.
"""

__all__ = ["Where"]


from functools import cache

from pydough.qdag.errors import PyDoughQDAGException
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
    ):
        super().__init__(predecessor, children)
        self._condition: PyDoughExpressionQDAG | None = None

    def with_condition(self, condition: PyDoughExpressionQDAG) -> "Where":
        """
        Specifies the condition that should be used by the WHERE node. This is
        called after the WHERE node is created so that the condition can be an
        expressions that reference child nodes of the WHERE. However, this must
        be called on the WHERE node before any properties are accessed by
        `to_string`, `equals`, etc.

        Args:
            `condition`: the expression used to filter.

        Returns:
            The mutated WHERE node (which has also been modified in-place).

        Raises:
            `PyDoughQDAGException` if the condition has already been added to
            the WHERE node.
        """
        if self._condition is not None:
            raise PyDoughQDAGException(
                "Cannot call `with_condition` more than once per Where node"
            )
        self._condition = has_hasnot_rewrite(condition, True)
        self.verify_singular_terms([self._condition])
        return self

    @property
    def condition(self) -> PyDoughExpressionQDAG:
        """
        The predicate expression for the WHERE clause.
        """
        if self._condition is None:
            raise PyDoughQDAGException(
                "Cannot access `condition` of a WHERE node before adding the predicate with `with_condition`"
            )
        return self._condition

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.WHERE"

    @property
    @cache
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
