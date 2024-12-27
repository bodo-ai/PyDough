"""
Definition of PyDough QDAG collection type for the BEST operator.
"""

__all__ = ["Best"]


from collections.abc import MutableSequence
from functools import cache

from pydough.qdag.errors import PyDoughQDAGException

from .collection_qdag import PyDoughCollectionQDAG
from .order_by import OrderBy


class Best(OrderBy):
    """
    The QDAG node implementation class representing the BEST operator, which
    finds the optimal instance of the current context with regards to an
    ancestor collection. The optimal instance is found via ordering expressions,
    but multiple optimal instances can be found if ties are allowed or multiple
    values are requested.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
        ancestor_levels: int,
        best_ancestor: PyDoughCollectionQDAG,
        allow_ties: bool = False,
        n_best: int = 1,
    ):
        super().__init__(predecessor, children)
        if n_best < 1:
            raise PyDoughQDAGException(f"Invalid n_best value: {n_best}")
        if n_best > 1 and allow_ties:
            raise PyDoughQDAGException(
                "Cannot allow ties when multiple best values are requested"
            )
        self._ancestor_levels: int = ancestor_levels
        self._best_ancestor: PyDoughCollectionQDAG = best_ancestor
        self._allow_ties: bool = allow_ties
        self._n_best: int = n_best

    @property
    def best_ancestor(self) -> PyDoughCollectionQDAG:
        """
        The ancestor in the hierarchy that the BEST operator is finding the
        collection that the BEST operator is finding the optimal instance
        with regards to.
        """
        return self._best_ancestor

    @property
    def ancestor_levels(self) -> int:
        """
        The number of levels upward that the best_ancestor is in the hierarchy.
        """
        return self._ancestor_levels

    @property
    def allow_ties(self) -> bool:
        """
        Whether the BEST operator allows ties to be returned when multiple
        optimal instances are found. Cannot be True if n_best is greater than
        1.
        """
        return self._allow_ties

    @property
    def n_best(self) -> int:
        """
        The number of optimal instances to find when using the BEST operator.
        If 1, only the best instance is returned. If greater than 1, multiple
        records are retrieved up to the specified value, without ties.
        """
        return self._n_best

    @cache
    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # The BEST operator is always if its parent is singular with
        # regards to the context, unless ties are allowed or multiple
        # best values are requested, in which case it depends on whether
        # the context the BEST operator is being used on is singular with
        # regards to the ancestor context.
        is_singular_best: bool = self.n_best == 1 and not self.allow_ties
        relative_ancestor: PyDoughCollectionQDAG = self.starting_predecessor
        relative_best_ancestor: PyDoughCollectionQDAG = (
            self.best_ancestor.starting_predecessor
        )
        if context == relative_best_ancestor:
            return is_singular_best

        # See if the context is between the current level and the best answer
        is_singular_so_far: bool = True
        while True:
            if relative_ancestor.ancestor_context is None:
                return False
            old_ancestor: PyDoughCollectionQDAG = relative_ancestor
            relative_ancestor = relative_ancestor.ancestor_context.starting_predecessor
            if is_singular_so_far and old_ancestor.is_singular(relative_ancestor):
                is_singular_so_far = False
            if relative_ancestor == relative_best_ancestor:
                break
            if context == relative_ancestor:
                return is_singular_best or is_singular_so_far

        return (is_singular_best or is_singular_so_far) and (
            relative_ancestor.is_singular(context)
        )

    @property
    def key(self) -> str:
        return f"{self.preceding_context.key}.BEST"

    @property
    def standalone_string(self) -> str:
        middle: str = ""
        if self.n_best > 1:
            middle = f"n_best={self.n_best}, "
        elif self.allow_ties:
            middle = f"allow_ties={self.allow_ties}), "
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"BEST({middle}levels={self.ancestor_levels}, by={collation_str})"

    @property
    def tree_item_string(self) -> str:
        middle: str = ""
        if self.n_best > 1:
            middle = f"n_best={self.n_best}, "
        elif self.allow_ties:
            middle = f"allow_ties={self.allow_ties}), "
        collation_str: str = ", ".join(
            [expr.to_string(True) for expr in self.collation]
        )
        return f"BEST[{middle}levels={self.ancestor_levels}, by={collation_str}]"

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, Best)
            and self.preceding_context == other.preceding_context
            and self.ancestor_levels == other.ancestor_levels
            and self.allow_ties == other.allow_ties
            and self.n_best == other.n_best
        )
