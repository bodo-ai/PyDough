"""
Definition of PyDough QDAG collection type for the BEST operator.
"""

__all__ = ["Best"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions.collation_expression import CollationExpression

from .child_access import ChildAccess
from .child_operator_child_access import ChildOperatorChildAccess
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class Best(ChildAccess):
    """
    The QDAG node implementation class representing the BEST operator, which
    finds the optimal instance of a subcollection with regards to the parent
    collection. The optimal instance is found via ordering expressions, but
    multiple optimal instances can be found if ties are allowed or multiple
    values are requested.
    """

    def __init__(
        self,
        child: ChildOperatorChildAccess,
        ancestor: PyDoughCollectionQDAG,
        allow_ties: bool = False,
        n_best: int = 1,
    ):
        super().__init__(ancestor)
        self._child: ChildOperatorChildAccess = child
        if n_best < 1:
            raise PyDoughQDAGException(f"Invalid n_best value: {n_best}")
        if n_best > 1 and allow_ties:
            raise PyDoughQDAGException(
                "Cannot allow ties when multiple best values are requested"
            )
        self._allow_ties: bool = allow_ties
        self._n_best: int = n_best

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> "Best":
        return Best(self.child, new_ancestor, self.allow_ties, self.n_best)

    @property
    def child(self) -> ChildOperatorChildAccess:
        """
        The child collection that the BEST node is finding the optimal
        instance(s) of.
        """
        return self._child

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
        # best values are requested.
        if self.n_best > 1 or self.allow_ties:
            return False
        relative_ancestor: PyDoughCollectionQDAG = (
            self.ancestor_context.starting_predecessor
        )
        return (context == relative_ancestor) or relative_ancestor.is_singular(context)

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.BEST({self.child.key})"

    @property
    def calc_terms(self) -> set[str]:
        raise NotImplementedError()

    @property
    def all_terms(self) -> set[str]:
        raise NotImplementedError()

    @property
    def ordering(self) -> list[CollationExpression] | None:
        raise NotImplementedError()

    def get_expression_position(self, expr_name: str) -> int:
        raise NotImplementedError()

    def get_term(self, term_name: str) -> PyDoughQDAG:
        raise NotImplementedError()

    @property
    def standalone_string(self) -> str:
        middle: str = ""
        if self.n_best > 1:
            middle = f", n_best={self.n_best}"
        elif self.allow_ties:
            middle = f", allow_ties={self.allow_ties})"
        collation_str: str = ""
        return f"BEST({self.child.to_string()}{middle}, by={collation_str})"

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        collation_str: str = ""
        return f"BEST[n={self.n_best},allow_ties={self.allow_ties},by={collation_str}]"

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        return CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
        )

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form(is_last)
        predecessor.has_children = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.depth = predecessor.depth + 1
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, Best)
            and self.preceding_context == other.preceding_context
            and self.child == other.child
            and self.allow_ties == other.allow_ties
            and self.n_best == other.n_best
        )
