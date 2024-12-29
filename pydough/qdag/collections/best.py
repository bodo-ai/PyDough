"""
Definition of PyDough QDAG collection type for the BEST operator.
"""

__all__ = ["Best"]


from collections.abc import MutableSequence
from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions import (
    CollationExpression,
    Reference,
)
from pydough.qdag.has_hasnot_rewrite import has_hasnot_rewrite

from .child_access import ChildAccess
from .child_operator import ChildOperator
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class Best(ChildOperator):
    """
    The QDAG node implementation class representing the BEST operator, which
    finds the optimal instance of the current context with regards to an
    ancestor collection. The optimal instance is found via ordering expressions,
    but multiple optimal instances can be found if ties are allowed or multiple
    values are requested.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        node: PyDoughCollectionQDAG,
        allow_ties: bool = False,
        n_best: int = 1,
    ):
        self._node: PyDoughCollectionQDAG = node
        self._children: MutableSequence[PyDoughCollectionQDAG] = [node]
        self._collation: list[CollationExpression] | None = None
        if n_best < 1:
            raise PyDoughQDAGException(f"Invalid n_best value: {n_best}")
        if n_best > 1 and allow_ties:
            raise PyDoughQDAGException(
                "Cannot allow ties when multiple best values are requested"
            )
        self._best_ancestor: PyDoughCollectionQDAG = ancestor
        self._allow_ties: bool = allow_ties
        self._n_best: int = n_best

    @property
    def node(self) -> PyDoughCollectionQDAG:
        """
        The node that the BEST operator is filtering with regards to an
        ancestor.
        """
        return self._node

    @property
    def best_ancestor(self) -> PyDoughCollectionQDAG:
        """
        The ancestor in the hierarchy that the BEST operator is finding the
        collection that the BEST operator is finding the optimal instance
        with regards to.
        """
        return self._best_ancestor

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

    def with_collation(self, collation: list[CollationExpression]) -> "Best":
        """
        Specifies the expressions that are used to do the ordering in an
        BEST node returning the mutated BEST node afterwards. This is
        called after the BEST node is created so that the terms can be
        expressions that reference child nodes of the BEST. However, this
        must be called on the BEST node before any properties are accessed
        by `calc_terms`, `all_terms`, `to_string`, etc.

        Args:
            `collation`: the list of collation nodes to order by.

        Returns:
            The mutated BEST node (which has also been modified in-place).

        Raises:
            `PyDoughQDAGException` if the condition has already been added to
            the WHERE node.
        """
        if self._collation is not None:
            raise PyDoughQDAGException(
                "Cannot call `with_collation` more than once per BEST node"
            )
        self._collation = [
            CollationExpression(
                has_hasnot_rewrite(col.expr, False), col.asc, col.na_last
            )
            for col in collation
        ]
        for col in self._collation:
            if not isinstance(col.expr, Reference):
                raise PyDoughQDAGException(
                    "Collation expressions inside of a BEST clause must be references"
                )
        return self

    @property
    def collation(self) -> list[CollationExpression]:
        """
        The ordering keys for the BEST clause.
        """
        if self._collation is None:
            raise PyDoughQDAGException(
                "Cannot access `collation` of an BEST node before calling `with_collation`"
            )
        return self._collation

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG:
        return self.best_ancestor

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG | None:
        return None

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
    def calc_terms(self) -> set[str]:
        return self.node.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.node.all_terms

    @property
    def key(self) -> str:
        return f"{self.best_ancestor.key}.BEST"

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.collation

    def get_expression_position(self, expr_name: str) -> int:
        return self.node.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        from pydough.qdag.expressions import PyDoughExpressionQDAG, Reference

        term: PyDoughQDAG = self.node.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        elif isinstance(term, PyDoughExpressionQDAG):
            term = Reference(self.node, term_name)
        return term

    @property
    def standalone_string(self) -> str:
        middle: str = ""
        if self.n_best > 1:
            middle = f"n_best={self.n_best}, "
        elif self.allow_ties:
            middle = f"allow_ties={self.allow_ties}, "
        collation_str: str = ", ".join([expr.to_string() for expr in self.collation])
        return f"BEST({self.node.to_string()}, {middle}by={collation_str})"

    def to_string(self) -> str:
        return f"{self.best_ancestor.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        middle: str = ""
        if self.n_best > 1:
            middle = f"n_best={self.n_best}, "
        elif self.allow_ties:
            middle = f"allow_ties={self.allow_ties}, "
        collation_str: str = ", ".join(
            [expr.to_string(True) for expr in self.collation]
        )
        return f"Best[{middle}by={collation_str}]"

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.best_ancestor.to_tree_form(is_last)
        predecessor.has_children = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.depth = predecessor.depth + 1
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Best)
            and self.preceding_context == other.preceding_context
            and self.allow_ties == other.allow_ties
            and self.n_best == other.n_best
        )
