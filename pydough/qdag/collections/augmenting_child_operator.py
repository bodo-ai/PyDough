"""
Defines an abstract subclass of ChildOperator for operations that augment their
preceding context without stepping down into another context, like CALC or
WHERE.
"""

__all__ = ["AugmentingChildOperator"]

from collections.abc import MutableSequence
from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import CollationExpression

from .child_access import ChildAccess
from .child_operator import ChildOperator
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class AugmentingChildOperator(ChildOperator):
    """
    Base class for PyDough collection QDAG nodes that are child operators
    and build off of their preceding context.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionQDAG,
        children: MutableSequence[PyDoughCollectionQDAG],
    ):
        self._preceding_context: PyDoughCollectionQDAG = predecessor
        self._children: MutableSequence[PyDoughCollectionQDAG] = children

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG | None:
        return self._preceding_context.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG:
        return self._preceding_context

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.preceding_context.ordering

    @property
    def unique_terms(self) -> list[str]:
        return self.preceding_context.unique_terms

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # A child operator, by default, inherits singular/plural relationships
        # from its predecessor.
        return self.preceding_context.is_singular(context)

    def get_expression_position(self, expr_name: str) -> int:
        return self.preceding_context.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        from pydough.qdag.expressions import PyDoughExpressionQDAG, Reference

        term: PyDoughQDAG = self.preceding_context.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        elif isinstance(term, PyDoughExpressionQDAG):
            term = Reference(self.preceding_context, term_name)
        return term

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.preceding_context.to_tree_form(is_last)
        predecessor.has_successor = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated(is_last)
        tree_form.depth = predecessor.depth
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, AugmentingChildOperator)
            and self.preceding_context == other.preceding_context
        )
