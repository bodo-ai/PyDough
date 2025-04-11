"""
Defines an abstract subclass of ChildOperator for operations that augment their
preceding context without stepping down into another context, like CALCULATE or
WHERE.
"""

__all__ = ["AugmentingChildOperator"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import CollationExpression, PyDoughExpressionQDAG

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
        children: list[PyDoughCollectionQDAG],
    ):
        self._preceding_context: PyDoughCollectionQDAG = predecessor
        self._children: list[PyDoughCollectionQDAG] = children

    @property
    def name(self) -> str:
        return self.preceding_context.name

    @property
    def ancestor_context(self) -> PyDoughCollectionQDAG | None:
        return self._preceding_context.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionQDAG:
        return self._preceding_context

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self.preceding_context.ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.preceding_context.inherited_downstreamed_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.preceding_context.ordering

    @property
    def calc_terms(self) -> set[str]:
        return self.preceding_context.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.preceding_context.all_terms

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
        from pydough.qdag.expressions import Reference

        term: PyDoughQDAG = self.preceding_context.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        elif isinstance(term, PyDoughExpressionQDAG):
            term = Reference(self.preceding_context, term_name)
        return term

    @cache
    def to_string(self) -> str:
        return f"{self.preceding_context.to_string()}.{self.standalone_string}"

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
