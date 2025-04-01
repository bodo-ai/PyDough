"""
Definition of PyDough QDAG collection type for an access to a child context done
by a child operator, as opposed to stepping down into the child access.
"""

__all__ = ["ChildOperatorChildAccess"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG

from .child_access import ChildAccess
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class ChildOperatorChildAccess(ChildAccess):
    """
    Special wrapper around a collection node instance that denotes it as an
    immediate child of a ChildOperator node, for the purposes of stringification.
    """

    def __init__(
        self,
        child_access: PyDoughCollectionQDAG,
    ):
        ancestor = child_access.ancestor_context
        assert ancestor is not None
        super().__init__(ancestor)
        self._child_access: PyDoughCollectionQDAG = child_access

    @property
    def name(self) -> str:
        return self.child_access.name

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> ChildAccess:
        raise NotImplementedError

    @property
    def child_access(self) -> PyDoughCollectionQDAG:
        """
        The collection node that is being wrapped.
        """
        return self._child_access

    @property
    def key(self) -> str:
        return self.child_access.key

    @property
    def calc_terms(self) -> set[str]:
        return self.child_access.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.child_access.all_terms

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self.child_access.ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.child_access.inherited_downstreamed_terms

    @property
    def unique_terms(self) -> list[str]:
        return self.child_access.unique_terms

    def get_expression_position(self, expr_name: str) -> int:
        return self.child_access.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        term = self.child_access.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        return term

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # When a child operator acceses a child collection, the child is
        # singular with regards to a context if the child is singular
        # relative to its own parent, and one of 3 other conditions is true:
        # 1. The child access is a BACK node (automatically singular)
        # 2. The parent of the child access is the context
        # 3. The parent of the child access is singular relative to the context
        ancestor: PyDoughCollectionQDAG | None = self.child_access.ancestor_context
        assert ancestor is not None
        relative_context: PyDoughCollectionQDAG = ancestor.starting_predecessor
        return self.child_access.is_singular(relative_context) and (
            (context == relative_context) or relative_context.is_singular(context)
        )

    @property
    def standalone_string(self) -> str:
        return self.child_access.standalone_string

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of an operator such as a CALCULATE node.
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        return "AccessChild"

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not is_last,
        )
        tree_form: CollectionTreeForm = self.child_access.to_tree_form_isolated(False)
        tree_form.depth = predecessor.depth + 1
        tree_form.predecessor = predecessor
        return tree_form

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        return self.to_tree_form_isolated(is_last)

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ChildOperatorChildAccess)
            and super().equals(other)
            and self.child_access.equals(other.child_access)
        )
