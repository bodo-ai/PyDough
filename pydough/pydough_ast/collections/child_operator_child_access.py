"""
TODO: add file-level docstring
"""

__all__ = ["ChildOperatorChildAccess"]


from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST

from .child_access import ChildAccess
from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class ChildOperatorChildAccess(ChildAccess):
    """
    Special wrapper around a collection node instance that denotes it as an
    immediate child of a ChildOperator node, for the purposes of stringification.
    """

    def __init__(
        self,
        child_access: PyDoughCollectionAST,
        is_last: bool,
    ):
        ancestor = child_access.ancestor_context
        assert ancestor is not None
        super().__init__(ancestor)
        self._child_access: PyDoughCollectionAST = child_access
        self._is_last: bool = is_last

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        raise NotImplementedError

    @property
    def child_access(self) -> PyDoughCollectionAST:
        """
        The collection node that is being wrapped.
        """
        return self._child_access

    @property
    def is_last(self) -> bool:
        """
        Whether this is the last child of the CALC
        """
        return self._is_last

    @property
    def calc_terms(self) -> set[str]:
        return self.child_access.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.child_access.all_terms

    def get_expression_position(self, expr_name: str) -> int:
        return self.child_access.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughAST:
        term = self.child_access.get_term(term_name)
        if isinstance(term, ChildAccess):
            term = term.clone_with_parent(self)
        return term

    @property
    def standalone_string(self) -> str:
        return self.child_access.standalone_string

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of a CALC node.
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        return "AccessChild"

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        return CollectionTreeForm(
            self.child_access.tree_item_string,
            predecessor.depth + 1,
            predecessor=predecessor,
        )
