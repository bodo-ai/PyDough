"""
TODO: add file-level docstring
"""

__all__ = ["ChildOperatorChildAccess"]


from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST

from .back_reference_collection import BackReferenceCollection
from .child_access import ChildAccess
from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm
from .hidden_back_reference_collection import HiddenBackReferenceCollection
from .sub_collection import SubCollection
from .table_collection import TableCollection


class ChildOperatorChildAccess(ChildAccess):
    """
    Special wrapper around a ChildAccess instance that denotes it as an
    immediate child of a CALC node, for the purposes of stringification.
    """

    def __init__(
        self,
        child_access: ChildAccess,
        is_last: bool,
    ):
        super().__init__(child_access.ancestor_context)
        self._child_access: ChildAccess = child_access
        self._is_last: bool = is_last

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        raise NotImplementedError

    @property
    def child_access(self) -> ChildAccess:
        """
        The ChildAccess node that is being wrapped.
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

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of a CALC node.
        if isinstance(self.child_access, HiddenBackReferenceCollection):
            return self.child_access.alias
        elif isinstance(
            self.child_access, (BackReferenceCollection, ChildOperatorChildAccess)
        ):
            return self.child_access.to_string()
        elif isinstance(self.child_access, TableCollection):
            return self.child_access.collection.name
        elif isinstance(self.child_access, SubCollection):
            return self.child_access.subcollection_property.name
        else:
            raise NotImplementedError

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            "AccessChild",
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        item_str: str
        if isinstance(self.child_access, TableCollection):
            item_str = f"TableCollection[{self.to_string()}]"
        else:
            item_str = f"SubCollection[{self.to_string()}]"
        return CollectionTreeForm(
            item_str,
            predecessor.depth + 1,
            predecessor=predecessor,
        )
