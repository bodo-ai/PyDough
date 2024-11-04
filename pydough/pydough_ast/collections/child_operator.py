"""
TODO: add file-level docstring
"""

__all__ = ["ChildOperator"]


from typing import List, Set
from abc import abstractmethod

from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm
from .calc_child_collection import CalcChildCollection


class ChildOperator(PyDoughCollectionAST):
    """
    Base class for PyDough collection AST nodes that have access to
    child collections, such as CALC or WHERE.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: List[CalcChildCollection],
    ):
        self._predecessor: PyDoughCollectionAST = predecessor
        self._children: List[CalcChildCollection] = children

    @property
    def children(self) -> List[CalcChildCollection]:
        """
        The child collections accessible from the CALC used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor

    @property
    def calc_terms(self) -> Set[str]:
        return set(self.calc_term_indices)

    @property
    def all_terms(self) -> Set[str]:
        if self._all_terms is None:
            raise PyDoughCollectionAST(
                "Cannot invoke `all_terms` before calling `with_terms`"
            )
        return set(self._all_terms)

    @property
    @abstractmethod
    def tree_item_string(self) -> str:
        """
        The string representation of the node on the single line that becomes
        the `item_str` in its `CollectionTreeForm`.
        """

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.preceding_context.to_tree_form()
        predecessor.has_successor = True
        tree_form: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            predecessor.depth,
            predecessor=predecessor,
        )
        for child in self.children:
            child_tree: CollectionTreeForm = child.to_tree_form()
            tree_form.has_children = True
            tree_form.nested_trees.append(child_tree)
        return tree_form

    def equals(self, other: "ChildOperator") -> bool:
        return (
            super().equals(other) and self.preceding_context == other.preceding_context
        )
