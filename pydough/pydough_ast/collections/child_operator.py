"""
TODO: add file-level docstring
"""

__all__ = ["ChildOperator"]

from abc import abstractmethod
from collections.abc import MutableSequence

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST

from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class ChildOperator(PyDoughCollectionAST):
    """
    Base class for PyDough collection AST nodes that have access to
    child collections, such as CALC or WHERE.
    """

    def __init__(
        self,
        predecessor: PyDoughCollectionAST,
        children: MutableSequence[PyDoughCollectionAST],
    ):
        self._preceding_context: PyDoughCollectionAST = predecessor
        self._children: MutableSequence[PyDoughCollectionAST] = children

        # Evaluated lazy
        self._propagated_properties: dict[str, PyDoughAST] | None = None

    @property
    def children(self) -> MutableSequence[PyDoughCollectionAST]:
        """
        The child collections accessible from the operator used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    @property
    def propagated_properties(self) -> dict[str, PyDoughAST]:
        """
        A mapping of names of properties properties inherited from the
        predecessor to the transformed versions of those properties with
        the current node as their parent.
        """
        if self._propagated_properties is None:
            self._propagated_properties = {}
            for term_name in self._preceding_context.all_terms:
                term: PyDoughAST = self._preceding_context.get_term(term_name)
                if isinstance(term, CollectionAccess):
                    term = term.clone_with_parent(self)
                self._propagated_properties[term_name] = term
        return self._propagated_properties

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self._preceding_context.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionAST:
        return self._preceding_context

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

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ChildOperator)
            and self.preceding_context == other.preceding_context
        )
