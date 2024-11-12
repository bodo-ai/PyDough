"""
TODO: add file-level docstring
"""

__all__ = ["ChildOperator"]

from collections.abc import MutableSequence
from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import CollationExpression

from .child_access import ChildAccess
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

    @property
    def children(self) -> MutableSequence[PyDoughCollectionAST]:
        """
        The child collections accessible from the operator used to derive
        expressions in terms of a subcollection.
        """
        return self._children

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self._preceding_context.ancestor_context

    @property
    def preceding_context(self) -> PyDoughCollectionAST:
        return self._preceding_context

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.preceding_context.ordering

    def get_expression_position(self, expr_name: str) -> int:
        return self.preceding_context.get_expression_position(expr_name)

    @cache
    def get_term(self, term_name: str) -> PyDoughAST:
        if term_name in self.all_terms:
            term: PyDoughAST = self.preceding_context.get_term(term_name)
            if isinstance(term, ChildAccess):
                term = term.clone_with_parent(self)
            return term
        else:
            raise PyDoughASTException(f"Unrecognized term: {term_name!r}")

    def to_tree_form_isolated(self) -> CollectionTreeForm:
        tree_form: CollectionTreeForm = CollectionTreeForm(
            self.tree_item_string,
            0,
            has_predecessor=True,
        )
        for child in self.children:
            child_tree: CollectionTreeForm = child.to_tree_form()
            tree_form.nested_trees.append(child_tree)
        return tree_form

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.preceding_context.to_tree_form()
        predecessor.has_successor = True
        tree_form: CollectionTreeForm = self.to_tree_form_isolated()
        tree_form.depth = predecessor.depth
        tree_form.predecessor = predecessor
        return tree_form

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ChildOperator)
            and self.preceding_context == other.preceding_context
        )
