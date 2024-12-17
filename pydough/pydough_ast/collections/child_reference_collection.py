"""
Definition of PyDough AST collection type for a reference to a child collection
of a child operator, e.g. `nations` in `regions(n_nations=COUNT(nations))`.
"""

__all__ = ["ChildReferenceCollection"]


from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.expressions.collation_expression import CollationExpression

from .child_access import ChildAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class ChildReferenceCollection(ChildAccess):
    """
    The AST node implementation class representing a reference to a collection
    term in a child collection of a CALC or other child operator.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionAST,
        collection: PyDoughCollectionAST,
        child_idx: int,
    ):
        self._collection: PyDoughCollectionAST = collection
        self._child_idx: int = child_idx
        super().__init__(ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> ChildAccess:
        return ChildReferenceCollection(new_ancestor, self.collection, self.child_idx)

    @property
    def collection(self) -> PyDoughCollectionAST:
        """
        The collection that the ChildReferenceCollection collection comes from.
        """
        return self._collection

    @property
    def child_idx(self) -> int:
        """
        The integer index of the child from the CALC that the
        ChildReferenceCollection refers to.
        """
        return self._child_idx

    @property
    def key(self) -> str:
        return self.standalone_string

    @property
    def calc_terms(self) -> set[str]:
        return self.collection.calc_terms

    @property
    def all_terms(self) -> set[str]:
        return self.collection.all_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.collection.ordering

    @cache
    def is_singular(self, context: PyDoughCollectionAST) -> bool:
        # A child reference collection is singular with regards to a context
        # if and only if the collection it refers to is singular with regard
        # to that context.
        return self.collection.is_singular(context)

    def get_expression_position(self, expr_name: str) -> int:
        return self.collection.get_expression_position(expr_name)

    def get_term(self, term_name: str) -> PyDoughAST:
        return self.collection.get_term(term_name)

    @property
    def standalone_string(self) -> str:
        return f"${self.child_idx+1}"

    def to_string(self, tree_form: bool = False) -> str:
        if tree_form:
            return self.standalone_string
        else:
            return self.collection.to_string()

    @property
    def tree_item_string(self) -> str:
        return self.standalone_string

    def to_tree_form_isolated(self, is_last: bool) -> CollectionTreeForm:
        raise NotImplementedError

    def to_tree_form(self, is_last: bool) -> CollectionTreeForm:
        raise NotImplementedError

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, ChildReferenceCollection)
            and self.collection == other.collection
        )
