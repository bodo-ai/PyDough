"""
Definition of PyDough QDAG collection type for a reference to a child collection
of a child operator, e.g. `nations` in `regions(n_nations=COUNT(nations))`.
"""

__all__ = ["ChildReferenceCollection"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.expressions import CollationExpression

from .child_access import ChildAccess
from .collection_qdag import PyDoughCollectionQDAG
from .collection_tree_form import CollectionTreeForm


class ChildReferenceCollection(ChildAccess):
    """
    The QDAG node implementation class representing a reference to a collection
    term in a child collection of a CALCULATE or other child operator.
    """

    def __init__(
        self,
        ancestor: PyDoughCollectionQDAG,
        collection: PyDoughCollectionQDAG,
        child_idx: int,
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._child_idx: int = child_idx
        super().__init__(ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> ChildAccess:
        return ChildReferenceCollection(new_ancestor, self.collection, self.child_idx)

    @property
    def name(self) -> str:
        return self.collection.name

    @property
    def collection(self) -> PyDoughCollectionQDAG:
        """
        The collection that the ChildReferenceCollection collection comes from.
        """
        return self._collection

    @property
    def child_idx(self) -> int:
        """
        The integer index of the child from the CALCULATE that the
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
    def ancestral_mapping(self) -> dict[str, int]:
        return self.collection.ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.collection.inherited_downstreamed_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return self.collection.ordering

    @property
    def unique_terms(self) -> list[str]:
        return self.collection.unique_terms

    @cache
    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # A child reference collection is singular with regards to a context
        # if and only if the collection it refers to is singular with regard
        # to that context.
        return self.collection.is_singular(context)

    def get_expression_position(self, expr_name: str) -> int:
        return self.collection.get_expression_position(expr_name)

    def get_term(self, term_name: str) -> PyDoughQDAG:
        return self.collection.get_term(term_name)

    @property
    def standalone_string(self) -> str:
        return f"${self.child_idx + 1}"

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
            isinstance(other, ChildReferenceCollection)
            and super().equals(other)
            and self.collection == other.collection
        )
