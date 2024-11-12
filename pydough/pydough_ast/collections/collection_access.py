"""
TODO: add file-level docstring
"""

__all__ = ["CollectionAccess"]


from abc import abstractmethod
from functools import cache

from pydough.metadata import (
    CollectionMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    TableColumnMetadata,
)
from pydough.metadata.properties import SubcollectionRelationshipMetadata
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.pydough_ast.expressions import ColumnProperty

from .collection_ast import PyDoughCollectionAST


class CollectionAccess(PyDoughCollectionAST):
    """
    The AST node implementation class representing a table collection accessed
    either directly or as a subcollection of another collection.
    """

    def __init__(
        self,
        collection: CollectionMetadata,
        ancestor: PyDoughCollectionAST,
        predecessor: PyDoughCollectionAST | None = None,
    ):
        self._collection: CollectionMetadata = collection
        self._ancestor: PyDoughCollectionAST = ancestor
        self._predecessor: PyDoughCollectionAST | None = predecessor
        self._all_property_names: set[str] = set()
        self._calc_property_names: set[str] = set()
        self._calc_property_order: dict[str, int] = {}
        for property_name in sorted(
            collection.get_property_names(),
            key=lambda name: collection.definition_order[name],
        ):
            self._all_property_names.add(property_name)
            property = collection.get_property(property_name)
            assert isinstance(property, PropertyMetadata)
            if not property.is_subcollection:
                self._calc_property_names.add(property_name)
                self._calc_property_order[property_name] = len(
                    self._calc_property_order
                )

    @abstractmethod
    def clone_with_parent(
        self, new_ancestor: PyDoughCollectionAST
    ) -> "CollectionAccess":
        """
        Copies `self` but with a new ancestor node that presumably has the
        original ancestor in its predecessor chain.

        Args:
            `new_ancestor`: the node to use as the new parent of the clone.

        Returns:
            The cloned version of `self`.
        """

    @property
    def collection(self) -> CollectionMetadata:
        """
        The metadata for the table that is being referenced by the collection
        node.
        """
        return self._collection

    @property
    def ancestor_context(self) -> PyDoughCollectionAST:
        return self._ancestor

    @property
    def preceding_context(self) -> PyDoughCollectionAST | None:
        return self._predecessor

    @property
    def calc_terms(self) -> set[str]:
        return self._calc_property_names

    @property
    def all_terms(self) -> set[str]:
        return self._all_property_names

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_terms:
            raise PyDoughASTException(f"Unrecognized term of {self!r}: {expr_name!r}")
        return self._calc_property_order[expr_name]

    @cache
    def get_term(self, term_name: str) -> PyDoughAST:
        from .compound_sub_collection import CompoundSubCollection
        from .sub_collection import SubCollection

        if term_name not in self.all_terms:
            raise PyDoughASTException(
                f"Unrecognized term of {self.collection.error_name}: {term_name!r}"
            )

        property = self.collection.get_property(term_name)
        assert isinstance(property, PropertyMetadata)
        if isinstance(property, CompoundRelationshipMetadata):
            return CompoundSubCollection(property, self)
        elif isinstance(property, SubcollectionRelationshipMetadata):
            return SubCollection(property, self)
        elif isinstance(property, TableColumnMetadata):
            return ColumnProperty(property)
        else:
            raise PyDoughASTException(
                f"Unsupported property type for collection access: {property.__class__.name}"
            )

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, CollectionAccess) and self.collection == other.collection
        )
