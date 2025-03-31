"""
Base definition of PyDough QDAG collection type for accesses to a subcollection
of the current context.
"""

__all__ = ["CollectionAccess"]


from functools import cache

from pydough.metadata import (
    CollectionMetadata,
    CompoundRelationshipMetadata,
    PropertyMetadata,
    SimpleTableMetadata,
    TableColumnMetadata,
)
from pydough.metadata.properties import SubcollectionRelationshipMetadata
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.qdag.expressions import (
    BackReferenceExpression,
    CollationExpression,
    ColumnProperty,
    Reference,
)

from .child_access import ChildAccess
from .collection_qdag import PyDoughCollectionQDAG


class CollectionAccess(ChildAccess):
    """
    The QDAG node implementation class representing a table collection accessed
    either directly or as a subcollection of another collection.
    """

    def __init__(
        self,
        collection: CollectionMetadata,
        ancestor: PyDoughCollectionQDAG,
    ):
        super().__init__(ancestor)
        self._collection: CollectionMetadata = collection
        self._all_property_names: set[str] = set()
        self._calc_property_names: set[str] = set()
        self._calc_property_order: dict[str, int] = {}
        self._ancestral_mapping: dict[str, int] = {
            name: level + 1 for name, level in ancestor.ancestral_mapping.items()
        }
        self._all_property_names.update(self._ancestral_mapping)
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

    @property
    def collection(self) -> CollectionMetadata:
        """
        The metadata for the table that is being referenced by the collection
        node.
        """
        return self._collection

    @property
    def calc_terms(self) -> set[str]:
        return self._calc_property_names

    @property
    def all_terms(self) -> set[str]:
        return self._all_property_names

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        return self._ancestral_mapping

    @property
    def inherited_downstreamed_terms(self) -> set[str]:
        return self.ancestor_context.inherited_downstreamed_terms

    @property
    def ordering(self) -> list[CollationExpression] | None:
        return None

    @property
    def unique_terms(self) -> list[str]:
        if isinstance(self.collection, SimpleTableMetadata):
            chosen_unique: str | list[str] = self.collection.unique_properties[0]
            return [chosen_unique] if isinstance(chosen_unique, str) else chosen_unique
        else:
            raise NotImplementedError(self.collection.__class__.__name__)

    def get_expression_position(self, expr_name: str) -> int:
        if expr_name not in self.calc_terms:
            raise PyDoughQDAGException(f"Unrecognized term of {self!r}: {expr_name!r}")
        return self._calc_property_order[expr_name]

    @cache
    def get_term(self, term_name: str) -> PyDoughQDAG:
        # Special handling of terms down-streamed from an ancestor CALCULATE
        # clause.
        if term_name in self.ancestral_mapping:
            # Verify that the ancestor name is not also a name in the current
            # context.
            if term_name in self.calc_terms:
                raise PyDoughQDAGException(
                    f"Cannot have term name {term_name!r} used in an ancestor of collection {self!r}"
                )
            # Create a back-reference to the ancestor term.
            return BackReferenceExpression(
                self, term_name, self.ancestral_mapping[term_name]
            )

        if term_name in self.inherited_downstreamed_terms:
            context: PyDoughCollectionQDAG = self
            while term_name not in context.all_terms:
                if context is self:
                    context = self.ancestor_context
                else:
                    assert context.ancestor_context is not None
                    context = context.ancestor_context
            return Reference(context, term_name)

        if term_name not in self.all_terms:
            raise PyDoughQDAGException(
                f"Unrecognized term of {self.collection.error_name}: {term_name!r}"
            )
        return self.get_term_from_property(term_name)

    def get_term_from_property(self, term_name: str) -> PyDoughQDAG:
        """
        Fetch the QDAG node corresponding to a property of the collection.
        """
        from .compound_sub_collection import CompoundSubCollection
        from .sub_collection import SubCollection

        property = self.collection.get_property(term_name)
        assert isinstance(property, PropertyMetadata)
        if isinstance(property, CompoundRelationshipMetadata):
            return CompoundSubCollection(property, self)
        elif isinstance(property, SubcollectionRelationshipMetadata):
            return SubCollection(property, self)
        elif isinstance(property, TableColumnMetadata):
            return ColumnProperty(property)
        else:
            raise PyDoughQDAGException(
                f"Unsupported property type for collection access: {property.__class__.name}"
            )

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, CollectionAccess)
            and super().equals(other)
            and self.collection == other.collection
        )
