"""
TODO: add file-level docstring
"""

__all__ = ["InheritedPropertyMetadata"]

from .property_metadata import PropertyMetadata
from .compound_relationship_metadata import CompoundRelationshipMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import HasType, PyDoughMetadataException


class InheritedPropertyMetadata(PropertyMetadata):
    """
    The implementation class for a property that does not exist directly in a
    collection, but rather is inherited when the collection is accessed via
    a compound relationship.
    """

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        property_inherited_from: CompoundRelationshipMetadata,
        property_to_inherit: PropertyMetadata,
    ):
        super().__init__(name, collection)
        HasType(CompoundRelationshipMetadata).verify(
            property_inherited_from, "property_inherited_from"
        )
        HasType(PropertyMetadata).verify(property_to_inherit, "property_to_inherit")
        self._property_inherited_from: CompoundRelationshipMetadata = (
            property_inherited_from
        )
        self._property_to_inherit: PropertyMetadata = property_to_inherit

    @property
    def property_inherited_from(self) -> CompoundRelationshipMetadata:
        """
        The property that this inherited property is derived from.
        """
        return self._property_inherited_from

    @property
    def property_to_inherit(self) -> CompoundRelationshipMetadata:
        """
        The property that this inherited property allows its collection to
        access.
        """
        return self._property_to_inherit

    @property
    def error_name(self):
        return self.create_error_name(
            self.name,
            self.collection.error_name,
            self.property_inherited_from.error_name,
            self.property_to_inherit.error_name,
        )

    @staticmethod
    def create_error_name(
        name: str,
        collection_error_name: str,
        source_error_name: str,
        property_error_name: str,
    ) -> str:
        return f"inherited property {name!r} of {collection_error_name} (alias of {property_error_name} inherited from {source_error_name})"

    @property
    def path(self) -> str:
        return f"{self.property_inherited_from.path}.{self.name}"

    @property
    def is_plural(self) -> bool:
        return (
            self.property_inherited_from.is_plural or self.property_to_inherit.is_plural
        )

    @property
    def is_reversible(self) -> bool:
        return self.property_to_inherit.is_reversible

    @property
    def is_subcollection(self) -> bool:
        return self.property_to_inherit.is_subcollection

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.property_inherited_from.components)
        comp.append(self.property_to_inherit.components)
        return comp

    def flip_source(self) -> "InheritedPropertyMetadata":
        """
        Returns a copy of self where the source property's direction is
        flipped. Only valid when the source property is a compound
        relationship.
        """
        if not isinstance(self.property_inherited_from, CompoundRelationshipMetadata):
            raise PyDoughMetadataException(f"Cannot flip source of {self.error_name}")
        reverse_property: ReversiblePropertyMetadata = (
            self.property_inherited_from.reverse_property
        )
        return InheritedPropertyMetadata(
            self.name,
            reverse_property.other_collection,
            reverse_property,
            self.property_to_inherit,
        )
