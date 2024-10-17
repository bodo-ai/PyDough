"""
TODO: add file-level docstring
"""

from . import PropertyMetadata, CompoundRelationshipMetadata
from pydough.metadata.errors import PyDoughMetadataException
from pydough.metadata.collections import CollectionMetadata


class InheritedPropertyMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        property_inherited_from: CompoundRelationshipMetadata,
        property_to_inherit: PropertyMetadata,
    ):
        super().__init__(name, collection)
        self.property_inherited_from: CompoundRelationshipMetadata = (
            property_inherited_from
        )
        self.property_to_inherit: PropertyMetadata = property_to_inherit

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
    def components(self) -> tuple:
        return (
            super().components
            + self.property_inherited_from.components
            + self.property_to_inherit.components
        )

    @property
    def source_collection(self) -> CollectionMetadata:
        """
        TODO: add function docstring
        """
        return self.property_inherited_from.primary_property.other_collection

    def parse_from_json(
        self, collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        raise PyDoughMetadataException(
            "Cannot directly construct an instance of InheritedPropertyMetadata from JSON"
        )
