"""
TODO: add file-level docstring
"""

from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
)
from pydough.metadata.collections import CollectionMetadata


class CartesianProductMetadata(ReversiblePropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
    ):
        super().__init__(name, collection, other_collection, False, False)

    def create_error_name(name: str, collection_error_name: str):
        return f"cartesian property {name!r} of {collection_error_name}"

    @property
    def components(self) -> tuple:
        return super().components

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        error_name = CartesianProductMetadata.create_error_name(
            property_name, collection.error_name
        )
        verify_json_has_property_with_type(
            property_json, "other_collection_name", str, error_name
        )
        verify_json_has_property_with_type(
            property_json, "reverse_relationship_name", str, error_name
        )

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        other_collection_name = property_json["other_collection_name"]
        verify_json_has_property_with_type(
            collection.graph.collections,
            other_collection_name,
            CollectionMetadata,
            collection.graph.error_name,
        )
        other_collection: CollectionMetadata = collection.graph.collections[
            other_collection_name
        ]
        reverse_name = property_json["reverse_relationship_name"]

        property = CartesianProductMetadata(
            property_name, reverse_name, collection, other_collection
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

    def build_reverse_relationship(self) -> None:
        raise NotImplementedError
        # reverse = CartesianProductMetadata(
        #     self.graph_name, self.other_collection_name, self.reverse_relationship_name
        # )
        # reverse.reverse_relationship_name = self.name
        # reverse.other_collection_name = self.collection_name
        # reverse.collection = self.reverse_collection
        # reverse.reverse_collection = self.collection
        # reverse.reverse_property = self
        # self.reverse_property = reverse
