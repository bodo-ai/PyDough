"""
TODO: add file-level docstring
"""

from typing import Dict
from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_object_has_property_with_type,
)


class CartesianProductMetadata(ReversiblePropertyMetadata):
    """
    TODO: add class docstring
    """

    def create_error_name(name: str, collection_error_name: str):
        return f"cartesian property {name!r} of {collection_error_name}"

    def components(self) -> tuple:
        return super().components()

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        error_name = f"cartesian product property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_json_has_property_with_type(
            property_json, "other_collection_name", str, error_name
        )

    def verify_ready_to_add(self, collection) -> None:
        from pydough.metadata.collections import CollectionMetadata

        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_object_has_property_with_type(
            self, "other_collection_name", str, error_name
        )
        verify_object_has_property_with_type(
            self, "collection", CollectionMetadata, error_name
        )
        verify_object_has_property_with_type(
            self, "reverse_collection", CollectionMetadata, error_name
        )
        verify_object_has_property_with_type(
            self, "reverse_relationship_name", str, error_name
        )
        verify_object_has_property_with_type(
            self, "reverse_property", PropertyMetadata, error_name
        )

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.other_collection_name = property_json["other_collection_name"]
        self.reverse_relationship_name = property_json["reverse_relationship_name"]

        verify_json_has_property_with_type(
            graph_json, self.collection_name, dict, f"graph {repr(self.graph_name)}"
        )
        self.collection = collections[self.collection_name]

        verify_json_has_property_with_type(
            graph_json,
            self.other_collection_name,
            dict,
            f"graph {repr(self.graph_name)}",
        )
        self.reverse_collection = collections[self.other_collection_name]

        self.build_reverse_relationship()

    def build_reverse_relationship(self) -> ReversiblePropertyMetadata:
        reverse = CartesianProductMetadata(
            self.graph_name, self.other_collection_name, self.reverse_relationship_name
        )
        reverse.reverse_relationship_name = self.name
        reverse.other_collection_name = self.collection_name
        reverse.collection = self.reverse_collection
        reverse.reverse_collection = self.collection
        reverse.reverse_property = self
        self.reverse_property = reverse
