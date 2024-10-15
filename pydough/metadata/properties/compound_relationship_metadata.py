"""
TODO: add file-level docstring
"""

from typing import Dict
from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    verify_object_has_property_with_type,
    verify_object_has_property_matching,
    is_string_string_mapping,
)


class CompoundRelationshipMetadata(ReversiblePropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        graph_name: str,
        collection_name: str,
        name: str,
    ):
        from pydough.metadata.collections import CollectionMetadata

        super().__init__(graph_name, collection_name, name)
        self.primary_property_name: str = None
        self.secondary_property_name: str = None
        self.inherited_properties_mapping: str = None
        self.singular: bool = None
        self.no_collisions: bool = None
        self.inherited_properties: Dict[str, PropertyMetadata] = None
        self.collection: CollectionMetadata = None
        self.primary_property: ReversiblePropertyMetadata = None
        self.secondary_collection: CollectionMetadata = None
        self.secondary_property: ReversiblePropertyMetadata = None

    def create_error_name(name: str, collection_error_name: str):
        return f"compound property {name!r} of {collection_error_name}"

    def components(self) -> tuple:
        return super().components() + (
            self.primary_property_name,
            self.secondary_property_name,
            self.inherited_properties_mapping,
        )

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        error_name = f"compound relationship property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_json_has_property_with_type(
            property_json, "primary_property", str, error_name
        )
        verify_json_has_property_with_type(
            property_json, "secondary_property", str, error_name
        )
        verify_json_has_property_with_type(
            property_json, "reverse_relationship_name", str, error_name
        )
        verify_json_has_property_with_type(property_json, "singular", bool, error_name)
        verify_json_has_property_with_type(
            property_json, "no_collisions", bool, error_name
        )
        verify_json_has_property_matching(
            property_json,
            "inherited_properties",
            lambda x: is_string_string_mapping(x, True),
            error_name,
            "JSON object of strings",
        )

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        from pydough.metadata.collections import CollectionMetadata

        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_object_has_property_with_type(
            self, "primary_property_name", str, error_name
        )
        verify_object_has_property_with_type(
            self, "secondary_property_name", str, error_name
        )
        verify_object_has_property_with_type(self, "singular", bool, error_name)
        verify_object_has_property_with_type(self, "no_collisions", bool, error_name)

        verify_object_has_property_matching(
            self,
            "inherited_properties",
            lambda x: is_string_string_mapping(x, True),
            error_name,
            "JSON object of strings",
        )
        verify_object_has_property_with_type(
            self, "primary_property", ReversiblePropertyMetadata, error_name
        )
        verify_object_has_property_with_type(
            self, "secondary_collection", CollectionMetadata, error_name
        )
        verify_object_has_property_with_type(
            self, "secondary_property", ReversiblePropertyMetadata, error_name
        )
        verify_object_has_property_with_type(
            self, "reverse_relationship_name", str, error_name
        )
        verify_object_has_property_with_type(
            self, "reverse_property", PropertyMetadata, error_name
        )

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        from pydough.metadata.collections import CollectionMetadata

        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.primary_property_name = property_json["primary_property"]
        self.secondary_property_name = property_json["secondary_property"]
        self.inherited_properties_mapping = property_json["inherited_properties"]
        self.singular = property_json["singular"]
        self.no_collisions = property_json["no_collisions"]
        self.reverse_relationship_name = property_json["reverse_relationship_name"]

        verify_json_has_property_with_type(
            graph_json, self.collection_name, dict, f"graph {repr(self.graph_name)}"
        )
        self.collection: CollectionMetadata = collections[self.collection_name]
        verify_json_has_property_with_type(
            self.collection.properties,
            self.primary_property_name,
            ReversiblePropertyMetadata,
            f"Collection {repr(self.collection.name)} in graph {repr(self.graph_name)}",
        )
        self.primary_property = self.collection.properties[self.primary_property_name]
        verify_object_has_property_with_type(
            self.primary_property,
            "reverse_collection",
            CollectionMetadata,
            f"Property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)}",
        )
        self.secondary_collection = self.primary_property.reverse_collection
        verify_json_has_property_with_type(
            self.secondary_collection.properties,
            self.secondary_property_name,
            ReversiblePropertyMetadata,
            f"Collection {repr(self.secondary_collection.name)} (accessed from property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)})",
        )
        self.secondary_property = self.secondary_collection.properties[
            self.secondary_property_name
        ]
        verify_object_has_property_with_type(
            self.secondary_property,
            "reverse_collection",
            CollectionMetadata,
            f"Property {self.secondary_property_name} of collection {repr(self.secondary_collection.name)} (accessed from property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)})",
        )

        self.inherited_properties = {}
        inherited_json = property_json["inherited_properties"]
        for alias_name in inherited_json:
            property_name = inherited_json[alias_name]
            verify_json_has_property_with_type(
                self.secondary_collection.properties,
                property_name,
                PropertyMetadata,
                f"collection {self.secondary_collection.name} in graph {self.graph_name}",
            )
            self.inherited_properties[alias_name] = property_name

        self.reverse_collection = self.secondary_property.reverse_collection

        self.build_reverse_relationship()

    def build_reverse_relationship(self) -> ReversiblePropertyMetadata:
        reverse = CompoundRelationshipMetadata(
            self.graph_name,
            self.reverse_collection.name,
            self.reverse_relationship_name,
        )
        reverse.singular = self.no_collisions
        reverse.no_collisions = self.singular
        reverse.reverse_relationship_name = self.name
        reverse.inherited_properties_mapping = self.inherited_properties_mapping
        reverse.inherited_properties = self.inherited_properties

        reverse.primary_property = self.secondary_property.reverse_property
        reverse.secondary_property = self.primary_property.reverse_property

        reverse.primary_property_name = reverse.primary_property.name
        reverse.secondary_property_name = reverse.secondary_property.name
        reverse.secondary_collection = self.secondary_collection

        reverse.collection = self.reverse_collection
        reverse.reverse_collection = self.collection

        reverse.reverse_property = self
        self.reverse_property = reverse
