"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from pydough.metadata.errors import (
    verify_typ_in_json,
    verify_json_string_mapping_in_json,
    verify_typ_in_object,
    verify_json_string_mapping_in_object,
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
        self.collection: CollectionMetadata = None
        self.primary_property: ReversiblePropertyMetadata = None
        self.secondary_collection: CollectionMetadata = None
        self.secondary_property: ReversiblePropertyMetadata = None

    def components(self) -> Tuple:
        return super().components() + (
            self.primary_property_name,
            self.secondary_property_name,
            self.inherited_properties_mapping,
        )

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        error_name = f"compound relationship property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_typ_in_json(property_json, "primary_property", str, error_name)
        verify_typ_in_json(property_json, "secondary_property", str, error_name)
        verify_typ_in_json(property_json, "reverse_relationship_name", str, error_name)
        verify_typ_in_json(property_json, "singular", bool, error_name)
        verify_typ_in_json(property_json, "no_collisions", bool, error_name)
        verify_json_string_mapping_in_json(
            property_json, "inherited_properties", error_name
        )

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        from pydough.metadata.collections import CollectionMetadata

        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_typ_in_object(self, "primary_property_name", str, error_name)
        verify_typ_in_object(self, "secondary_property_name", str, error_name)
        verify_typ_in_object(self, "singular", bool, bool)
        verify_typ_in_object(self, "no_collisions", str, error_name)
        verify_json_string_mapping_in_object(
            self, "inherited_properties_mapping", error_name
        )
        verify_typ_in_object(
            self, "primary_property", ReversiblePropertyMetadata, error_name
        )
        verify_typ_in_object(
            self, "secondary_collection", CollectionMetadata, error_name
        )
        verify_typ_in_object(
            self, "secondary_property", ReversiblePropertyMetadata, error_name
        )
        verify_typ_in_object(
            self, "reverse_subcollection", CollectionMetadata, error_name
        )
        verify_typ_in_object(self, "reverse_relationship_name", str, error_name)
        verify_typ_in_object(self, "reverse_property", PropertyMetadata, error_name)

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        from pydough.metadata.collections import CollectionMetadata

        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.primary_property_name = property_json["primary_property"]
        self.secondary_property_name = property_json["secondary_property"]
        self.inherited_properties_mapping = property_json["inherited_properties"]
        self.singular = property_json["singular"]
        self.no_collisions = property_json["no_collisions"]
        self.reverse_relationship_name = property_json["reverse_relationship_name"]

        verify_typ_in_json(
            graph_json, self.collection_name, dict, f"graph {repr(self.graph_name)}"
        )
        self.collection: CollectionMetadata = collections[self.collection_name]
        verify_typ_in_json(
            self.collection.properties,
            self.primary_property_name,
            ReversiblePropertyMetadata,
            f"Collection {repr(self.collection.name)} in graph {repr(self.graph_name)}",
        )
        self.primary_property = self.collection.properties[self.primary_property_name]
        verify_typ_in_object(
            self.primary_property,
            "reverse_collection",
            CollectionMetadata,
            f"Property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)}",
        )
        self.secondary_collection = self.primary_property.reverse_collection
        verify_typ_in_json(
            self.secondary_collection.properties,
            self.secondary_property_name,
            ReversiblePropertyMetadata,
            f"Collection {repr(self.secondary_collection.name)} (accessed from property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)})",
        )
        self.secondary_property = self.secondary_collection.properties[
            self.secondary_property_name
        ]
        verify_typ_in_object(
            self.secondary_property,
            "reverse_collection",
            CollectionMetadata,
            f"Property {self.secondary_property_name} of collection {repr(self.secondary_collection.name)} (accessed from property {self.primary_property_name} of collection {repr(self.collection.name)} in graph {repr(self.graph_name)})",
        )
        self.reverse_collection = self.secondary_property.reverse_collection

        self.build_reverse_relationship()

    def build_reverse_relationship(self) -> ReversiblePropertyMetadata:
        raise NotImplementedError
