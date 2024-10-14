"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import (
    verify_typ_in_json,
    verify_json_string_list_mapping_in_json,
    verify_typ_in_object,
    verify_json_string_list_in_object,
)


class SimpleJoinMetadata(PropertyMetadata):
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
        self.other_collection_name: str = None
        self.singular: bool = None
        self.no_collisions: bool = None
        self.keys: Dict[str, str] = None
        self.reverse_relationship_name: str = None
        self.parent_collection: CollectionMetadata = None
        self.sub_collection: CollectionMetadata = None
        self.reverse_relationship: PropertyMetadata = None

    def components(self) -> Tuple:
        return super().components() + (
            self.other_collection_name,
            self.singular,
            self.no_collisions,
            self.keys,
            self.reverse_relationship_name,
        )

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"simple join property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_typ_in_json(property_json, "other_collection_name", str, error_name)
        verify_typ_in_json(property_json, "singular", bool, error_name)
        verify_typ_in_json(property_json, "no_collisions", bool, error_name)
        verify_json_string_list_mapping_in_json(property_json, "keys", error_name)
        verify_typ_in_json(property_json, "reverse_relationship_name", str, error_name)

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_typ_in_object(self, "other_collection_name", str, error_name)
        verify_typ_in_object(self, "singular", bool, error_name)
        verify_typ_in_object(self, "no_collisions", bool, error_name)
        verify_json_string_list_in_object(self, "keys", error_name)
        verify_typ_in_object(self, "reverse_relationship_name", str, error_name)

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.other_collection_name = property_json["other_collection_name"]
        self.singular = property_json["singular"]
        self.no_collisions = property_json["no_collisions"]
        self.keys = property_json["keys"]
        self.reverse_relationship_name = property_json["reverse_relationship_name"]

        verify_typ_in_json(
            graph_json, self.collection_name, dict, f"graph {repr(self.graph_name)}"
        )
        self.parent_collection = graph_json[self.collection_name]

        verify_typ_in_json(
            graph_json,
            self.other_collection_name,
            dict,
            f"graph {repr(self.graph_name)}",
        )
        self.sub_collection = graph_json[self.other_collection_name]

        self.build_reverse_relationship()

    def build_reverse_relationship(self) -> PropertyMetadata:
        """
        TODO: add function doscstring.
        """
        reverse = SimpleJoinMetadata(
            self.graph_name, self.other_collection_name, self.reverse_relationship_name
        )
        reverse.singular = self.no_collisions
        reverse.no_collisions = self.singular
        reverse.reverse_relationship_name = self.name

        reverse_keys = {}
        for key in self.keys:
            for other_key in self.keys[key]:
                if other_key not in reverse_keys:
                    reverse_keys[other_key] = []
                reverse_keys[other_key].append(key)
        reverse.keys = reverse_keys

        reverse.reverse_relationship = self
        self.reverse_relationship = reverse
