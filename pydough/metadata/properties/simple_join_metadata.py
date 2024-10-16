"""
TODO: add file-level docstring
"""

from .reversible_property_metadata import ReversiblePropertyMetadata
from typing import List, Dict
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    verify_matches_predicate,
    is_string_string_list_mapping,
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
)


class SimpleJoinMetadata(ReversiblePropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
        keys: Dict[str, List[str]],
    ):
        super().__init__(
            name, reverse_name, collection, other_collection, singular, no_collisions
        )
        verify_matches_predicate(
            keys,
            is_string_string_list_mapping,
            self.error_name,
            "non-empty JSON object containing non-empty lists of strings",
        )
        self.keys = keys

    @property
    def components(self) -> tuple:
        return super().components + (self.keys,)

    def create_error_name(name: str, collection_error_name: str):
        return f"simple join property {name!r} of {collection_error_name}"

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        error_name = SimpleJoinMetadata.create_error_name(
            property_name, collection.error_name
        )
        verify_json_has_property_with_type(
            property_json, "other_collection_name", str, error_name
        )
        verify_json_has_property_with_type(property_json, "singular", bool, error_name)
        verify_json_has_property_with_type(
            property_json, "no_collisions", bool, error_name
        )
        verify_json_has_property_with_type(
            property_json, "reverse_relationship_name", str, error_name
        )
        verify_json_has_property_matching(
            property_json,
            "keys",
            is_string_string_list_mapping,
            error_name,
            "non-empty JSON object containing non-empty lists of strings",
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
        singular = property_json["singular"]
        no_collisions = property_json["no_collisions"]
        keys = property_json["keys"]
        reverse_name = property_json["reverse_relationship_name"]

        property = SimpleJoinMetadata(
            property_name,
            reverse_name,
            collection,
            other_collection,
            singular,
            no_collisions,
            keys,
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

    def build_reverse_relationship(self) -> None:
        reverse_keys = {}
        for key in self.keys:
            for other_key in self.keys[key]:
                if other_key not in reverse_keys:
                    reverse_keys[other_key] = []
                reverse_keys[other_key].append(key)
        reverse = SimpleJoinMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
            self.no_collisions,
            self.singular,
            reverse_keys,
        )
        self.reverse_property = reverse
        reverse.reverse_property = self
