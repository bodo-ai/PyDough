"""
TODO: add file-level docstring
"""

from . import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata
from typing import List, Dict, Tuple
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    verify_matches_predicate,
    is_string_string_list_mapping,
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    verify_no_extra_keys_in_json,
)


class SimpleJoinMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    join between a collection and its subcollection based on equi-join keys.
    """

    # List of names of of fields that can be included in the JSON object
    # describing a simple join property.
    allowed_fields: List[str] = PropertyMetadata.allowed_fields + [
        "other_collection_name",
        "reverse_relationship_name",
        "singular",
        "no_collisions",
        "keys",
    ]

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
        self._keys: Dict[str, List[str]] = keys
        self._join_pairs: List[Tuple[PropertyMetadata, PropertyMetadata]] = []
        # Build the join pairs list by transforming the dictionary of property
        # names from keys into the actual properties of the source/target
        # collection.
        for property_name, matching_property_names in keys.items():
            source_property = self.collection.get_property(property_name)
            for matching_property_name in matching_property_names:
                target_property = self.other_collection.get_property(
                    matching_property_name
                )
                self._join_pairs.append((source_property, target_property))

    @property
    def keys(self) -> Dict[str, List[str]]:
        """
        A dictionary mapping the names of properties in the current collection
        to the names of properties in the other collection that they must be
        equal to in order to identify matches.
        """
        return self._keys

    @property
    def join_pairs(self) -> List[Tuple[PropertyMetadata, PropertyMetadata]]:
        """
        A list of pairs of properties from the current collection and other
        collection that must be equal to in order to identify matches.
        """
        return self._join_pairs

    @property
    def components(self) -> tuple:
        return super().components + (self.keys,)

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"simple join property {name!r} of {collection_error_name}"

    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new SimpleJoinMetadata instance
        Should be dispatched from PropertyMetadata.verify_json_metadata which
        implements more generic checks.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Create the string used to identify the property in error messages.
        error_name = SimpleJoinMetadata.create_error_name(
            property_name, collection.error_name
        )

        # Verify that the JSON has the fields `other_collection_name`,
        # `singular`, `no_collisions`, `reverse_relationship_name`,
        # and `keys`, without any extra fields.
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
        verify_no_extra_keys_in_json(
            property_json, SimpleJoinMetadata.allowed_fields, error_name
        )

    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for simple join properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted nto.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the other collection's name, the reverse relationship's name,
        # the joining keys, and the singular/no_collision fields from the JSON,
        # then fetch the other collection from the graph's collections. Assumes
        # the other collection has already been defined and added to the graph.
        other_collection_name = property_json["other_collection_name"]
        singular = property_json["singular"]
        no_collisions = property_json["no_collisions"]
        keys = property_json["keys"]
        reverse_name = property_json["reverse_relationship_name"]
        verify_json_has_property_with_type(
            collection.graph.collections,
            other_collection_name,
            CollectionMetadata,
            collection.graph.error_name,
        )
        other_collection: CollectionMetadata = collection.graph.collections[
            other_collection_name
        ]

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property: SimpleJoinMetadata = SimpleJoinMetadata(
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
        # Invert the keys dictionary, mapping each string that was in any of
        # the lists of self.keys to all of the keys of self.keys that mapped
        # to those lists.
        reverse_keys = {}
        for key in self.keys:
            for other_key in self.keys[key]:
                if other_key not in reverse_keys:
                    reverse_keys[other_key] = []
                reverse_keys[other_key].append(key)

        # Construct the reverse relationship by flipping the forward & reverse
        # names, the source / target collections, and the plural properties.
        # Then fill the `reverse_property` fields with one another.
        reverse = SimpleJoinMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
            self.no_collisions,
            self.singular,
            reverse_keys,
        )
        self._reverse_property = reverse
        reverse._reverse_property = self
