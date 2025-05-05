"""
Definition of PyDough metadata for properties that connect two collections by
joining them on certain key columns.
"""

__all__ = ["SimpleJoinMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    NoExtraKeys,
    PyDoughMetadataException,
    extract_array,
    extract_bool,
    extract_object,
    extract_string,
    simple_join_keys_predicate,
)
from pydough.metadata.graphs import GraphMetadata

from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata


class SimpleJoinMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    join between a collection and its subcollection based on equi-join keys.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a simple join property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "parent collection",
        "child collection",
        "singular",
        "keys",
        "always matches",
    }

    def __init__(
        self,
        name: str,
        parent_collection: CollectionMetadata,
        child_collection: CollectionMetadata,
        singular: bool,
        always_matches: bool,
        keys: dict[str, list[str]],
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        super().__init__(
            name,
            parent_collection,
            child_collection,
            singular,
            always_matches,
            description,
            synonyms,
            extra_semantic_info,
        )
        simple_join_keys_predicate.verify(keys, self.error_name)
        self._keys: dict[str, list[str]] = keys
        self._join_pairs: list[tuple[PropertyMetadata, PropertyMetadata]] = []
        # Build the join pairs list by transforming the dictionary of property
        # names from keys into the actual properties of the source/target
        # collection.
        for property_name, matching_property_names in keys.items():
            source_property = self.collection.get_property(property_name)
            assert isinstance(source_property, PropertyMetadata)
            if source_property.is_subcollection:
                raise PyDoughMetadataException(
                    f"{self.error_name} cannot use {source_property.error_name} as a join key"
                )
            for matching_property_name in matching_property_names:
                target_property = self.child_collection.get_property(
                    matching_property_name
                )
                assert isinstance(target_property, PropertyMetadata)
                if target_property.is_subcollection:
                    raise PyDoughMetadataException(
                        f"{self.error_name} cannot use {target_property.error_name} as a join key"
                    )
                self._join_pairs.append((source_property, target_property))

    @property
    def keys(self) -> dict[str, list[str]]:
        """
        A dictionary mapping the names of properties in the current collection
        to the names of properties in the other collection that they must be
        equal to in order to identify matches.
        """
        return self._keys

    @property
    def join_pairs(self) -> list[tuple[PropertyMetadata, PropertyMetadata]]:
        """
        A list of pairs of properties from the current collection and other
        collection that must be equal to in order to identify matches.
        """
        return self._join_pairs

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.keys)
        return comp

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"simple join property {name!r} of {collection_error_name}"

    @staticmethod
    def parse_from_json(
        graph: GraphMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure to generate a new SimpleJoinMetadata instance from the
        JSON describing the metadata for a property within a collection.
        Inserts the new property directly into the metadata for one of the
        collections in the graph.

        Args:
            `graph`: the metadata for the entire graph, already containing the
            collection that the property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the parent collection from the graph.
        parent_collection_name: str = extract_string(
            property_json,
            "parent collection",
            f"metadata for property {property_name!r} within {graph.error_name}",
        )
        parent_collection = graph.get_collection(parent_collection_name)
        assert isinstance(parent_collection, CollectionMetadata)
        # Create the string used to identify the property in error messages.
        error_name = SimpleJoinMetadata.create_error_name(
            property_name, parent_collection.error_name
        )

        # Extract the child collection from the graph.
        child_collection_name: str = extract_string(
            property_json,
            "child collection",
            f"metadata for property {property_name!r} within {graph.error_name}",
        )
        child_collection = graph.get_collection(child_collection_name)
        assert isinstance(child_collection, CollectionMetadata)

        # Extract the singular field and the join keys from the JSON.
        singular: bool = extract_bool(
            property_json,
            "singular",
            f"metadata for property {property_name} within {graph.error_name}",
        )
        always_matches: bool = False
        if "always matches" in property_json:
            always_matches = extract_bool(property_json, "always matches", error_name)
        HasPropertyWith("keys", simple_join_keys_predicate).verify(
            property_json, error_name
        )
        keys = property_json["keys"]

        # Extract the optional fields from the JSON object.
        description: str | None = None
        synonyms: list[str] | None = None
        extra_semantic_info: dict | None = None
        if "description" in property_json:
            description = extract_string(property_json, "description", error_name)
        if "synonyms" in property_json:
            synonyms = extract_array(property_json, "synonyms", error_name)
        if "extra semantic info" in property_json:
            extra_semantic_info = extract_object(
                property_json, "extra semantic info", error_name
            )
        NoExtraKeys(SimpleJoinMetadata.allowed_fields).verify(property_json, error_name)

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property: SimpleJoinMetadata = SimpleJoinMetadata(
            property_name,
            parent_collection,
            child_collection,
            singular,
            always_matches,
            keys,
            description,
            synonyms,
            extra_semantic_info,
        )
        parent_collection.add_property(property)

    def build_reverse_relationship(
        self,
        name: str,
        is_singular: bool,
        always_matches: bool,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ) -> ReversiblePropertyMetadata:
        # Invert the keys dictionary, mapping each string that was in any of
        # the lists of self.keys to all of the keys of self.keys that mapped
        # to those lists.
        reverse_keys: dict[str, list[str]] = {}
        for key in self.keys:
            for other_key in self.keys[key]:
                if other_key not in reverse_keys:
                    reverse_keys[other_key] = []
                reverse_keys[other_key].append(key)

        # Construct the reverse relationship
        return SimpleJoinMetadata(
            name,
            self.child_collection,
            self.collection,
            is_singular,
            always_matches,
            reverse_keys,
            description,
            synonyms,
            extra_semantic_info,
        )
