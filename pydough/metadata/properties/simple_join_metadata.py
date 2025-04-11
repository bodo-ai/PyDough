"""
Definition of PyDough metadata for properties that connect two collections by
joining them on certain key columns.
"""

__all__ = ["SimpleJoinMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    HasType,
    NoExtraKeys,
    PyDoughMetadataException,
    is_bool,
    is_string,
    simple_join_keys_predicate,
)

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
        "other_collection_name",
        "reverse_relationship_name",
        "singular",
        "no_collisions",
        "keys",
    }

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
        keys: dict[str, list[str]],
    ):
        super().__init__(
            name, reverse_name, collection, other_collection, singular, no_collisions
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
                target_property = self.other_collection.get_property(
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
        HasPropertyWith("other_collection_name", is_string).verify(
            property_json, error_name
        )
        HasPropertyWith("singular", is_bool).verify(property_json, error_name)
        HasPropertyWith("no_collisions", is_bool).verify(property_json, error_name)
        HasPropertyWith("reverse_relationship_name", is_string).verify(
            property_json, error_name
        )
        HasPropertyWith("keys", simple_join_keys_predicate).verify(
            property_json, error_name
        )
        NoExtraKeys(SimpleJoinMetadata.allowed_fields).verify(property_json, error_name)

    @staticmethod
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
        HasPropertyWith(other_collection_name, HasType(CollectionMetadata)).verify(
            collection.graph.collections, collection.graph.error_name
        )
        other_collection = collection.graph.collections[other_collection_name]
        assert isinstance(other_collection, CollectionMetadata)

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
        reverse_keys: dict[str, list[str]] = {}
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
