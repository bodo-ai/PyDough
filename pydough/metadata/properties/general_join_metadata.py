"""
Definition of PyDough metadata for properties that connect two collections by
joining them on certain key columns.
"""

__all__ = ["GeneralJoinMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    HasType,
    NoExtraKeys,
    is_bool,
    is_string,
)

from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata


class GeneralJoinMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    join between a collection and its subcollection based on arbitrary PyDough
    code invoking columns between the two collections via the names `self`
    and `other`.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a simple join property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "other_collection_name",
        "reverse_relationship_name",
        "singular",
        "no_collisions",
        "condition",
    }

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
        condition: str,
        self_name: str,
        other_name: str,
    ):
        super().__init__(
            name, reverse_name, collection, other_collection, singular, no_collisions
        )
        self._condition: str = condition
        self._self_name: str = self_name
        self._other_name: str = other_name

    @property
    def condition(self) -> str:
        """
        The PyDough condition string that will be used to join the two
        collections. The columns from the parent collection are referred to
        via a prefix of `self_name`, and the columns from the child collection
        with `other_name`.
        """
        return self._condition

    @property
    def self_name(self) -> str:
        """
        The name used to refer to columns from the parent collection in the
        condition string.
        """
        return self._self_name

    @property
    def other_name(self) -> str:
        """
        The name used to refer to columns from the child collection in the
        condition string.
        """
        return self._other_name

    @property
    def components(self) -> list:
        comp: list = super().components
        comp.append((self.condition, self.self_name, self.other_name))
        return comp

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"general join property {name!r} of {collection_error_name}"

    @staticmethod
    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new GeneralJoinMetadata instance
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
        error_name = GeneralJoinMetadata.create_error_name(
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
        HasPropertyWith("condition", is_string).verify(property_json, error_name)
        NoExtraKeys(GeneralJoinMetadata.allowed_fields).verify(
            property_json, error_name
        )

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
        condition = property_json["condition"]
        reverse_name = property_json["reverse_relationship_name"]
        HasPropertyWith(other_collection_name, HasType(CollectionMetadata)).verify(
            collection.graph.collections, collection.graph.error_name
        )
        other_collection = collection.graph.collections[other_collection_name]
        assert isinstance(other_collection, CollectionMetadata)

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property: GeneralJoinMetadata = GeneralJoinMetadata(
            property_name,
            reverse_name,
            collection,
            other_collection,
            singular,
            no_collisions,
            condition,
            "self",
            "other",
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

    def build_reverse_relationship(self) -> None:
        # Construct the reverse relationship by flipping the self & other
        # names.
        reverse = GeneralJoinMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
            self.no_collisions,
            self.singular,
            self.condition,
            self.other_name,
            self.self_name,
        )
        self._reverse_property = reverse
        reverse._reverse_property = self
