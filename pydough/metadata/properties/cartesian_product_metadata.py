"""
Definition of PyDough metadata for a property that connects two collections via
a cartesian product of their records.
"""

__all__ = ["CartesianProductMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import HasPropertyWith, HasType, NoExtraKeys, is_string

from .property_metadata import PropertyMetadata
from .reversible_property_metadata import ReversiblePropertyMetadata


class CartesianProductMetadata(ReversiblePropertyMetadata):
    """
    Concrete metadata implementation for a PyDough property representing a
    cartesian product between a collection and its subcollection.
    """

    # Set of names of fields that can be included in the JSON object
    # describing a cartesian product property.
    allowed_fields: set[str] = PropertyMetadata.allowed_fields | {
        "other_collection_name",
        "reverse_relationship_name",
    }

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
    ):
        super().__init__(name, reverse_name, collection, other_collection, False, False)

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"cartesian property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        return super().components

    @staticmethod
    def verify_json_metadata(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Verifies that the JSON describing the metadata for a property within
        a collection is well-formed to create a new CartesianProductMetadata
        instance. Should be dispatched from
        PropertyMetadata.verify_json_metadata which implements more generic
        checks.

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
        error_name: str = CartesianProductMetadata.create_error_name(
            property_name, collection.error_name
        )

        # Verify that the JSON has the required `other_collection_name` and
        # `reverse_relationship_name` fields, without anything extra.
        HasPropertyWith("other_collection_name", is_string).verify(
            property_json, error_name
        )
        HasPropertyWith("reverse_relationship_name", is_string).verify(
            property_json, error_name
        )
        NoExtraKeys(CartesianProductMetadata.allowed_fields).verify(
            property_json, error_name
        )

    @staticmethod
    def parse_from_json(
        collection: CollectionMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure dispatched from PropertyMetadata.parse_from_json to handle
        the parsing for cartesian product properties.

        Args:
            `collection`: the metadata for the PyDough collection that the
            property would be inserted into.
            `property_name`: the name of the property that would be inserted.
            `property_json`: the JSON object that would be parsed to create
            the new table column property.

        Raises:
            `PyDoughMetadataException`: if the JSON for the property is
            malformed.
        """
        # Extract the other collection's name and the reverse relationship's
        # name from the JSON, then fetch the other collection from the graph's
        # collections. Assumes the other collection has already been defined
        # and added to the graph.
        other_collection_name = property_json["other_collection_name"]
        reverse_name = property_json["reverse_relationship_name"]
        HasPropertyWith(other_collection_name, HasType(CollectionMetadata)).verify(
            collection.graph.collections, collection.graph.error_name
        )
        other_collection = collection.graph.collections[other_collection_name]
        assert isinstance(other_collection, CollectionMetadata)

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property: CartesianProductMetadata = CartesianProductMetadata(
            property_name, reverse_name, collection, other_collection
        )
        property.build_reverse_relationship()
        collection.add_property(property)
        other_collection.add_property(property.reverse_property)

    def build_reverse_relationship(self) -> None:
        # Construct the reverse relationship by flipping the forward & reverse
        # names, the source / target collections, and the plural properties.
        # Then fill the `reverse_property` fields with one another.
        reverse = CartesianProductMetadata(
            self.reverse_name,
            self.name,
            self.other_collection,
            self.collection,
        )
        self._reverse_property = reverse
        reverse._reverse_property = self
