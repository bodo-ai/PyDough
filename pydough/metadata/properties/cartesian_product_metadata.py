"""
Definition of PyDough metadata for a property that connects two collections via
a cartesian product of their records.
"""

__all__ = ["CartesianProductMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    HasPropertyWith,
    NoExtraKeys,
    extract_string,
    is_string,
)
from pydough.metadata.graphs import GraphMetadata

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
        "parent collection",
        "child collection",
    }

    def __init__(
        self,
        name: str,
        parent_collection: CollectionMetadata,
        child_collection: CollectionMetadata,
    ):
        super().__init__(name, parent_collection, child_collection, False)

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
        graph: GraphMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure to generate a new CartesianProductMetadata instance from the
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

        # Extract the child collection from the graph.
        child_collection_name: str = extract_string(
            property_json,
            "child collection",
            f"metadata for property {property_name!r} within {graph.error_name}",
        )
        child_collection = graph.get_collection(child_collection_name)
        assert isinstance(child_collection, CollectionMetadata)

        # Build the new property and add it to the parent collection.
        property: CartesianProductMetadata = CartesianProductMetadata(
            property_name, parent_collection, child_collection
        )
        parent_collection.add_property(property)

    def build_reverse_relationship(
        self, name: str, is_singular: bool
    ) -> ReversiblePropertyMetadata:
        return CartesianProductMetadata(name, self.child_collection, self.collection)
