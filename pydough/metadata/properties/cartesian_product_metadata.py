"""
Definition of PyDough metadata for a property that connects two collections via
a cartesian product of their records.
"""

__all__ = ["CartesianProductMetadata"]


from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import (
    NoExtraKeys,
    extract_array,
    extract_bool,
    extract_object,
    extract_string,
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
        "always matches",
    }

    def __init__(
        self,
        name: str,
        parent_collection: CollectionMetadata,
        child_collection: CollectionMetadata,
        always_matches: bool,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        super().__init__(
            name,
            parent_collection,
            child_collection,
            False,
            always_matches,
            description,
            synonyms,
            extra_semantic_info,
        )

    @staticmethod
    def create_error_name(name: str, collection_error_name: str):
        return f"cartesian property {name!r} of {collection_error_name}"

    @property
    def components(self) -> list:
        return super().components

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
        error_name = CartesianProductMetadata.create_error_name(
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
        always_matches: bool = False
        if "always matches" in property_json:
            always_matches = extract_bool(property_json, "always matches", error_name)

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

        NoExtraKeys(CartesianProductMetadata.allowed_fields).verify(
            property_json, error_name
        )

        # Build the new property and add it to the parent collection.
        property: CartesianProductMetadata = CartesianProductMetadata(
            property_name,
            parent_collection,
            child_collection,
            always_matches,
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
        return CartesianProductMetadata(
            name,
            self.child_collection,
            self.collection,
            always_matches,
            description,
            synonyms,
            extra_semantic_info,
        )
