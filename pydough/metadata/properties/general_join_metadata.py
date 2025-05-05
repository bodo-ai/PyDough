"""
Definition of PyDough metadata for properties that connect two collections by
joining them on certain key columns.
"""

__all__ = ["GeneralJoinMetadata"]


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
        "parent collection",
        "child collection",
        "singular",
        "condition",
        "always matches",
    }

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        always_matches: bool,
        condition: str,
        self_name: str,
        other_name: str,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        super().__init__(
            name,
            collection,
            other_collection,
            singular,
            always_matches,
            description,
            synonyms,
            extra_semantic_info,
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
    def parse_from_json(
        graph: GraphMetadata, property_name: str, property_json: dict
    ) -> None:
        """
        Procedure to generate a new GeneralJoinMetadata instance from the
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
        error_name = GeneralJoinMetadata.create_error_name(
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

        # Extract  the singular & condition fields from the JSON.
        singular: bool = extract_bool(
            property_json,
            "singular",
            error_name,
        )
        always_matches: bool = False
        if "always matches" in property_json:
            always_matches = extract_bool(property_json, "always matches", error_name)
        condition = extract_string(property_json, "condition", error_name)

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
        NoExtraKeys(GeneralJoinMetadata.allowed_fields).verify(
            property_json, error_name
        )

        # Build the new property, its reverse, then add both
        # to their collection's properties.
        property: GeneralJoinMetadata = GeneralJoinMetadata(
            property_name,
            parent_collection,
            child_collection,
            singular,
            always_matches,
            condition,
            "self",
            "other",
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
        return GeneralJoinMetadata(
            name,
            self.child_collection,
            self.collection,
            is_singular,
            always_matches,
            self.condition,
            self.other_name,
            self.self_name,
            description,
            synonyms,
            extra_semantic_info,
        )
