"""
TODO: add file-level docstring
"""

from typing import List, Union
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    is_list_of_strings_or_string_lists,
    PyDoughMetadataException,
    verify_no_extra_keys_in_json,
    verify_has_type,
    verify_matches_predicate,
)
from pydough.metadata.graphs import GraphMetadata
from . import CollectionMetadata
from pydough.metadata.properties import (
    PropertyMetadata,
    TableColumnMetadata,
    SimpleJoinMetadata,
    CompoundRelationshipMetadata,
    CartesianProductMetadata,
    InheritedPropertyMetadata,
)


class SimpleTableMetadata(CollectionMetadata):
    """
    Concrete metadata implementation for a PyDough collection representing a
    relational table where the properties are columns to the table, or subsets
    of other such tables created from joins.
    """

    # List of names of of properties that can be included in the JSON
    # object describing a simple table collection.
    allowed_properties = CollectionMetadata.allowed_properties + [
        "table_path",
        "unique_properties",
    ]

    def __init__(
        self,
        name: str,
        graph,
        table_path: str,
        unique_properties: List[Union[str, List[str]]],
    ):
        super().__init__(name, graph)
        self._table_path: str = table_path
        self._unique_properties: List[Union[str, List[str]]] = unique_properties
        verify_has_type(table_path, str, f"Property 'table_path' of {self.error_name}")
        verify_matches_predicate(
            unique_properties,
            is_list_of_strings_or_string_lists,
            f"Property 'unique_properties' of {self.error_name}",
            "non-empty list of strings or non-empty lists of strings",
        )

    @property
    def table_path(self) -> str:
        """
        The path used to identify the table within whatever data storage
        mechanism is being used.
        """
        return self._table_path

    @property
    def unique_properties(self) -> List[Union[str, List[str]]]:
        """
        The list of all names of properties of the collection that are
        guaranteed to be unique within the collection. Entries that are a
        string represent a single column being completely unique, while entries
        that are a list of strings indicate that each combination of those
        properties is unique.
        """
        return self._unique_properties

    @staticmethod
    def create_error_name(name, graph_error_name):
        return f"simple table collection {name!r} in {graph_error_name}"

    @property
    def components(self) -> tuple:
        return super().components + (self.table_path, self.unique_properties)

    def verify_allows_property(
        self, property: PropertyMetadata, inherited: bool
    ) -> None:
        """
        Verifies that a property is safe to add to the collection.

        Args:
            `property`: the metadata for a PyDough property that is being
            added to the collection.
            `inherited`: True if verifying a property being inserted as an
            inherited property, False otherwise.

        Raises:
            `PyDoughMetadataException`: if `property` is not a valid property
            to insert into the collection.
        """
        # Invoke the more generic checks.
        super().verify_allows_property(property, inherited)

        # Ensure taht the property is one of the supported types for this
        # type of collection.
        match property:
            case (
                TableColumnMetadata()
                | CartesianProductMetadata()
                | SimpleJoinMetadata()
                | CompoundRelationshipMetadata()
                | InheritedPropertyMetadata()
            ):
                pass
            case _:
                raise PyDoughMetadataException(
                    f"Simple table collections does not allow inserting {property.error_name}"
                )

    def verify_json_metadata(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Verifies that a JSON object contains well formed data to create a new simple
        table collection.

        Args:
            `graph`: the metadata for the graph that the collection would
            be added to.
            `collection_name`: the name of the collection that would be added
            to the graph.
            `collection_json`: the JSON object that is being verified to ensure
            it represents a valid collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        # Create the string used to identify the collection in error messages.
        error_name = SimpleTableMetadata.create_error_name(
            collection_name, graph.error_name
        )

        # Check that the JSON data contains the required properties
        # `table_path` and `unique_properties`, without any extra properties.
        verify_json_has_property_with_type(
            collection_json, "table_path", str, error_name
        )
        verify_json_has_property_matching(
            collection_json,
            "unique_properties",
            is_list_of_strings_or_string_lists,
            error_name,
            "list non-empty of strings or non-empty lists of strings",
        )
        verify_no_extra_keys_in_json(
            collection_json, SimpleTableMetadata.allowed_properties, error_name
        )

    def parse_from_json(
        graph: GraphMetadata, collection_name: str, collection_json: dict
    ) -> None:
        """
        Parses a JSON object into the metadata for a simple table collection
        and inserts it into the graph.

        Args:
            `graph`: the metadata for the graph that the collection will be
            added to.
            `collection_name`: the name of the collection that will be added
            to the graph.
            `collection_json`: the JSON object that is being parsed to create
            the new collection.

        Raises:
            `PyDoughMetadataException`: if the JSON does not meet the necessary
            structure properties.
        """
        # Verify that the JSON is well structured.
        SimpleTableMetadata.verify_json_metadata(
            graph, collection_name, collection_json
        )

        # Extract the relevant properties from the JSON to build the new
        # collection, then add it to the graph.
        table_path = collection_json["table_path"]
        unique_properties = collection_json["unique_properties"]
        new_collection = SimpleTableMetadata(
            collection_name, graph, table_path, unique_properties
        )
        graph.add_collection(new_collection)
