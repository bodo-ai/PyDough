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
    TODO: add class docstring
    """

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
        self.table_path: str = table_path
        self.unique_properties: List[Union[str, List[str]]] = unique_properties
        verify_has_type(table_path, str, f"Property 'table_path' of {self.error_name}")
        verify_matches_predicate(
            unique_properties,
            is_list_of_strings_or_string_lists,
            f"Property 'unique_properties' of {self.error_name}",
            "non-empty list of strings or non-empty lists of strings",
        )

    @staticmethod
    def create_error_name(name, graph_error_name):
        return f"simple table collection {name!r} in {graph_error_name}"

    @property
    def components(self) -> tuple:
        return super().components + (self.table_path, self.unique_properties)

    def verify_allows_property(
        self, property: PropertyMetadata, inherited: bool
    ) -> None:
        super().verify_allows_property(property, inherited)
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
        # Invoke the more generic checks
        CollectionMetadata.verify_json_metadata(graph, collection_name, collection_json)
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
        SimpleTableMetadata.verify_json_metadata(
            graph, collection_name, collection_json
        )
        table_path = collection_json["table_path"]
        unique_properties = collection_json["unique_properties"]
        new_collection = SimpleTableMetadata(
            collection_name, graph, table_path, unique_properties
        )
        graph.add_collection(new_collection)
