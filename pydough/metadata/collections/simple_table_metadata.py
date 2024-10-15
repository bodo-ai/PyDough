"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple, List, Union
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    verify_json_has_property_matching,
    verify_object_has_property_with_type,
    verify_object_has_property_matching,
    is_list_of_strings_or_string_lists,
    PyDoughMetadataException,
    verify_no_extra_keys_in_json,
)
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

    def __init__(self, graph_name: str, name: str):
        super().__init__(graph_name, name)
        self.table_path: str = None
        self.unique_properties: List[Union[str, List[str]]] = None

    def components(self) -> Tuple:
        return super().components() + (self.table_path, self.unique_properties)

    def verify_is_property_valid_for_collection(
        self, property: PropertyMetadata
    ) -> None:
        match property:
            case (
                TableColumnMetadata()
                | SimpleJoinMetadata()
                | CompoundRelationshipMetadata()
                | CartesianProductMetadata()
                | InheritedPropertyMetadata()
            ):
                property.verify_ready_to_add(self)
            case _:
                raise PyDoughMetadataException(
                    f"Invalid property type for SimpleTableMetadata: {property.__class__.__name__}"
                )

    def verify_json_metadata(
        graph_name: str, collection_name: str, collection_json: Dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        error_name = f"simple table collection {repr(collection_name)} in graph {repr(graph_name)}"

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

    def parse_from_json(self, graph_json: dict) -> None:
        verify_json_has_property_with_type(
            graph_json, self.name, dict, f"graph {repr(self.graph_name)}", "JSON object"
        )
        collection_json = graph_json[self.name]
        self.table_path = collection_json["table_path"]
        self.unique_properties = collection_json["unique_properties"]

    def verify_complete(self, collections: Dict[str, "CollectionMetadata"]) -> None:
        super().verify_complete(collections)
        error_name = f"simple table collection {repr(self.name)}"
        verify_object_has_property_with_type(self, "table_path", str, error_name)
        verify_object_has_property_matching(
            self,
            "unique_properties",
            is_list_of_strings_or_string_lists,
            error_name,
            "list non-empty of strings or non-empty lists of strings",
        )
