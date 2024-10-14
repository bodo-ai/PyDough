"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple, List, Union
from pydough.metadata.errors import (
    verify_list_of_string_or_strings_in_json,
    verify_no_extra_keys_in_json,
    verify_typ_in_json,
    PyDoughMetadataException,
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
        TODO: add function doscstring.
        """
        error_name = f"simple table collection {repr(collection_name)} in graph {repr(graph_name)}"

        verify_typ_in_json(collection_json, "table_path", str, error_name)
        verify_list_of_string_or_strings_in_json(
            collection_json, "unique_properties", error_name
        )
        verify_no_extra_keys_in_json(
            collection_json, SimpleTableMetadata.allowed_properties, error_name
        )

    def parse_from_json(self, graph_json: dict) -> None:
        """
        TODO: add function doscstring.
        """
        print(f"PARSING {self}")
