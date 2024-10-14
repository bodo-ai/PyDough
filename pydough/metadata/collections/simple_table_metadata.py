"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple, List, Union
from pydough.metadata.errors import (
    verify_string_in_json,
    verify_list_of_string_or_strings_in_json,
    verify_no_extra_keys_in_json,
)
from pydough.metadata.collections import CollectionMetadata


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

    def verify_json_metadata(
        graph_name: str, collection_name: str, collection_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"simple table collection {repr(collection_name)} in graph {repr(graph_name)}"

        verify_string_in_json(collection_json, "table_path", error_name)
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
