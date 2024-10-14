"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from .property_metadata import PropertyMetadata
from pydough.metadata.errors import verify_string_in_json
from pydough.types import parse_type_from_string


class TableColumnMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        graph_name: str,
        collection_name: str,
        name: str,
    ):
        super().__init__(graph_name, collection_name, name)
        self.column_name = None
        self.data_type = None

    def components(self) -> Tuple:
        return super().components() + (self.column_name, self.data_type)

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"table column property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_string_in_json(property_json, "column_name", error_name)
        verify_string_in_json(property_json, "data_type", error_name)

    def parse_from_json(self, graph_json: dict) -> None:
        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.column_name = property_json["column_name"]
        self.data_type = parse_type_from_string(property_json["data_type"])
