"""
TODO: add file-level docstring
"""

from typing import Dict
from .scalar_attribute_metadata import ScalarAttributeMetadata
from pydough.metadata.errors import (
    verify_object_has_property_with_type,
)
from pydough.types import parse_type_from_string, PyDoughType


class TableColumnMetadata(ScalarAttributeMetadata):
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

    def create_error_name(name: str, collection_error_name: str):
        return f"table column property {name!r} of {collection_error_name}"

    def components(self) -> tuple:
        return super().components() + (self.column_name, self.data_type)

    def verify_json_metadata(
        graph, collection, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        # verify_valid_name(property_name)
        # verify_has_type(graph, GraphMetadata, "graph")
        # verify_has_type(collection, GraphMetadata, "graph")
        # error_name = SimpleTableMetadata.create_error_name(
        #     collection_name, graph.error_name
        # )
        # error_name = TableColumnMetadata.create_error_name()
        # error_name = f"table column property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        # verify_json_has_property_with_type(
        #     property_json, "column_name", str, error_name
        # )
        # verify_json_has_property_with_type(property_json, "data_type", str, error_name)

    def verify_ready_to_add(self, collection) -> None:
        super().verify_ready_to_add(collection)
        error_name = f"{self.__class__.__name__} instance {self.name}"
        verify_object_has_property_with_type(self, "column_name", str, error_name)
        verify_object_has_property_with_type(self, "data_type", PyDoughType, error_name)

    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        property_json = graph_json[self.collection_name]["properties"][self.name]
        self.column_name = property_json["column_name"]
        self.data_type = parse_type_from_string(property_json["data_type"])
