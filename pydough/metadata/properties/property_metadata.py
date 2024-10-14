"""
TODO: add file-level docstring
"""

from typing import Dict, Tuple
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_string_in_json,
    verify_valid_name,
)


class PropertyMetadata(object):
    """
    TODO: add class docstring
    """

    def __init__(self, graph_name: str, collection_name: str, name: str):
        self.graph_name = graph_name
        self.collection_name = collection_name
        self.name = name
        self.dependencies = []

    def components(self) -> Tuple:
        """
        TODO: add function doscstring.
        """
        return (self.graph_name, self.collection_name, self.name)

    def __repr__(self):
        return f"{type(self)}({','.join(repr(component) for component in self.components())})"

    def __eq__(self, other):
        return (type(self) is type(other)) and (self.components() == other.components())

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function doscstring.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
        )

        verify_valid_name(collection_name)
        error_name = f"property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_string_in_json(property_json, "type", error_name)
        match property_json["type"]:
            case "table_column":
                TableColumnMetadata.verify_json_metadata(
                    graph_name, collection_name, property_name, property_json
                )
            case "simple_join":
                SimpleJoinMetadata.verify_json_metadata(
                    graph_name, collection_name, property_name, property_json
                )
            case "compound":
                CompoundRelationshipMetadata.verify_json_metadata(
                    graph_name, collection_name, property_name, property_json
                )
            case property_type:
                raise PyDoughMetadataException(
                    f"Unrecognized property type for {error_name}: {repr(property_type)}"
                )

    def parse_from_json(self, graph_json: dict) -> None:
        """
        TODO: add function doscstring.
        """
        pass
