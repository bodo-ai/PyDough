"""
TODO: add file-level docstring
"""

from abc import ABC, abstractmethod

from typing import Dict, Tuple
from pydough.metadata.errors import (
    verify_json_has_property_with_type,
    PyDoughMetadataException,
    verify_valid_name,
)


class PropertyMetadata(ABC):
    """
    TODO: add class docstring
    """

    def __init__(self, graph_name: str, collection_name: str, name: str):
        self.graph_name = graph_name
        self.collection_name = collection_name
        self.name = name

    @abstractmethod
    def components(self) -> Tuple:
        """
        TODO: add function docstring.
        """
        return (self.graph_name, self.collection_name, self.name)

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(repr(component) for component in self.components())})"

    def __eq__(self, other):
        return (type(self) is type(other)) and (self.components() == other.components())

    def verify_json_metadata(
        graph_name: str, collection_name: str, property_name: str, property_json: Dict
    ) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.properties import (
            TableColumnMetadata,
            SimpleJoinMetadata,
            CompoundRelationshipMetadata,
        )

        verify_valid_name(collection_name)
        error_name = f"property {repr(property_name)} of collection {repr(collection_name)} in graph {repr(graph_name)}"
        verify_json_has_property_with_type(property_json, "type", str, error_name)
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

    @abstractmethod
    def verify_ready_to_add(self, collection) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.collections import CollectionMetadata

        if not isinstance(collection, CollectionMetadata):
            raise PyDoughMetadataException(
                f"Expected the collection of verify_ready_to_add to be a CollectionMetadata, received: {collection.__class__.__name__}"
            )

    @abstractmethod
    def parse_from_json(self, collections: Dict, graph_json: Dict) -> None:
        """
        TODO: add function docstring.
        """
