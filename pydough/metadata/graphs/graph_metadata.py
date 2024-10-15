"""
TODO: add file-level docstring
"""

from typing import List, Union, Dict
from collections import defaultdict
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_valid_name,
    verify_has_type,
)
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.properties import PropertyMetadata


class GraphMetadata(object):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, collections: Dict[str, CollectionMetadata]):
        self.name: str = name
        self.collections: Dict[str, CollectionMetadata] = collections

    def __repr__(self):
        raise f"GraphMetadata({self.name})"

    def __eq__(self, other):
        return isinstance(other, GraphMetadata) and (self.name, self.collections) == (
            other.name,
            other.collections,
        )

    def get_collection_names(self) -> List[str]:
        """
        Fetches all of the names of collections in the current graph.
        """
        return list(self.collections)

    def get_collection(self, collection_name: str) -> CollectionMetadata:
        """
        Fetches a specific collection's metadata from within the graph by name.
        """
        if collection_name not in self.collections:
            raise PyDoughMetadataException(
                f"Graph {self.name} does not have a collection named {collection_name}"
            )
        return self.collections[collection_name]

    def get_nouns(
        self,
    ) -> Dict[str, List[Union["GraphMetadata", CollectionMetadata, PropertyMetadata]]]:
        """
        Fetches all of the names of collections/properties in the graph.
        """
        nouns = defaultdict[list]
        nouns[self.name].append(self)
        for collection in self.collections.values():
            for name, value in collection.get_nouns():
                nouns[name].append(value)
        return nouns

    def verify_json_metadata(graph_name, graph_json) -> None:
        """
        TODO: add function docstring.
        """
        error_name = f"PyDough metadata for graph {repr(graph_name)}"
        verify_valid_name(graph_name)
        verify_has_type(graph_json, dict, error_name, "JSON object")
        for collection_name in graph_json:
            CollectionMetadata.verify_json_metadata(
                graph_name, collection_name, graph_json
            )
