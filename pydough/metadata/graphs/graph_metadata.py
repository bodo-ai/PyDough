from typing import List, Union, Dict
from collections import defaultdict
from pydough.metadata.errors import verify_is_json
from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.properties import PropertyMetadata


class GraphMetadata(object):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str, collections: Dict[str, CollectionMetadata]):
        self.name: str = name
        self.collections: Dict[str, CollectionMetadata] = collections
        self.nouns: Dict[str, List[Union[CollectionMetadata, PropertyMetadata]]] = (
            defaultdict(list)
        )
        self.nouns[self.name].append(self)
        for collection in self.collections.values():
            for noun, value in collection.get_nouns():
                self.nouns[noun].append(value)

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
            raise Exception(
                f"Graph {self.name} does not have a collection named {collection_name}"
            )
        return self.collections[collection_name]

    def get_nouns(self) -> Dict[str, List[Union[CollectionMetadata, PropertyMetadata]]]:
        """
        Fetches all of the names of collections/properties in the graph.
        """
        return self.nouns

    def verify_json_metadata(graph_name, graph_json) -> None:
        """
        TODO: add function doscstring.
        """
        error_name = f"PyDough metadata for graph {repr(graph_name)}"
        verify_is_json(graph_json, error_name)
        for collection_name in graph_json:
            CollectionMetadata.verify_json_metadata(
                graph_name, collection_name, graph_json
            )
