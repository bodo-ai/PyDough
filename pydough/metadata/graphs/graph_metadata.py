"""
TODO: add file-level docstring
"""

from typing import List, Dict
from collections import defaultdict
from pydough.metadata.errors import (
    PyDoughMetadataException,
    verify_valid_name,
    verify_has_type,
)

from pydough.metadata.abstract_metadata import AbstractMetadata


class GraphMetadata(AbstractMetadata):
    """
    TODO: add class docstring
    """

    def __init__(self, name: str):
        from pydough.metadata.collections import CollectionMetadata

        verify_valid_name(name)
        self.name: str = name
        self.collections: Dict[str, CollectionMetadata] = {}

    @property
    def error_name(self):
        """
        TODO: add function docstring
        """
        return f"graph {self.name!r}"

    @property
    def components(self) -> tuple:
        return (self.name,)

    def add_collection(self, collection):
        """
        Adds a new collection to the graph.
        """
        from pydough.metadata.collections import CollectionMetadata

        verify_has_type(collection, CollectionMetadata, "collection")
        collection: CollectionMetadata = collection
        if collection.name in self.collections:
            if self.collections[collection.name] == collection:
                raise PyDoughMetadataException(
                    f"Already added {collection.error_name} to {self.error_name}"
                )
            raise PyDoughMetadataException(
                f"Duplicate collections: {collection.error_name} versus {self.collections[collection.name].error_name}"
            )
        self.collections[collection.name] = collection

    def get_collection_names(self) -> List[str]:
        """
        Fetches all of the names of collections in the graph.
        """
        return list(self.collections)

    def get_collection(self, collection_name: str) -> AbstractMetadata:
        """
        Fetches a specific collection's metadata from within the graph by name.
        """
        if collection_name not in self.collections:
            raise PyDoughMetadataException(
                f"Graph {self.name} does not have a collection named {collection_name}"
            )
        return self.collections[collection_name]

    def get_nouns(self) -> Dict[str, List[AbstractMetadata]]:
        nouns = defaultdict[list]
        nouns[self.name].append(self)
        for collection in self.collections.values():
            for name, value in collection.get_nouns():
                nouns[name].append(value)
        return nouns

    def verify_json_metadata(self, graph_json: dict) -> None:
        """
        TODO: add function docstring.
        """
        from pydough.metadata.collections import CollectionMetadata

        for collection_name in graph_json:
            CollectionMetadata.verify_json_metadata(self, collection_name, graph_json)
