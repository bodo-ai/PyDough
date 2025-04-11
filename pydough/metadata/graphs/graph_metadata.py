"""
Definition of PyDough metadata for a graph.
"""

from collections import defaultdict

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.errors import HasType, PyDoughMetadataException, is_valid_name


class GraphMetadata(AbstractMetadata):
    """
    Concrete metadata implementation for a PyDough graph that can contain
    PyDough collections.
    """

    def __init__(self, name: str):
        is_valid_name.verify(name, "graph name")
        self._name: str = name
        self._collections: dict[str, AbstractMetadata] = {}

    @property
    def name(self) -> str:
        """
        The name of the graph.
        """
        return self._name

    @property
    def collections(self) -> dict[str, AbstractMetadata]:
        """
        The collections contained within the graph.
        """
        return self._collections

    @property
    def error_name(self) -> str:
        return f"graph {self.name!r}"

    @property
    def components(self) -> list:
        return [self.name]

    @property
    def path(self) -> str:
        return self.name

    def add_collection(self, collection: AbstractMetadata) -> None:
        """
        Adds a new collection to the graph.

        Args:
            `collection`: the collection being inserted into the graph.

        Raises:
            `PyDoughMetadataException`: if `collection` cannot be inserted
            into the graph because.
        """
        from pydough.metadata.collections import CollectionMetadata

        # Make sure the collection is actually a collection
        HasType(CollectionMetadata).verify(collection, "collection")
        assert isinstance(collection, CollectionMetadata)

        # Verify sure the collection has not already been added to the graph
        # and does not have a name collision with any other collections in
        # the graph.
        if collection.name in self.collections:
            if self.collections[collection.name] == collection:
                raise PyDoughMetadataException(
                    f"Already added {collection.error_name} to {self.error_name}"
                )
            raise PyDoughMetadataException(
                f"Duplicate collections: {collection.error_name} versus {self.collections[collection.name].error_name}"
            )
        self.collections[collection.name] = collection

    def get_collection_names(self) -> list[str]:
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
                f"{self.error_name} does not have a collection named {collection_name!r}"
            )
        return self.collections[collection_name]

    def __getitem__(self, key: str):
        return self.get_collection(key)

    def get_nouns(self) -> dict[str, list[AbstractMetadata]]:
        nouns: dict[str, list[AbstractMetadata]] = defaultdict(list)
        nouns[self.name].append(self)
        for collection in self.collections.values():
            for name, values in collection.get_nouns().items():
                nouns[name].extend(values)
        return nouns
