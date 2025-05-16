"""
Definition of PyDough metadata for a graph.
"""

from pydough.metadata.abstract_metadata import AbstractMetadata
from pydough.metadata.errors import HasType, PyDoughMetadataException, is_valid_name


class GraphMetadata(AbstractMetadata):
    """
    Concrete metadata implementation for a PyDough graph that can contain
    PyDough collections.
    """

    allowed_fields: set[str] = {
        "name",
        "version",
        "collections",
        "relationships",
        "additional definitions",
        "verified pydough analysis",
        "extra semantic info",
    }
    """
    Fields allowed in the JSON object describing a graph.
    """

    def __init__(
        self,
        name: str,
        additional_definitions: list[str] | None,
        verified_pydough_analysis: list[dict] | None,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        is_valid_name.verify(name, "graph name")
        self._additional_definitions: list[str] | None = additional_definitions
        self._verified_pydough_analysis: list[dict] | None = verified_pydough_analysis
        self._name: str = name
        self._collections: dict[str, AbstractMetadata] = {}
        super().__init__(description, synonyms, extra_semantic_info)

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

    @property
    def additional_definitions(self) -> list[str] | None:
        """
        Additional semantic definitions of logical concepts using the
        collections within the graph.
        """
        return self._additional_definitions

    @property
    def verified_pydough_analysis(self) -> list[dict] | None:
        """
        Verified PyDough analysis examples using the collections within
        the graph.
        """
        return self._verified_pydough_analysis

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
