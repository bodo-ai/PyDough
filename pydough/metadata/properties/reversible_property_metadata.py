"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from .property_metadata import PropertyMetadata


class ReversiblePropertyMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(self, graph_name: str, collection_name: str, name: str):
        from pydough.metadata.collections import CollectionMetadata

        super().__init__(graph_name, collection_name, name)
        self.reverse_relationship_name: str = None
        self.reverse_property: ReversiblePropertyMetadata = None
        self.reverse_collection: CollectionMetadata = None

    @abstractmethod
    def build_reverse_relationship(self) -> "ReversiblePropertyMetadata":
        """
        TODO: add function docstring.
        """
