"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from .subcollection_relationship_metadata import SubcollectionRelationshipMetadata
from pydough.metadata.collections import CollectionMetadata


class ReversiblePropertyMetadata(SubcollectionRelationshipMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
    ):
        super().__init__(name, collection, other_collection, singular, no_collisions)
        self.reverse_name: str = reverse_name
        self.reverse_property: ReversiblePropertyMetadata = None

    @property
    @abstractmethod
    def components(self) -> tuple:
        return super().components + (self.reverse_name,)

    @abstractmethod
    def build_reverse_relationship(self) -> None:
        """
        TODO: add function docstring.
        """

    @property
    def is_reversible(self) -> bool:
        return True
