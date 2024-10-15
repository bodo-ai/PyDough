"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from .subcollection_relationship_metadata import SubcollectionRelationshipMetadata


class ReversiblePropertyMetadata(SubcollectionRelationshipMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        reverse_name: str,
        collection,
        other_collection,
        singular: bool,
        no_collisions: bool,
    ):
        super().__init__(name, collection, other_collection, singular, no_collisions)
        self.reverse_name = reverse_name
        self.reverse_property = None

    @abstractmethod
    def components(self) -> tuple:
        return super().components() + (self.reverse_name,)

    @abstractmethod
    def build_reverse_relationship(self) -> "ReversiblePropertyMetadata":
        """
        TODO: add function docstring.
        """

    def is_reversible(self) -> bool:
        return True
