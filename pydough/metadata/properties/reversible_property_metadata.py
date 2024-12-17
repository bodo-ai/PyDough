"""
Definition of the base class for PyDough metadata for properties that
access a subcollection and are reversible.
"""

__all__ = ["ReversiblePropertyMetadata"]

from abc import abstractmethod

from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import PyDoughMetadataException

from .subcollection_relationship_metadata import SubcollectionRelationshipMetadata


class ReversiblePropertyMetadata(SubcollectionRelationshipMetadata):
    """
    Abstract base class for PyDough metadata for properties that map
    to a subcollection of a collection and also have a corresponding
    reverse relationship.
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
        self._reverse_name: str = reverse_name
        self._reverse_property: ReversiblePropertyMetadata | None = None

    @property
    def reverse_name(self) -> str:
        """
        The name of the reverse property.
        """
        return self._reverse_name

    @property
    def reverse_property(self) -> "ReversiblePropertyMetadata":
        """
        The reverse version of the property.

        Raises:
            `PyDoughMetadataException`: if the reverse property has not yet
            been defined.
        """
        if self._reverse_property is None:
            raise PyDoughMetadataException(
                f"Reverse property of {self.error_name} has not yet been defined."
            )
        return self._reverse_property

    @property
    @abstractmethod
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.reverse_name)
        return comp

    @abstractmethod
    def build_reverse_relationship(self) -> None:
        """
        Defines the reverse version of the property, which should obey the
        following rules:
        - `self.reverse_property.reverse_property is self`
        - `self.name == self.reverse_property.reverse_name`
        - `self.reverse_name == self.reverse_property.name`
        - `self.singular == self.reverse_property.no_collisions`
        - `self.no_collisions == self.reverse_property.singular`
        - `self.collection is self.reverse_property.other_collection`
        - `self.other_collection is self.reverse_property.collection`
        """

    @property
    def is_reversible(self) -> bool:
        return True
