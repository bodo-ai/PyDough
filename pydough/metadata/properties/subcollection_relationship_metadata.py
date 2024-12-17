"""
Definition of the base class for PyDough metadata for properties that
access a subcollection.
"""

__all__ = ["SubcollectionRelationshipMetadata"]

from abc import abstractmethod

from pydough.metadata.collections import CollectionMetadata
from pydough.metadata.errors import HasType, is_bool

from .property_metadata import PropertyMetadata


class SubcollectionRelationshipMetadata(PropertyMetadata):
    """
    Abstract base class for PyDough metadata for properties that map
    to a subcollection of a collection, e.g. by joining two tables.
    """

    def __init__(
        self,
        name: str,
        collection: CollectionMetadata,
        other_collection: CollectionMetadata,
        singular: bool,
        no_collisions: bool,
    ):
        super().__init__(name, collection)
        HasType(CollectionMetadata).verify(
            collection,
            f"other collection of {self.__class__.__name__}",
        )
        is_bool.verify(singular, f"Property 'singular' of {self.__class__.__name__}")
        is_bool.verify(
            no_collisions,
            f"Property 'no_collisions' of {self.__class__.__name__}",
        )
        self._other_collection: CollectionMetadata = other_collection
        self._singular: bool = singular
        self._no_collisions: bool = no_collisions

    @property
    def other_collection(self) -> CollectionMetadata:
        """
        The metadata for the subcollection that the property maps its own
        collection to.
        """
        return self._other_collection

    @property
    def singular(self) -> bool:
        """
        True if there is at most 1 record of the subcollection for each record
        of the collection, False if there could be more than 1.
        """
        return self._singular

    @property
    def no_collisions(self) -> bool:
        """
        True if no two distinct record from the collection have the same record
        of the subcollection referenced by the property, False if such
        collisions can occur.
        """
        return self._no_collisions

    @property
    @abstractmethod
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.other_collection.name)
        comp.append(self.singular)
        comp.append(self.no_collisions)
        return comp

    @property
    def is_plural(self) -> bool:
        return not self.singular

    @property
    def is_subcollection(self) -> bool:
        return True
