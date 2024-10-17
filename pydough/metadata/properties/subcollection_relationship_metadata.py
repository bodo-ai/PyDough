"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from .property_metadata import PropertyMetadata
from pydough.metadata.errors import verify_has_type
from pydough.metadata.collections import CollectionMetadata


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
        verify_has_type(
            collection,
            CollectionMetadata,
            f"Other collection of {self.__class__.__name__}",
        )
        verify_has_type(
            singular, bool, f"Property 'singular' of {self.__class__.__name__}"
        )
        verify_has_type(
            no_collisions,
            bool,
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
    def singular(self) -> CollectionMetadata:
        """
        True if there is at most 1 record of the subcollection for each record
        of the collection, False if there could be more than 1.
        """
        return self._singular

    @property
    def no_collisions(self) -> CollectionMetadata:
        """
        True if no two distinct record from the collection have the same record
        of the subcollection referenced by the property, False if such
        collisions can occur.
        """
        return self._no_collisions

    @property
    @abstractmethod
    def components(self) -> tuple:
        return super().components + (
            self.other_collection.name,
            self.singular,
            self.no_collisions,
        )

    @property
    def is_plural(self) -> bool:
        return self.singular

    @property
    def is_subcollection(self) -> bool:
        return True
