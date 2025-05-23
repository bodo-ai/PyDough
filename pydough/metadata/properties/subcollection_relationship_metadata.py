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
        parent_collection: CollectionMetadata,
        child_collection: CollectionMetadata,
        singular: bool,
        always_matches: bool,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ):
        super().__init__(
            name, parent_collection, description, synonyms, extra_semantic_info
        )
        HasType(CollectionMetadata).verify(
            child_collection,
            f"child collection of {self.__class__.__name__}",
        )
        is_bool.verify(singular, f"Property 'singular' of {self.__class__.__name__}")
        self._child_collection: CollectionMetadata = child_collection
        self._singular: bool = singular
        self._always_matches: bool = always_matches

    @property
    def child_collection(self) -> CollectionMetadata:
        """
        The metadata for the subcollection that the property maps its own
        collection to.
        """
        return self._child_collection

    @property
    def singular(self) -> bool:
        """
        True if there is at most 1 record of the subcollection for each record
        of the collection, False if there could be more than 1.
        """
        return self._singular

    @property
    def always_matches(self) -> bool:
        """
        True if the property always matches onto at least one record of the
        subcollection for each record of the collection, False if it may not
        match.
        """
        return self._always_matches

    @property
    @abstractmethod
    def components(self) -> list:
        comp: list = super().components
        comp.append(self.child_collection.name)
        comp.append(self.singular)
        return comp

    @property
    def is_plural(self) -> bool:
        return not self.singular

    @property
    def is_subcollection(self) -> bool:
        return True
