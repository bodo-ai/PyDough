"""
TODO: add file-level docstring
"""

from abc import abstractmethod

from .property_metadata import PropertyMetadata
from pydough.metadata.errors import (
    verify_has_type,
)


class SubcollectionRelationshipMetadata(PropertyMetadata):
    """
    TODO: add class docstring
    """

    def __init__(
        self,
        name: str,
        collection,
        other_collection,
        singular: bool,
        no_collisions: bool,
    ):
        from pydough.metadata.collections import CollectionMetadata

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
            no_collisions,
            f"Property 'no_collisions' of {self.__class__.__name__}",
        )
        self.other_collection = other_collection
        self.singular = singular
        self.no_collisions = no_collisions

    @abstractmethod
    def components(self) -> tuple:
        return super().components() + (
            self.other_collection.name,
            self.singular,
            self.no_collisions,
        )

    def is_plural(self) -> bool:
        return self.singular

    def is_subcollection(self) -> bool:
        return True
