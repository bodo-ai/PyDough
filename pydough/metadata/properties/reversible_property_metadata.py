"""
Definition of the base class for PyDough metadata for properties that
access a subcollection and are reversible.
"""

__all__ = ["ReversiblePropertyMetadata"]

from abc import abstractmethod

from .subcollection_relationship_metadata import SubcollectionRelationshipMetadata


class ReversiblePropertyMetadata(SubcollectionRelationshipMetadata):
    """
    Abstract base class for PyDough metadata for properties that map
    to a subcollection of a collection and also have a corresponding
    reverse relationship.
    """

    @abstractmethod
    def build_reverse_relationship(
        self,
        name: str,
        is_singular: bool,
        always_matches: bool,
        description: str | None,
        synonyms: list[str] | None,
        extra_semantic_info: dict | None,
    ) -> "ReversiblePropertyMetadata":
        """
        Creates the reverse version of the property, going back from the child
        to the parent.

        Args:
            `name`: the name of the reverse property.
            `is_singular`: whether the reverse property is singular.
            `always_matches`: whether the reverse property always matches.
            `description`: the description of the reverse property.
            `synonyms`: the synonyms of the reverse property.
            `extra_semantic_info`: any extra semantic information for the
            reverse property.

        Returns:
            The reverse property of self with the specified name and
            cardinality.
        """

    @property
    def is_reversible(self) -> bool:
        return True
