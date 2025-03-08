"""
Definition of PyDough QDAG collection type for accesses to a subcollection of
a table collection.
"""

__all__ = ["SubCollection"]


from functools import cache

from pydough.metadata.properties import SubcollectionRelationshipMetadata

from .collection_access import CollectionAccess
from .collection_qdag import PyDoughCollectionQDAG


class SubCollection(CollectionAccess):
    """
    The QDAG node implementation class representing a subcollection accessed
    from its parent collection.
    """

    def __init__(
        self,
        subcollection_property: SubcollectionRelationshipMetadata,
        ancestor: PyDoughCollectionQDAG,
    ):
        super().__init__(subcollection_property.other_collection, ancestor)
        self._subcollection_property: SubcollectionRelationshipMetadata
        self._subcollection_property = subcollection_property

    def clone_with_parent(
        self, new_ancestor: PyDoughCollectionQDAG
    ) -> CollectionAccess:
        return SubCollection(self.subcollection_property, new_ancestor)

    @property
    def subcollection_property(self) -> SubcollectionRelationshipMetadata:
        """
        The subcollection property referenced by the collection node.
        """
        return self._subcollection_property

    @cache
    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # A subcollection is singular if the underlying subcollection property
        # is singular and the parent collection is singular relative to the
        # desired context (or the parent is the desired context).
        if self.subcollection_property.is_plural:
            return False
        relative_ancestor: PyDoughCollectionQDAG = (
            self.ancestor_context.starting_predecessor
        )
        return (context == relative_ancestor) or relative_ancestor.is_singular(context)

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.{self.subcollection_property.name}"

    @property
    def standalone_string(self) -> str:
        return self.subcollection_property.name

    @property
    def tree_item_string(self) -> str:
        return f"SubCollection[{self.standalone_string}]"

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and type(other) is type(self)
            and isinstance(other, SubCollection)
            and self.preceding_context == other.preceding_context
            and self.subcollection_property == other.subcollection_property
        )
