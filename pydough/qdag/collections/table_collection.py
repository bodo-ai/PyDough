"""
Definition of PyDough QDAG collection type for accessing a table collection
directly.
"""

__all__ = ["TableCollection"]


from pydough.metadata import CollectionMetadata

from .collection_access import CollectionAccess
from .collection_qdag import PyDoughCollectionQDAG


class TableCollection(CollectionAccess):
    """
    The QDAG node implementation class representing a table collection accessed
    as a root.
    """

    def __init__(self, collection: CollectionMetadata, ancestor: PyDoughCollectionQDAG):
        super().__init__(collection, ancestor)

    @property
    def name(self) -> str:
        return self.collection.name

    def clone_with_parent(
        self, new_ancestor: PyDoughCollectionQDAG
    ) -> CollectionAccess:
        return TableCollection(self.collection, new_ancestor)

    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        # A table collection is always a plural subcollection of the global
        # context since PyDough does not know how many rows it contains.
        return False

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.{self.collection.name}"

    @property
    def standalone_string(self) -> str:
        return self.collection.name

    @property
    def tree_item_string(self) -> str:
        return f"TableCollection[{self.standalone_string}]"
