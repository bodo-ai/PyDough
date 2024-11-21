"""
TODO: add file-level docstring
"""

__all__ = ["TableCollection"]


from pydough.metadata import CollectionMetadata

from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST


class TableCollection(CollectionAccess):
    """
    The AST node implementation class representing a table collection accessed
    as a root.
    """

    def __init__(self, collection: CollectionMetadata, ancestor: PyDoughCollectionAST):
        super().__init__(collection, ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        return TableCollection(self.collection, new_ancestor)

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.{self.collection.name}"

    @property
    def standalone_string(self) -> str:
        return self.collection.name

    @property
    def tree_item_string(self) -> str:
        return f"TableCollection[{self.standalone_string}]"
