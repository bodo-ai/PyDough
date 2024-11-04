"""
TODO: add file-level docstring
"""

__all__ = ["TableCollection"]


from pydough.metadata import CollectionMetadata
from .collection_ast import PyDoughCollectionAST
from .collection_access import CollectionAccess


class TableCollection(CollectionAccess):
    """
    The AST node implementation class representing a table collection accessed
    as a root.
    """

    def __init__(self, collection: CollectionMetadata, ancestor: PyDoughCollectionAST):
        super().__init__(collection, ancestor)

    def to_string(self) -> str:
        return self.collection.name

    def to_tree_form(self) -> None:
        raise NotImplementedError
