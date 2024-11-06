"""
TODO: add file-level docstring
"""

__all__ = ["TableCollection"]


from pydough.metadata import CollectionMetadata

from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class TableCollection(CollectionAccess):
    """
    The AST node implementation class representing a table collection accessed
    as a root.
    """

    def __init__(self, collection: CollectionMetadata, ancestor: PyDoughCollectionAST):
        super().__init__(collection, ancestor)

    def to_string(self) -> str:
        return self.collection.name

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form()
        predecessor.has_children = True
        return CollectionTreeForm(
            f"TableCollection[{self.to_string()}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )
