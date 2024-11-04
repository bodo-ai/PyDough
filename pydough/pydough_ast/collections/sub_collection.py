"""
TODO: add file-level docstring
"""

__all__ = ["SubCollection"]


from pydough.metadata.properties import SubcollectionRelationshipMetadata
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm
from .collection_access import CollectionAccess


class SubCollection(CollectionAccess):
    """
    The AST node implementation class representing a subcollection accessed
    from its parent collection.
    """

    def __init__(
        self,
        subcollection_property: SubcollectionRelationshipMetadata,
        ancestor: PyDoughCollectionAST,
    ):
        super().__init__(subcollection_property.other_collection, ancestor)
        self._subcollection_property: SubcollectionRelationshipMetadata
        self._subcollection_property = subcollection_property

    @property
    def subcollection_property(self) -> SubcollectionRelationshipMetadata:
        """
        The subcollection property referenced by the collection node.
        """
        return self._subcollection_property

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.subcollection_property.name}"

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form()
        predecessor.has_children = True
        return CollectionTreeForm(
            f"SubCollection[{self.subcollection_property.name}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )

    def equals(self, other: "SubCollection") -> bool:
        return (
            super().equals(other)
            and self.preceding_context == other.preceding_context
            and self.subcollection_property == other.subcollection_property
        )
