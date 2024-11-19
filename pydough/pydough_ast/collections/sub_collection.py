"""
TODO: add file-level docstring
"""

__all__ = ["SubCollection"]


from pydough.metadata.properties import SubcollectionRelationshipMetadata

from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST


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

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        return SubCollection(self.subcollection_property, new_ancestor)

    @property
    def subcollection_property(self) -> SubcollectionRelationshipMetadata:
        """
        The subcollection property referenced by the collection node.
        """
        return self._subcollection_property

    @property
    def key(self) -> str:
        return f"{self.ancestor_context.key}.{self.subcollection_property.name}"

    @property
    def standalone_string(self) -> str:
        return self.subcollection_property.name

    def to_string(self) -> str:
        return f"{self.ancestor_context.to_string()}.{self.standalone_string}"

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
