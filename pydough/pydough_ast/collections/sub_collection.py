"""
TODO: add file-level docstring
"""

__all__ = ["SubCollection"]


from pydough.metadata.properties import SubcollectionRelationshipMetadata
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from .table_collection import TableCollection


class SubCollection(TableCollection):
    """
    The AST node implementation class representing a subcollection accessed
    from its parent collection.
    """

    def __init__(
        self,
        parent: PyDoughCollectionAST,
        subcollection_property: SubcollectionRelationshipMetadata,
    ):
        super().__init__(subcollection_property.other_collection)
        self._parent: PyDoughAST = parent
        self._subcollection_property: SubcollectionRelationshipMetadata = (
            subcollection_property
        )

    @property
    def parent(self) -> PyDoughCollectionAST:
        """
        The parent node that the collection node is a subcollection of.
        """
        return self._parent

    @property
    def subcollection_property(self) -> SubcollectionRelationshipMetadata:
        """
        The subcollection property referenced by the collection node.
        """
        return self._subcollection_property

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self.parent

    def to_string(self) -> str:
        return f"{self.parent.to_string()}.{self.collection.name}"

    def to_tree_string(self) -> str:
        raise NotImplementedError

    def equals(self, other: "SubCollection") -> bool:
        return (
            super().equals(other)
            and self.preceding_context == other.preceding_context
            and self.subcollection_property == other.subcollection_property
        )
