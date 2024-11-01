"""
TODO: add file-level docstring
"""

__all__ = ["HiddenBackReferenceCollection"]


from .collection_ast import PyDoughCollectionAST
from .back_reference_collection import BackReferenceCollection
from .sub_collection import SubCollection
from pydough.metadata.properties import SubcollectionRelationshipMetadata


class HiddenBackReferenceCollection(BackReferenceCollection):
    """
    The AST node implementation class representing a subcollection of an
    ancestor collection.
    """

    def __init__(
        self,
        compound: PyDoughCollectionAST,
        ancestor: PyDoughCollectionAST,
        alias: str,
        term_name: str,
        back_levels: int,
    ):
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = ancestor
        self._subcollection: PyDoughCollectionAST = self._ancestor.get_term(term_name)
        self._alias: str = alias
        super(SubCollection, self).__init__(self._subcollection.collection)
        self._parent: PyDoughCollectionAST = compound
        self._subcollection_property: SubcollectionRelationshipMetadata = (
            self._subcollection.subcollection_property
        )

    @property
    def alias(self) -> str:
        """
        The alias that the back reference uses.
        """
        return self._alias

    def to_string(self) -> str:
        return f"{self.parent.to_string()}.{self.alias}"

    def to_tree_string(self) -> str:
        raise NotImplementedError
