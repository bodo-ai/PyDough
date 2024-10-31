"""
TODO: add file-level docstring
"""

__all__ = ["HiddenBackReferenceCollection"]


from .collection_ast import PyDoughCollectionAST
from .back_reference_collection import BackReferenceCollection


class HiddenBackReferenceCollection(BackReferenceCollection):
    """
    The AST node implementation class representing a subcollection of an
    ancestor collection.
    """

    def __init__(
        self,
        collection: PyDoughCollectionAST,
        ancestor: PyDoughCollectionAST,
        term_name: str,
        back_levels: int,
    ):
        self._collection: PyDoughCollectionAST = collection
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = ancestor
        super(BackReferenceCollection, self).__init__(
            self._ancestor.get_term(term_name)
        )

    def to_string(self) -> str:
        return f"HiddenBackReferenceCollection[{self.back_levels}.{self.collection.to_string()}]"

    def to_tree_string(self) -> str:
        raise NotImplementedError
