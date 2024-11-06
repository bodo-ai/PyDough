"""
TODO: add file-level docstring
"""

__all__ = ["HiddenBackReferenceCollection"]


from .back_reference_collection import BackReferenceCollection
from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


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
        self._compound: PyDoughCollectionAST = compound
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._alias: str = alias
        collection_access = ancestor.get_term(term_name)
        assert isinstance(collection_access, CollectionAccess)
        self._collection_access = collection_access
        super(BackReferenceCollection, self).__init__(
            self._collection_access.collection, compound
        )

    @property
    def compound(self) -> PyDoughCollectionAST:
        """
        The compound collection containing the hidden backreference.
        """
        return self._compound

    @property
    def alias(self) -> str:
        """
        The alias that the back reference uses.
        """
        return self._alias

    def to_string(self) -> str:
        return f"{self.compound.to_string()}.{self.alias}"

    def to_tree_form(self) -> CollectionTreeForm:
        assert self.ancestor_context is not None
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form()
        predecessor.has_children = True
        return CollectionTreeForm(
            f"SubCollection[{self.alias}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )
