"""
TODO: add file-level docstring
"""

__all__ = ["HiddenBackReferenceCollection"]


from .collection_ast import PyDoughCollectionAST
from .back_reference_collection import BackReferenceCollection
from .collection_tree_form import CollectionTreeForm
from .collection_access import CollectionAccess


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
        self._compound = compound
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._alias: str = alias
        self._collection_access: CollectionAccess = ancestor.get_term(term_name)
        super(BackReferenceCollection, self).__init__(
            self._collection_access.collection, compound
        )

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        return self.compound.clone_with_parent(new_ancestor).get_term(self.alias)

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
        predecessor: CollectionTreeForm = self.ancestor_context.to_tree_form()
        predecessor.has_children = True
        return CollectionTreeForm(
            f"SubCollection[{self.alias}]",
            predecessor.depth + 1,
            predecessor=predecessor,
        )
