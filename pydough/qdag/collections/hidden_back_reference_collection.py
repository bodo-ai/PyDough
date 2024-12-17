"""
Definition of PyDough AST collection type for accesses to a subcollection of an
ancestor of the current context in a manner that is hidden because the ancestor
is from a compound subcollection access.
"""

__all__ = ["HiddenBackReferenceCollection"]


from pydough.qdag.errors import PyDoughASTException

from .back_reference_collection import BackReferenceCollection
from .collection_access import CollectionAccess
from .collection_qdag import PyDoughCollectionAST
from .compound_sub_collection import CompoundSubCollection


class HiddenBackReferenceCollection(BackReferenceCollection):
    """
    The AST node implementation class representing a subcollection of an
    ancestor collection.
    """

    def __init__(
        self,
        context: PyDoughCollectionAST,
        alias: str,
        term_name: str,
        back_levels: int,
    ):
        self._context: PyDoughCollectionAST = context
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._alias: str = alias

        compound: PyDoughCollectionAST = context
        while compound.preceding_context is not None:
            compound = compound.preceding_context
        if not isinstance(compound, CompoundSubCollection):
            raise PyDoughASTException(
                f"Malformed hidden backreference expression: {self.to_string()}"
            )
        self._compound: CompoundSubCollection = compound
        hidden_ancestor: CollectionAccess = compound.subcollection_chain[-back_levels]
        collection_access = hidden_ancestor.get_collection(term_name)
        assert isinstance(collection_access, CollectionAccess)
        self._collection_access = collection_access
        super(BackReferenceCollection, self).__init__(
            collection_access.collection, context
        )

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
        return HiddenBackReferenceCollection(
            new_ancestor, self.alias, self.term_name, self.back_levels
        )

    @property
    def context(self) -> PyDoughCollectionAST:
        """
        The collection context the hidden backreference operates within.
        """
        return self._context

    @property
    def compound(self) -> CompoundSubCollection:
        """
        The compound subcollection access that the hidden back reference
        traces to.
        """
        return self._compound

    @property
    def alias(self) -> str:
        """
        The alias that the back reference uses.
        """
        return self._alias

    @property
    def key(self) -> str:
        return f"{self.context.key}.{self.alias}"

    @property
    def standalone_string(self) -> str:
        return self.alias

    def to_string(self) -> str:
        return f"{self.context.to_string()}.{self.standalone_string}"

    @property
    def tree_item_string(self) -> str:
        return f"SubCollection[{self.standalone_string}]"
