"""
TODO: add file-level docstring
"""

__all__ = ["CalcChildCollection"]


from .hidden_back_reference_collection import HiddenBackReferenceCollection
from .back_reference_collection import BackReferenceCollection
from .collection_access import CollectionAccess
from .table_collection import TableCollection


class CalcChildCollection(CollectionAccess):
    """
    Special wrapper around a CollectionAccess instance that denotes it as an
    immediate child of a CALC node, for the purposes of stringification.
    """

    def __init__(
        self,
        collection_access: CollectionAccess,
    ):
        super().__init__(
            collection_access.collection,
            collection_access.ancestor_context,
            collection_access.preceding_context,
        )
        self._collection_access: CollectionAccess = collection_access

    @property
    def collection_access(self) -> CollectionAccess:
        """
        The CollectionAccess node that is being wrapped.
        """
        return self._collection_access

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of a CALC node.
        if isinstance(self.collection_access, HiddenBackReferenceCollection):
            return self.collection_access.alias
        elif isinstance(self.collection_access, BackReferenceCollection):
            return self.collection_access.term_name
        elif isinstance(self.collection_access, TableCollection):
            return self.collection_access.collection.name
        else:
            return self.collection_access.subcollection_property.name

    def to_tree_form(self) -> None:
        raise NotImplementedError
