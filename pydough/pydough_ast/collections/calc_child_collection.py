"""
TODO: add file-level docstring
"""

__all__ = ["CalcChildCollection"]


from .back_reference_collection import BackReferenceCollection
from .collection_access import CollectionAccess
from .collection_tree_form import CollectionTreeForm
from .hidden_back_reference_collection import HiddenBackReferenceCollection
from .sub_collection import SubCollection
from .table_collection import TableCollection


class CalcChildCollection(CollectionAccess):
    """
    Special wrapper around a CollectionAccess instance that denotes it as an
    immediate child of a CALC node, for the purposes of stringification.
    """

    def __init__(
        self,
        collection_access: CollectionAccess,
        is_last: bool,
    ):
        ancestor = collection_access.ancestor_context
        assert ancestor is not None
        super().__init__(
            collection_access.collection,
            ancestor,
            collection_access.preceding_context,
        )
        self._collection_access: CollectionAccess = collection_access
        self._is_last: bool = is_last

    @property
    def collection_access(self) -> CollectionAccess:
        """
        The CollectionAccess node that is being wrapped.
        """
        return self._collection_access

    @property
    def is_last(self) -> bool:
        """
        Whether this is the last child of the CALC
        """
        return self._is_last

    def to_string(self) -> str:
        # Does not include the parent since this exists within the context
        # of a CALC node.
        if isinstance(self.collection_access, HiddenBackReferenceCollection):
            return self.collection_access.alias
        elif isinstance(self.collection_access, BackReferenceCollection):
            return self.collection_access.to_string()
        elif isinstance(self.collection_access, TableCollection):
            return self.collection_access.collection.name
        elif isinstance(self.collection_access, SubCollection):
            return self.collection_access.subcollection_property.name
        else:
            raise NotImplementedError

    def to_tree_form(self) -> CollectionTreeForm:
        predecessor: CollectionTreeForm = CollectionTreeForm(
            "CalcSubCollection",
            0,
            has_predecessor=True,
            has_children=True,
            has_successor=not self.is_last,
        )
        item_str: str
        if isinstance(self.collection_access, TableCollection):
            item_str = f"TableCollection[{self.to_string()}]"
        else:
            item_str = f"SubCollection[{self.to_string()}]"
        return CollectionTreeForm(
            item_str,
            predecessor.depth + 1,
            predecessor=predecessor,
        )
