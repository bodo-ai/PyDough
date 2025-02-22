"""
Definition of PyDough QDAG collection type for accesses to a subcollection of an
ancestor of the current context.
"""

__all__ = ["BackReferenceCollection"]


from functools import cache

from pydough.qdag.errors import PyDoughQDAGException

from .child_access import ChildAccess
from .collection_access import CollectionAccess
from .collection_qdag import PyDoughCollectionQDAG


class BackReferenceCollection(CollectionAccess):
    """
    The QDAG node implementation class representing a subcollection of an
    ancestor collection.
    """

    def __init__(
        self,
        parent: PyDoughCollectionQDAG,
        term_name: str,
        back_levels: int,
    ):
        if not (isinstance(back_levels, int) and back_levels > 0):
            raise PyDoughQDAGException(
                f"Expected number of levels in BACK to be a positive integer, received {back_levels!r}"
            )
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        ancestor: PyDoughCollectionQDAG = parent
        for _ in range(back_levels):
            if ancestor.ancestor_context is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughQDAGException(
                    f"Cannot reference back {msg} above {parent!r}"
                )
            ancestor = ancestor.ancestor_context
        access = ancestor.get_collection(term_name)
        assert isinstance(access, CollectionAccess)
        self._collection_access: CollectionAccess = access
        super().__init__(self._collection_access.collection, ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionQDAG) -> ChildAccess:
        return BackReferenceCollection(new_ancestor, self.term_name, self.back_levels)

    @property
    def back_levels(self) -> int:
        """
        The number of levels upward that the backreference refers to.
        """
        return self._back_levels

    @property
    def term_name(self) -> str:
        """
        The name of the subcollection being accessed from the ancestor.
        """
        return self._term_name

    @property
    def collection_access(self) -> CollectionAccess:
        """
        The collection access property of the ancestor that BACK points to.
        """
        return self._collection_access

    @cache
    def is_singular(self, context: PyDoughCollectionQDAG) -> bool:
        return self.collection_access.is_singular(
            self.collection_access.ancestor_context.starting_predecessor
        )

    @property
    def key(self) -> str:
        return self.standalone_string

    @property
    def standalone_string(self) -> str:
        return f"BACK({self.back_levels}).{self.term_name}"

    def to_string(self) -> str:
        return self.standalone_string

    @property
    def tree_item_string(self) -> str:
        return f"BackSubCollection[{self.back_levels}, {self.term_name}]"
