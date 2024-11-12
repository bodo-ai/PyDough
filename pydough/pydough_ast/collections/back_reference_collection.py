"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceCollection"]


from pydough.pydough_ast.errors import PyDoughASTException

from .collection_access import CollectionAccess
from .collection_ast import PyDoughCollectionAST
from .collection_tree_form import CollectionTreeForm


class BackReferenceCollection(CollectionAccess):
    """
    The AST node implementation class representing a subcollection of an
    ancestor collection.
    """

    def __init__(
        self,
        parent: PyDoughCollectionAST,
        term_name: str,
        back_levels: int,
    ):
        if not (isinstance(back_levels, int) and back_levels > 0):
            raise PyDoughASTException(
                f"Expected number of levels in BACK to be a positive integer, received {back_levels!r}"
            )
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        ancestor: PyDoughCollectionAST = parent
        for _ in range(back_levels):
            if ancestor.ancestor_context is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughASTException(
                    f"Cannot reference back {msg} above {parent!r}"
                )
            ancestor = ancestor.ancestor_context
        access = ancestor.get_collection(term_name)
        assert isinstance(access, CollectionAccess)
        self._collection_access: CollectionAccess = access
        super().__init__(self._collection_access.collection, ancestor)

    def clone_with_parent(self, new_ancestor: PyDoughCollectionAST) -> CollectionAccess:
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

    def to_string(self) -> str:
        return f"BACK({self.back_levels}).{self.term_name}"

    def to_tree_form(self) -> CollectionTreeForm:
        raise NotImplementedError
