"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceCollection"]

from typing import Dict, Tuple
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from pydough.pydough_ast.errors import PyDoughASTException
from .collection_access import CollectionAccess


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
            ancestor = ancestor.ancestor_context
            if ancestor is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughASTException(
                    f"Cannot reference back {msg} above {parent!r}"
                )
        self._collection_access: CollectionAccess = ancestor.get_term(term_name)
        super().__init__(self._collection_access.collection, ancestor)

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

    @property
    def properties(self) -> Dict[str, Tuple[int | None, PyDoughAST]]:
        return self.collection_access.properties

    def to_string(self) -> str:
        return f"BACK({self.back_levels}).{self.term_name}"

    def to_tree_form(self) -> None:
        raise NotImplementedError
