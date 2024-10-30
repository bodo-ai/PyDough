"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceCollection"]


from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from .table_collection import TableCollection
from pydough.pydough_ast.errors import PyDoughASTException


class BackReferenceCollection(TableCollection):
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
        self._parent: PyDoughAST = parent
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = parent
        for _ in range(back_levels):
            self._ancestor = self._ancestor.ancestor_context
            if self._ancestor is None:
                raise PyDoughASTException(
                    f"Cannot reference back {back_levels} levels above {parent!r}"
                )
        super.__init__(self._ancestor.get_term(term_name))

    @property
    def parent(self) -> PyDoughCollectionAST:
        """
        The parent node that the collection node is a subcollection of.
        """
        return self._parent

    @property
    def back_levels(self) -> int:
        """
        The number of levels upward that the backreference refers to.
        """
        return self._back_levels

    @property
    def ancestor(self) -> PyDoughCollectionAST:
        """
        The specific ancestor collection that the ancestor refers to.
        """
        return self._ancestor

    @property
    def ancestor_context(self) -> PyDoughCollectionAST | None:
        return self.parent

    def to_string(self) -> str:
        return f"BackReference[{self.back_levels}.{self.collection.to_string()}"

    def to_tree_string(self) -> str:
        raise NotImplementedError

    def equals(self, other: "BackReferenceCollection") -> bool:
        return super().equals(other) and self.ancestor == other.ancestor
