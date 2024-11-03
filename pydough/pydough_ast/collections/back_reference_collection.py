"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceCollection"]


from pydough.metadata.properties import SubcollectionRelationshipMetadata
from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from .collection_ast import PyDoughCollectionAST
from .sub_collection import SubCollection
from pydough.pydough_ast.errors import PyDoughASTException
from .collection_tree_form import CollectionTreeForm


class BackReferenceCollection(SubCollection):
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
        self._parent: PyDoughAST = parent
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = parent
        for _ in range(back_levels):
            self._ancestor = self._ancestor.ancestor_context
            if self._ancestor is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughASTException(
                    f"Cannot reference back {msg} above {parent!r}"
                )
        self._ancestor.get_term(term_name)
        self._subcollection: SubCollection = self._ancestor.get_term(term_name)
        super(SubCollection, self).__init__(self._subcollection.collection)
        self._subcollection_property: SubcollectionRelationshipMetadata = (
            self._subcollection.subcollection_property
        )

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
    def subcollection(self) -> SubCollection:
        """
        The subcollection property of the ancestor that BACK points to.
        """
        return self._subcollection

    def to_string(self) -> str:
        return f"BACK({self.back_levels}).{self._subcollection.to_string()}"

    def to_tree_form(self) -> CollectionTreeForm:
        raise NotImplementedError
