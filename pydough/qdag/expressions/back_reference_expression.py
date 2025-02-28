"""
Definition of PyDough QDAG nodes that reference an expression of an ancestor
context.
"""

__all__ = ["BackReferenceExpression"]
from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG
from .reference import Reference


class BackReferenceExpression(Reference):
    """
    The QDAG node implementation class representing a reference to a term in
    the ancestor context.
    """

    def __init__(
        self, collection: PyDoughCollectionQDAG, term_name: str, back_levels: int
    ):
        if not (isinstance(back_levels, int) and back_levels > 0):
            raise PyDoughQDAGException(
                f"Expected number of levels in BACK to be a positive integer, received {back_levels!r}"
            )
        self._collection: PyDoughCollectionQDAG = collection
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionQDAG = collection
        for _ in range(back_levels):
            ancestor = self._ancestor.ancestor_context
            if ancestor is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughQDAGException(
                    f"Cannot reference back {msg} above {collection!r}"
                )
            self._ancestor = ancestor
        self._expression = self._ancestor.get_expr(term_name)

    @property
    def back_levels(self) -> int:
        """
        The number of levels upward that the backreference refers to.
        """
        return self._back_levels

    @property
    def ancestor(self) -> PyDoughCollectionQDAG:
        """
        The specific ancestor collection that the ancestor refers to.
        """
        return self._ancestor

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expression.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expression.is_aggregation

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return self.term_name

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, BackReferenceExpression)
            and self.ancestor.equals(other.ancestor)
        )
