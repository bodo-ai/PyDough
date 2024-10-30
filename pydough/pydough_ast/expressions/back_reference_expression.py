"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceExpression"]

from . import PyDoughExpressionAST
from pydough.types import PyDoughType
from pydough.pydough_ast.collections import PyDoughCollectionAST
from pydough.pydough_ast.errors import PyDoughASTException
from .reference import Reference


class BackReferenceExpression(Reference):
    """
    The AST node implementation class representing a reference to a term in
    the ancestor context.
    """

    def __init__(
        self, collection: PyDoughCollectionAST, term_name: str, back_levels: int
    ):
        self._collection: PyDoughCollectionAST = collection
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = collection
        for _ in range(back_levels):
            self._ancestor = self._ancestor.ancestor_context
            if self._ancestor is None:
                raise PyDoughASTException(
                    f"Cannot reference back {back_levels} levels above {collection!r}"
                )
        self._expression: PyDoughExpressionAST = self._ancestor.get_term(term_name)

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
    def expression(self) -> PyDoughExpressionAST:
        """
        The original expression that the reference refers to.
        """
        return self._expression

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expression.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expression.is_aggregation

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self) -> str:
        return f"BackReferenceExpression[{self.back_levels}:{self.term_name}]"

    def equals(self, other: "BackReferenceExpression") -> bool:
        return super().equals(other) and self.ancestor.equals(other.ancestor)
