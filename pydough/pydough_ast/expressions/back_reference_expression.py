"""
TODO: add file-level docstring
"""

__all__ = ["BackReferenceExpression"]
from pydough.pydough_ast.collections.collection_ast import PyDoughCollectionAST
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.types import PyDoughType

from .expression_ast import PyDoughExpressionAST
from .reference import Reference


class BackReferenceExpression(Reference):
    """
    The AST node implementation class representing a reference to a term in
    the ancestor context.
    """

    def __init__(
        self, collection: PyDoughCollectionAST, term_name: str, back_levels: int
    ):
        if not (isinstance(back_levels, int) and back_levels > 0):
            raise PyDoughASTException(
                f"Expected number of levels in BACK to be a positive integer, received {back_levels!r}"
            )
        self._collection: PyDoughCollectionAST = collection
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = collection
        for _ in range(back_levels):
            ancestor = self._ancestor.ancestor_context
            if ancestor is None:
                msg: str = "1 level" if back_levels == 1 else f"{back_levels} levels"
                raise PyDoughASTException(
                    f"Cannot reference back {msg} above {collection!r}"
                )
            self._ancestor = ancestor
            assert ancestor is not None
        self._expression: PyDoughExpressionAST = self._ancestor.get_expr(term_name)

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

    def to_string(self, tree_form: bool = False) -> str:
        return f"BACK({self.back_levels}).{self.term_name}"

    def equals(self, other: object) -> bool:
        return (
            super().equals(other)
            and isinstance(other, BackReferenceExpression)
            and self.ancestor.equals(other.ancestor)
        )
