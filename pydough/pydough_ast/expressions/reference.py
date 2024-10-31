"""
TODO: add file-level docstring
"""

__all__ = ["Reference"]

from . import PyDoughExpressionAST
from pydough.types import PyDoughType
from pydough.pydough_ast.collections import PyDoughCollectionAST


class Reference(PyDoughExpressionAST):
    """
    The AST node implementation class representing a reference to a term in
    a preceding collection.
    """

    def __init__(self, collection: PyDoughCollectionAST, term_name: str):
        self._collection: PyDoughCollectionAST = collection
        self._term_name: str = term_name
        self._expression: PyDoughExpressionAST = collection.get_term(term_name)

    @property
    def collection(self) -> PyDoughCollectionAST:
        """
        The collection that the Reference term comes from.
        """
        return self._collection

    @property
    def term_name(self) -> str:
        """
        The name of the term that the Reference refers to.
        """
        return self._term_name

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
        return self.term_name

    def equals(self, other: "Reference") -> bool:
        return super().equals(other) and self.expression.equals(other.expression)
