"""
TODO: add file-level docstring
"""

__all__ = ["ChildReference"]

from . import PyDoughExpressionAST
from pydough.pydough_ast.collections import PyDoughCollectionAST
from .reference import Reference
from pydough.pydough_ast.collections import Calc


class ChildReference(Reference):
    """
    The AST node implementation class representing a reference to a term in
    a child collection of a CALC.
    """

    def __init__(
        self, collection: PyDoughCollectionAST, child_idx: int, term_name: str
    ):
        self._collection: Calc = collection
        self._child_idx: int = child_idx
        self._term_name: str = term_name
        self._expression: PyDoughExpressionAST = self._collection.get_term(term_name)

    @property
    def child_idx(self) -> int:
        """
        The integer index of the child from the CALC that hte ChildReference
        refers to.
        """
        return self._child_idx

    def to_string(self) -> str:
        return f"{self.collection.to_string()}.{self.term_name}"
