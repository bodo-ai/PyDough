"""
TODO: add file-level docstring
"""

__all__ = ["ChildReferenceExpression"]

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.collections.collection_ast import PyDoughCollectionAST

from .expression_ast import PyDoughExpressionAST
from .reference import Reference


class ChildReferenceExpression(Reference):
    """
    The AST node implementation class representing a reference to a term in
    a child collection of a CALC.
    """

    def __init__(
        self, collection: PyDoughCollectionAST, child_idx: int, term_name: str
    ):
        self._collection: PyDoughCollectionAST = collection
        self._child_idx: int = child_idx
        self._term_name: str = term_name
        self._expression: PyDoughExpressionAST = self._collection.get_expr(term_name)

    @property
    def child_idx(self) -> int:
        """
        The integer index of the child from the CALC that hte ChildReferenceExpression
        refers to.
        """
        return self._child_idx

    def is_singular(self, context: PyDoughAST) -> bool:
        assert isinstance(context, PyDoughCollectionAST)
        return self.collection.is_singular(context) and self.expression.is_singular(
            self.collection
        )

    def to_string(self, tree_form: bool = False) -> str:
        if tree_form:
            return f"${self.child_idx+1}.{self.term_name}"
        else:
            return f"{self.collection.to_string()}.{self.term_name}"
