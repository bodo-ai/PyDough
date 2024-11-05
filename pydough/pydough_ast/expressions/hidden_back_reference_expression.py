"""
TODO: add file-level docstring
"""

__all__ = ["HiddenBackReferenceExpression"]

from pydough.pydough_ast.collections import PyDoughCollectionAST
from .back_reference_expression import BackReferenceExpression


class HiddenBackReferenceExpression(BackReferenceExpression):
    """
    The AST node implementation class representing a reference to a term in
    the ancestor context through the lens of a compound relationship's
    inherited properties.
    """

    def __init__(
        self,
        collection: PyDoughCollectionAST,
        ancestor: PyDoughCollectionAST,
        alias: str,
        term_name: str,
        back_levels: int,
    ):
        self._collection: PyDoughCollectionAST = collection
        self._alias: str = alias
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionAST = ancestor
        self._expression = self._ancestor.get_expr(term_name)

    @property
    def alias(self) -> str:
        """
        The alias that the back reference uses.
        """
        return self._alias

    def to_string(self, tree_form: bool = False) -> str:
        return f"HiddenBackReferenceExpression[{self.back_levels}:{self.term_name}]"
