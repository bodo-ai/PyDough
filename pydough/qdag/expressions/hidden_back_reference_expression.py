"""
Definition of PyDough QDAG nodes that reference an expression of an ancestor
context that is hidden because the ancestor comes from a compound
subcollection.
"""

__all__ = ["HiddenBackReferenceExpression"]

from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG

from .back_reference_expression import BackReferenceExpression
from .expression_qdag import PyDoughExpressionQDAG


class HiddenBackReferenceExpression(BackReferenceExpression):
    """
    The QDAG node implementation class representing a reference to a term in
    the ancestor context through the lens of a compound relationship's
    inherited properties.
    """

    def __init__(
        self,
        collection: PyDoughCollectionQDAG,
        ancestor: PyDoughCollectionQDAG,
        alias: str,
        term_name: str,
        back_levels: int,
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._alias: str = alias
        self._term_name: str = term_name
        self._back_levels: int = back_levels
        self._ancestor: PyDoughCollectionQDAG = ancestor
        self._expression: PyDoughExpressionQDAG = self._ancestor.get_expr(term_name)

    @property
    def alias(self) -> str:
        """
        The alias that the back reference uses.
        """
        return self._alias

    def to_string(self, tree_form: bool = False) -> str:
        return f"HiddenBackReferenceExpression[{self.back_levels}:{self.term_name}]"
