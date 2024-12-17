"""
Definition of PyDough QDAG nodes used to wrap another expression node in
ordering information so the expression can be used as an ordering key.
"""

__all__ = ["CollationExpression"]


from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG


class CollationExpression(PyDoughExpressionQDAG):
    """
    The wrapper class around expressions to denote how to use them as an
    ordering key.
    """

    def __init__(self, expr: PyDoughExpressionQDAG, asc: bool, na_last: bool):
        self._expr: PyDoughExpressionQDAG = expr
        self._asc: bool = asc
        self._na_last: bool = na_last

    @property
    def expr(self) -> PyDoughExpressionQDAG:
        """
        The expression being used as a collation.
        """
        return self._expr

    @property
    def asc(self) -> bool:
        """
        Whether the collation key is ascending or descending.
        """
        return self._asc

    @property
    def na_last(self) -> bool:
        """
        Whether the collation key places nulls at the end.
        """
        return self._na_last

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expr.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expr.is_aggregation

    def is_singular(self, context: PyDoughQDAG) -> bool:
        # Collation expressions inherit their singular-or-plural nature from
        # their underlying expression.
        return self.expr.is_singular(context)

    def to_string(self, tree_form: bool = False) -> str:
        expr_string: str = self.expr.to_string(tree_form)
        if self.expr.requires_enclosing_parens(self):
            expr_string = f"({expr_string})"
        suffix = "ASC" if self.asc else "DESC"
        kwarg = "'last'" if self.na_last else "'first'"
        return f"{expr_string}.{suffix}(na_pos={kwarg})"

    def requires_enclosing_parens(self, parent: "PyDoughExpressionQDAG") -> bool:
        return False

    def equals(self, other: object) -> bool:
        if isinstance(other, CollationExpression):
            return (
                self.expr.equals(other.expr)
                and self.asc == other.asc
                and self.na_last == other.na_last
            )
        else:
            return self.expr.equals(other)
