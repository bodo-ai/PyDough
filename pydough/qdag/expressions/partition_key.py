"""
Definition of PyDough QDAG nodes used to wrap certain expressions to denote them
as keys of a PARTITION BY clause.
"""

__all__ = ["PartitionKey"]


from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG
from pydough.types import PyDoughType

from .child_reference_expression import ChildReferenceExpression
from .expression_qdag import PyDoughExpressionQDAG


class PartitionKey(PyDoughExpressionQDAG):
    """
    The wrapper class around expressions to denote that an expression
    is a key used for partitioning. Currently only allows the expression to be
    a reference to an expression from the partition data.
    """

    def __init__(
        self, collection: PyDoughCollectionQDAG, expr: ChildReferenceExpression
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._expr: ChildReferenceExpression = expr

    @property
    def collection(self) -> PyDoughCollectionQDAG:
        """
        The PARTITION BY collection that the expression is being used as a key
        for.
        """
        return self._collection

    @property
    def expr(self) -> ChildReferenceExpression:
        """
        The expression being used as a partition key.
        """
        return self._expr

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expr.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expr.is_aggregation

    @cache
    def is_singular(self, context: PyDoughQDAG) -> bool:
        # A partition key is singular with regards to a context if and only
        # if the PARTITION BY clause it corresponds to is also singular with
        # regards to that context (or the PARTITION BY clause is the context).
        assert isinstance(context, PyDoughCollectionQDAG)
        return (context == self.collection) or self.collection.is_singular(context)

    def to_string(self, tree_form: bool = False) -> str:
        return self.expr.to_string(tree_form)

    def requires_enclosing_parens(self, parent: "PyDoughExpressionQDAG") -> bool:
        return self.expr.requires_enclosing_parens(parent)

    def equals(self, other: object) -> bool:
        if isinstance(other, PartitionKey):
            return self.expr.equals(other.expr)
        else:
            return self.expr.equals(other)
