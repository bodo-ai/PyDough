"""
TODO: add file-level docstring
"""

__all__ = ["PartitionKey"]


from functools import cache

from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.pydough_ast.collections.collection_ast import PyDoughCollectionAST
from pydough.types import PyDoughType

from .child_reference_expression import ChildReferenceExpression
from .expression_ast import PyDoughExpressionAST


class PartitionKey(PyDoughExpressionAST):
    """
    The wrapper class around expressions to denote that an expression
    is a key used for partitioning. Currently only allows the expression to be
    a reference to an expression from the partition data.
    """

    def __init__(
        self, collection: PyDoughCollectionAST, expr: ChildReferenceExpression
    ):
        self._collection: PyDoughCollectionAST = collection
        self._expr: ChildReferenceExpression = expr

    @property
    def collection(self) -> PyDoughCollectionAST:
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
    def is_singular(self, context: PyDoughAST) -> bool:
        assert isinstance(context, PyDoughCollectionAST)
        return (context == self.collection) or self.collection.is_singular(context)

    def to_string(self, tree_form: bool = False) -> str:
        return self.expr.to_string(tree_form)

    def requires_enclosing_parens(self, parent: "PyDoughExpressionAST") -> bool:
        return self.expr.requires_enclosing_parens(parent)

    def equals(self, other: object) -> bool:
        if isinstance(other, PartitionKey):
            return self.expr.equals(other.expr)
        else:
            return self.expr.equals(other)
