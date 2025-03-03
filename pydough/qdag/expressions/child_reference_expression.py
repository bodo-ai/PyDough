"""
Definition of PyDough QDAG nodes for referencing expressions from a child
collection of a child operator, e.g. `orders.order_date` in
`customers.CALCULATE(most_recent_order=MAX(orders.order_date))`.
"""

__all__ = ["ChildReferenceExpression"]

from functools import cache

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG
from pydough.qdag.errors import PyDoughQDAGException

from .expression_qdag import PyDoughExpressionQDAG
from .reference import Reference


class ChildReferenceExpression(Reference):
    """
    The QDAG node implementation class representing a reference to a term in
    a child collection of a CALCULATE or similar child operator node.
    """

    def __init__(
        self, collection: PyDoughCollectionQDAG, child_idx: int, term_name: str
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._child_idx: int = child_idx
        self._term_name: str = term_name
        self._expression: PyDoughExpressionQDAG = self._collection.get_expr(term_name)
        if not self.expression.is_singular(collection.starting_predecessor):
            raise PyDoughQDAGException(
                f"Cannot reference plural expression {self.expression} from {self.collection}"
            )

    @property
    def child_idx(self) -> int:
        """
        The integer index of the child from the child operator that the
        ChildReferenceExpression refers to.
        """
        return self._child_idx

    @cache
    def is_singular(self, context: PyDoughQDAG) -> bool:
        # Child reference expressions are already known to be singular relative
        # to the child collection to the via their construction, so they are
        # singular relative to the context if and only if their child collection
        # is singular relative to the context.
        assert isinstance(context, PyDoughCollectionQDAG)
        return self.collection.is_singular(context)

    def to_string(self, tree_form: bool = False) -> str:
        if tree_form:
            return f"${self.child_idx + 1}.{self.term_name}"
        else:
            return f"{self.collection.to_string()}.{self.term_name}"
