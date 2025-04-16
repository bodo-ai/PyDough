"""
Definition of PyDough QDAG nodes for expression references to another expression
in a preceding context.
"""

__all__ = ["SidedReference"]


from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG


class SidedReference(PyDoughExpressionQDAG):
    """
    The QDAG node implementation class representing a reference to a term from
    one of two sides of connection between a parent collection and a child
    connection. This type of expression node is only allowed to exist inside of
    a general join condition.
    """

    def __init__(
        self, term_name: str, collection: PyDoughCollectionQDAG, is_parent: bool
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._term_name: str = term_name
        base_collection: PyDoughCollectionQDAG
        if is_parent:
            assert collection.ancestor_context is not None
            base_collection = collection.ancestor_context.starting_predecessor
        else:
            base_collection = collection.starting_predecessor
        self._expression: PyDoughExpressionQDAG = base_collection.get_expr(term_name)
        if not self.expression.is_singular(collection.starting_predecessor):
            raise PyDoughQDAGException(
                f"Cannot reference plural expression {self.expression} from {self.collection}"
            )
        self._is_parent: bool = is_parent

    @property
    def collection(self) -> PyDoughCollectionQDAG:
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
    def expression(self) -> PyDoughExpressionQDAG:
        """
        The original expression that the reference refers to.
        """
        return self._expression

    @property
    def is_parent(self) -> bool:
        """
        Whether the reference is to the parent side of the connection.
        """
        return self._is_parent

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expression.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expression.is_aggregation

    def is_singular(self, context: PyDoughQDAG) -> bool:
        # References are already known to be singular via their construction.
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        prefix: str = "PARENT" if self.is_parent else "CHILD"
        return f"{prefix}.{self.term_name}"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, SidedReference)
            and self.term_name == other.term_name
            and self.is_parent == other.is_parent
            and self.collection.equals(other.collection)
        )
