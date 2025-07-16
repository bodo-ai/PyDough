"""
Definition of PyDough QDAG nodes for expression references to another expression
in a preceding context.
"""

__all__ = ["Reference"]


from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.collections.collection_qdag import PyDoughCollectionQDAG
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG


class Reference(PyDoughExpressionQDAG):
    """
    The QDAG node implementation class representing a reference to a term in
    a preceding collection.
    """

    def __init__(
        self, collection: PyDoughCollectionQDAG, term_name: str, term_type: PyDoughType
    ):
        self._collection: PyDoughCollectionQDAG = collection
        self._term_name: str = term_name
        self._term_type: PyDoughType = term_type

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
    def pydough_type(self) -> PyDoughType:
        return self._term_type

    @property
    def is_aggregation(self) -> bool:
        return False

    def is_singular(self, context: PyDoughQDAG) -> bool:
        # References are already known to be singular via their construction.
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return self.term_name

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Reference)
            and self.term_name == other.term_name
            and self.collection.equals(other.collection)
            and self.pydough_type == other.pydough_type
        )
