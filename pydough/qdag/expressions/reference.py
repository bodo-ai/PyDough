"""
Definition of PyDough AST nodes for expression references to another expression
in a preceding context.
"""

__all__ = ["Reference"]


from pydough.qdag.abstract_pydough_qdag import PyDoughAST
from pydough.qdag.collections.collection_qdag import PyDoughCollectionAST
from pydough.qdag.errors import PyDoughASTException
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionAST


class Reference(PyDoughExpressionAST):
    """
    The AST node implementation class representing a reference to a term in
    a preceding collection.
    """

    def __init__(self, collection: PyDoughCollectionAST, term_name: str):
        self._collection: PyDoughCollectionAST = collection
        self._term_name: str = term_name
        self._expression: PyDoughExpressionAST = collection.get_expr(term_name)
        if not self.expression.is_singular(collection.starting_predecessor):
            raise PyDoughASTException(
                f"Cannot reference plural expression {self.expression} from {self.collection}"
            )

    @property
    def collection(self) -> PyDoughCollectionAST:
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
    def expression(self) -> PyDoughExpressionAST:
        """
        The original expression that the reference refers to.
        """
        return self._expression

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expression.pydough_type

    @property
    def is_aggregation(self) -> bool:
        return self.expression.is_aggregation

    def is_singular(self, context: PyDoughAST) -> bool:
        # References are already known to be singular via their construction.
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return self.term_name

    def equals(self, other: object) -> bool:
        return isinstance(other, Reference) and self.expression.equals(other.expression)
