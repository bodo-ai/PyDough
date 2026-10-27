"""
Definition of PyDough QDAG nodes for expressions that expand into another
PyDough expression.
"""

__all__ = ["PyDoughExpressionProperty"]

from pydough.metadata.properties import PyDoughExpressionColumnMetadata
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG


class PyDoughExpressionProperty(PyDoughExpressionQDAG):
    """
    The QDAG node implementation class representing a column of a relational
    table.
    """

    def __init__(self, expression_metadata: PyDoughExpressionColumnMetadata):
        self._expression_metadata: PyDoughExpressionColumnMetadata = expression_metadata

    @property
    def expression_metadata(self) -> PyDoughExpressionColumnMetadata:
        """
        The metadata for the table column this expression refers to.
        """
        return self._expression_metadata

    @property
    def pydough_type(self) -> PyDoughType:
        return self.expression_metadata.data_type

    @property
    def is_aggregation(self) -> bool:
        return False

    def is_singular(self, context: PyDoughQDAG) -> bool:
        # Column properties are always singular since they can only exist
        # within the context of the table collection they come from.
        return True

    def requires_enclosing_parens(self, parent: PyDoughExpressionQDAG) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return f"PyDoughExpression[{self.expression_metadata.expression}]"

    def equals(self, other: object) -> bool:
        return isinstance(other, PyDoughExpressionProperty) and (
            self.expression_metadata == other.expression_metadata
        )
