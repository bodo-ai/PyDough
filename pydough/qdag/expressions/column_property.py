"""
Definition of PyDough QDAG nodes for expressions that are columns of a table
collection.
"""

__all__ = ["ColumnProperty"]

from pydough.metadata.properties import TableColumnMetadata
from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.qdag.errors import PyDoughQDAGException
from pydough.types import PyDoughType

from .expression_qdag import PyDoughExpressionQDAG


class ColumnProperty(PyDoughExpressionQDAG):
    """
    The QDAG node implementation class representing a column of a relational
    table.
    """

    def __init__(self, column_property: TableColumnMetadata):
        self._column_property: TableColumnMetadata = column_property

    @property
    def column_property(self) -> TableColumnMetadata:
        """
        The metadata for the table column this expression refers to.
        """
        return self._column_property

    @property
    def pydough_type(self) -> PyDoughType:
        return self.column_property.data_type

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
        if not hasattr(self.column_property.collection, "table_path"):
            raise PyDoughQDAGException(
                f"collection of {self.column_property.error_name} does not have a 'table_path' field"
            )
        table_path: str = self.column_property.collection.table_path
        column_name: str = self.column_property.column_name
        return f"Column[{table_path}.{column_name}]"

    def equals(self, other: object) -> bool:
        return isinstance(other, ColumnProperty) and (
            self.column_property == other.column_property
        )
