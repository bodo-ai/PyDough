"""
TODO: add file-level docstring
"""

__all__ = ["ColumnProperty"]

from pydough.metadata.properties import TableColumnMetadata
from pydough.pydough_ast.errors import PyDoughASTException
from pydough.types import PyDoughType

from .expression_ast import PyDoughExpressionAST


class ColumnProperty(PyDoughExpressionAST):
    """
    The AST node implementation class representing a column of a relational
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

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        if not hasattr(self.column_property.collection, "table_path"):
            raise PyDoughASTException(
                f"collection of {self.column_property.error_name} does not have a 'table_path' field"
            )
        table_path: str = self.column_property.collection.table_path
        column_name: str = self.column_property.column_name
        return f"Column[{table_path}.{column_name}]"

    def equals(self, other: object) -> bool:
        return isinstance(other, ColumnProperty) and (
            self.column_property == other.column_property
        )
