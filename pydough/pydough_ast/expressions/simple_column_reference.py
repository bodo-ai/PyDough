"""
The representation of a simplified column for use as an expression in the Relational
tree. Since the relational step is geared towards building the actual SQL query, it doesn't
need the back references to the metadata, which can complicate the ability to determine information
like equivalence between columns.
"""

__all__ = ["SimpleColumnReference"]

from pydough.types import PyDoughType

from .expression_ast import PyDoughExpressionAST


class SimpleColumnReference(PyDoughExpressionAST):
    """
    The AST node implementation class representing a simple reference to a column.
    This is used once the AST is converted to a relational tree to avoid creating
    duplicate Relational version of every function.

    Note: We avoid using index based accesses to columns to avoid confusion in cases
    where we may have a subset of information and to avoid needing to reorder columns.
    """

    def __init__(self, name: str, data_type: PyDoughType):
        self._name: str = name
        self._data_type: PyDoughType = data_type

    def __hash__(self) -> int:
        return hash((self._name, self._data_type))

    @property
    def name(self) -> object:
        """
        The name of the column.
        """
        return self._name

    @property
    def pydough_type(self) -> PyDoughType:
        return self._data_type

    @property
    def is_aggregation(self) -> bool:
        return False

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return f"Column({self.name})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, SimpleColumnReference)
            and (self.pydough_type == other.pydough_type)
            and (self.name == other.name)
        )
