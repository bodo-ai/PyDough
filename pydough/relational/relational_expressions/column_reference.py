"""
The representation of a column access for use in a relational tree.
The provided name of the column should match the name that can be used
for the column in the input node.
"""

__all__ = ["ColumnReference"]

from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.types import PyDoughType

from .abstract import RelationalExpression


class ColumnReference(RelationalExpression):
    """
    The Expression implementation for accessing a column
    in a relational node.
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

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"Column({self.name})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ColumnReference)
            and (self.pydough_type == other.pydough_type)
            and (self.name == other.name)
        )
