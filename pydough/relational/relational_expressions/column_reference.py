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
        super().__init__(data_type)
        self._name: str = name

    def __hash__(self) -> int:
        return hash((self.name, self.data_type))

    @property
    def name(self) -> object:
        """
        The name of the column.
        """
        return self._name

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"Column(name={self.name}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ColumnReference)
            and (self.data_type == self.data_type)
            and (self.name == other.name)
        )
