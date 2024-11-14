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

    def __init__(
        self, name: str, data_type: PyDoughType, input_name: str | None = None
    ) -> None:
        super().__init__(data_type)
        self._name: str = name
        self._input_name: str | None = input_name

    def __hash__(self) -> int:
        return hash((self.name, self.data_type))

    @property
    def name(self) -> object:
        """
        The name of the column.
        """
        return self._name

    @property
    def input_name(self) -> str | None:
        """
        The name of the input node. This is a required
        translation used by nodes with multiple inputs. The input
        name doesn't need to have any "inherent" meaning and is only
        important in the context of the current node.
        """
        return self._input_name

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        input_name_str = f"input={self.input_name}, " if self.input_name else ""
        return f"Column({input_name_str}name={self.name}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ColumnReference)
            and (self.data_type == other.data_type)
            and (self.name == other.name)
            and (self.input_name == other.input_name)
        )
