"""
The representation of a column access for use in a relational tree.
The provided name of the column should match the name that can be used
for the column in the input node.
"""

__all__ = ["ColumnReference"]

from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression


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
    def name(self) -> str:
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

    def with_input(self, input_name: str | None) -> "ColumnReference":
        """
        Returns a clone of a ColumnReference but with a new input name.

        Args:
            `input_name`: the value of input_name used for the clone.

        Returns:
            The cloned value of `self` with the desired `input_name`, or just
            `self` if the input name is the same as `self`.
        """
        if self.input_name == input_name:
            return self
        return ColumnReference(self.name, self.data_type, input_name)

    def to_string(self, compact: bool = False) -> str:
        if compact:
            prefix: str = f"{self.input_name}." if self.input_name else ""
            return f"{prefix}{self.name}"
        else:
            input_name_str = f"input={self.input_name}, " if self.input_name else ""
            return f"Column({input_name_str}name={self.name}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ColumnReference)
            and (self.name == other.name)
            and (self.input_name == other.input_name)
            and super().equals(other)
        )

    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        visitor.visit_column_reference(self)

    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> RelationalExpression:
        return shuttle.visit_column_reference(self)
