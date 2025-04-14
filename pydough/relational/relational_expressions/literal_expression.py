"""
The representation of a literal value for using in a relational
expression.
"""

__all__ = ["LiteralExpression"]

from typing import Any

from pydough.types import PyDoughType

from .abstract_expression import RelationalExpression


class LiteralExpression(RelationalExpression):
    """
    The Expression implementation for an Literal value
    in a relational node. There are no restrictions on the
    relationship between the value and the type so we can
    represent arbitrary Python classes as any type and lowering
    to SQL is responsible for determining how this can be
    achieved (e.g. casting) or translation must prevent this
    from being generated.
    """

    def __init__(self, value: Any, data_type: PyDoughType):
        super().__init__(data_type)
        self._value: Any = value

    @property
    def value(self) -> object:
        """
        The literal's Python value.
        """
        return self._value

    def to_string(self, compact: bool = False) -> str:
        if compact:
            return f"{repr(self.value)}:{self.data_type.json_string}"
        else:
            return f"Literal(value={repr(self.value)}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, LiteralExpression)
            and (self.value == other.value)
            and super().equals(other)
        )

    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        visitor.visit_literal_expression(self)

    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> RelationalExpression:
        return shuttle.visit_literal_expression(self)
