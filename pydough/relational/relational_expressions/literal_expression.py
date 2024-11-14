"""
The representation of a literal value for using in a relational
expression.
"""

__all__ = ["LiteralExpression"]

from typing import Any

from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.types import PyDoughType

from .abstract import RelationalExpression


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

    def __hash__(self) -> int:
        # Note: This will break if the value isn't hashable.
        return hash((self.value, self.data_type))

    @property
    def value(self) -> object:
        """
        The literal's Python value.
        """
        return self._value

    def to_sqlglot(self) -> SQLGlotExpression:
        raise NotImplementedError(
            "Conversion to SQLGlot Expressions is not yet implemented."
        )

    def to_string(self) -> str:
        return f"Literal(value={self.value}, type={self.data_type})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, LiteralExpression)
            and (self.data_type == other.data_type)
            and (self.value == other.value)
        )
