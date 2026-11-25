"""
This file contains the abstract base classes for the relational
expression representation. Relational expressions are representations
of literals, column accesses, or other functions that are used in the
relational tree to build the final SQL query.
"""

from abc import ABC, abstractmethod

__all__ = ["RelationalExpression"]

import pydough.pydough_operators as pydop
from pydough.types import BooleanType, PyDoughType


class RelationalExpression(ABC):
    def __init__(self, data_type: PyDoughType) -> None:
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        return self._data_type

    @staticmethod
    def form_conjunction(terms: list["RelationalExpression"]) -> "RelationalExpression":
        """
        Builds a condition from a conjunction of terms.

        Args:
            `terms`: the list of relational expressions forming the
            conjunction.

        Returns:
            A relational expression describing the logical-AND of the
            values of `terms`.
        """
        from .call_expression import CallExpression
        from .literal_expression import LiteralExpression

        terms = [
            term
            for term in terms
            if not (isinstance(term, LiteralExpression) and bool(term.value))
        ]

        if len(terms) == 0:
            return LiteralExpression(True, BooleanType())
        elif len(terms) == 1:
            return terms[0]
        else:
            return CallExpression(pydop.BAN, BooleanType(), terms)

    def equals(self, other: "RelationalExpression") -> bool:
        """
        Determine if two RelationalExpression nodes are exactly identical,
        including ordering. This does not check if two expression are equal
        after any alterations, for example commuting the inputs.

        Args:
            `other`: The other relational expression to compare against.

        Returns:
            Are the two relational expressions equal.
        """
        return (
            isinstance(other, RelationalExpression)
            and self.data_type == other.data_type
        )

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RelationalExpression) and self.equals(other)

    def is_same_key(self, other: "RelationalExpression") -> bool:
        """
        Determine if two expressions are interchangeable as partition or sort
        keys, so that one of them can be removed as a duplicate.

        This is stricter than `equals`, which compares literal values with
        `==`: here every literal must also have the same Python type and
        representation. For example, `x + 1` and `x + 1.0` are equal but are
        not the same key, since they can sort or group large integers
        differently.

        Args:
            `other`: The other relational expression to compare against.

        Returns:
            Whether the two expressions are the same key.
        """
        # Imported here to avoid a circular import with the expression classes.
        from .literal_type_finder import LiteralTypeFinder

        if not self.equals(other):
            return False
        self_finder: LiteralTypeFinder = LiteralTypeFinder()
        other_finder: LiteralTypeFinder = LiteralTypeFinder()
        self.accept(self_finder)
        other.accept(other_finder)
        return self_finder.get_literal_types() == other_finder.get_literal_types()

    @abstractmethod
    def to_string(self, compact: bool = False) -> str:
        """
        Convert the relational expression to a string.

        Returns:
            A string representation of the this expression including converting
            any of its inputs to strings.
        """

    def __repr__(self) -> str:
        return self.to_string()

    def __hash__(self) -> int:
        return hash(self.to_string())

    @abstractmethod
    def accept(self, visitor: "RelationalExpressionVisitor") -> None:  # type: ignore # noqa
        """
        Visit this relational expression with the provided visitor.

        Args:
            `visitor`: The visitor to use to visit this node.
        """

    @abstractmethod
    def accept_shuttle(
        self,
        shuttle: "RelationalExpressionShuttle",  # type: ignore # noqa
    ) -> "RelationalExpression":
        """
        Visit this relational expression with the provided shuttle and
        return the new expression.

        Args:
            `shuttle`: The shuttle to use to visit this node.

        Returns:
            The new expression resulting from visiting this node.
        """
