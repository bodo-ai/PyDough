"""
This file contains the abstract base classes for the relational
expression representation. Relational expressions are representations
of literals, column accesses, or other functions that are used in the
relational tree to build the final SQL query.
"""

from abc import ABC, abstractmethod
from typing import Any

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
            other (RelationalExpression): The other relational expression to compare against.

        Returns:
            bool: Are the two relational expressions equal.
        """
        return (
            isinstance(other, RelationalExpression)
            and self.data_type == other.data_type
        )

    def __eq__(self, other: Any) -> bool:
        return self.equals(other)

    @abstractmethod
    def to_string(self, compact: bool = False) -> str:
        """
        Convert the relational expression to a string.

        Returns:
            str: A string representation of the this expression including converting
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
            visitor (RelationalExpressionVisitor): The visitor to use to visit
                this node.
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
            shuttle (RelationalExpressionShuttle): The shuttle to use to visit
                this node.

        Returns:
            RelationalExpression: The new expression resulting from visiting
                this node.
        """
