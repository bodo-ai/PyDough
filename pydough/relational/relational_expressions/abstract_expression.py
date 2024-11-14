"""
This file contains the abstract base classes for the relational
expression representation. Relational expressions are representations
of literals, column accesses, or other functions that are used in the
relational tree to build the final SQL query.
"""

from abc import ABC, abstractmethod
from typing import Any

__all__ = ["RelationalExpression"]

from pydough.types import PyDoughType


class RelationalExpression(ABC):
    def __init__(self, data_type: PyDoughType) -> None:
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        return self._data_type

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
    def to_string(self) -> str:
        """
        Convert the relational expression to a string.

        Returns:
            str: A string representation of the this expression including converting
            any of its inputs to strings.
        """

    def __repr__(self) -> str:
        return self.to_string()
