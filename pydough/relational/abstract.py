"""
This file contains the abstract base classes for the relational
representation. This roughly maps to a Relational Algebra representation
but is not exact because it needs to maintain PyDough traits that define
ordering and other properties of the relational expression.
"""

from abc import ABC, abstractmethod
from collections.abc import MutableSequence
from typing import Any, NamedTuple

from sqlglot.expressions import Expression as SQLGlotExpression

from .relational_expressions.abstract import RelationalExpression


class Relational(ABC):
    """
    The base class for any relational node. This interface defines the basic
    structure of all relational nodes in the PyDough system.
    """

    @property
    @abstractmethod
    def inputs(self) -> MutableSequence["Relational"]:
        """
        Returns any inputs to the current relational expression.

        Returns:
            MutableSequence["Relational"]: The list of inputs, each of which must
            be a relational expression.
        """

    @property
    @abstractmethod
    def columns(self) -> MutableSequence["RelationalColumn"]:
        """
        Returns the columns of the relational expression.

        Returns:
            MutableSequence[Column]: The columns of the relational expression.
        """

    @abstractmethod
    def equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        including column ordering.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Relational) and self.equals(other)

    @abstractmethod
    def to_sqlglot(self) -> SQLGlotExpression:
        """Translate the given relational node
        and its children to a SQLGlot expression.

        Returns:
            Expression: A SqlGlot expression representing the relational node.
        """

    @abstractmethod
    def to_string(self) -> str:
        """
        Convert the relational node to a string.

        TODO: Refactor this API to include some form of string
        builder so we can draw lines between children properly.

        Returns:
            str: A string representation of the relational tree
            with this node at the root.
        """

    def __repr__(self) -> str:
        return self.to_string()


class RelationalColumn(NamedTuple):
    """
    An column expression consisting of a name and an expression.
    """

    name: str
    expr: RelationalExpression
