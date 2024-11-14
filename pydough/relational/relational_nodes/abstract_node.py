"""
This file contains the abstract base classes for the relational
representation. This roughly maps to a Relational Algebra representation
but is not exact because it needs to maintain PyDough traits that define
ordering and other properties of the relational expression.
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any

from pydough.relational.relational_expressions import RelationalExpression


class Relational(ABC):
    """
    The base class for any relational node. This interface defines the basic
    structure of all relational nodes in the PyDough system.
    """

    def __init__(self, columns: MutableMapping[str, RelationalExpression]) -> None:
        self._columns: MutableMapping[str, RelationalExpression] = columns

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
    def columns(self) -> MutableMapping[str, RelationalExpression]:
        """
        Returns the columns of the relational expression.

        TODO: Associate an ordering in the future to avoid unnecessary SQL with the
        final ordering of the root nodes.

        Returns:
            MutableMapping[str, RelationalExpression]: The columns of the relational expression.
                This does not have a defined ordering.
        """
        return self._columns

    @abstractmethod
    def node_equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        excluding column ordering. This should be extended to avoid
        duplicating equality logic shared across relational nodes.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """

    def equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        including column ordering.

        Args:
            other (Relational): The other relational node to compare against.

        Returns:
            bool: Are the two relational nodes equal.
        """
        return self.node_equals(other) and self.columns == other.columns

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, Relational) and self.equals(other)

    @abstractmethod
    def to_string(self) -> str:
        """
        Convert the relational node to a string.

        Returns:
            str: A string representation of the relational tree
            with this node at the root.
        """

    def __repr__(self) -> str:
        return self.to_string()
