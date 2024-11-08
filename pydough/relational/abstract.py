"""
This file contains the abstract base classes for the relational
representation. This roughly maps to a Relational Algebra representation
but is not exact because it needs to maintain PyDough traits that define
ordering and other properties of the relational expression.
"""

from abc import ABC, abstractmethod
from collections.abc import MutableMapping, MutableSequence
from typing import Any, NamedTuple

from sqlglot.expressions import Expression

from pydough.pydough_ast.expressions import PyDoughExpressionAST


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
    def traits(self) -> MutableMapping[str, Any]:
        """
        Return the traits of the relational expression.
        The traits in general may have a variable schema,
        but each entry should be strongly defined. Here are
        traits that should always be available:

        - orderings: MutableSequence[PyDoughExpressionAST]

        Returns:
            MutableMapping[str, Any]: The traits of the relational expression.
        """
        return {"orderings": self.orderings}

    @property
    @abstractmethod
    def orderings(self) -> MutableSequence["PyDoughExpressionAST"]:
        """
        Returns the PyDoughExpressionAST that the relational expression is ordered by.
        Each PyDoughExpressionAST is a result computed relative to the given set of columns.

        Returns:
            MutableSequence[PyDoughExpressionAST]: The PyDoughExpressionAST that the relational expression is ordered by,
            possibly empty.
        """

    @property
    @abstractmethod
    def columns(self) -> MutableSequence["Column"]:
        """
        Returns the columns of the relational expression.

        Returns:
            MutableSequence[Column]: The columns of the relational expression.
        """

    @abstractmethod
    def to_sqlglot(self) -> "Expression":
        """Translate the given relational expression
        and its children to a SQLGlot expression.

        Returns:
            Expression: A SqlGlot expression representing the relational expression.
        """

    @abstractmethod
    def to_string(self) -> str:
        """
        Convert the relational expression to a string.

        TODO: Refactor this API to include some form of string
        builder so we can draw lines between children properly.

        Returns:
            str: A string representation of the relational tree
            with this node at the root.
        """

    @abstractmethod
    def can_merge(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes can be merged together.

        Args:
            other (Relational): The other relational node to merge with.

        Returns:
            bool: Can the two relational nodes be merged together.
        """

    @abstractmethod
    def merge(self, other: "Relational") -> "Relational":
        """
        Merge two relational nodes together to produce one output
        relational node. This requires can_merge to return True.

        Args:
            other (Relational): The other relational node to merge with.

        Returns:
            Relational: A new relational node that is the result of merging
            the two input relational nodes together and removing any redundant
            components.
        """


class Column(NamedTuple):
    """
    An column expression consisting of a name and an expression.
    """

    name: str
    expr: "PyDoughExpressionAST"
