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

    def __init__(
        self,
        columns: MutableSequence["Column"],
        orderings: MutableSequence["PyDoughExpressionAST"] | None,
    ) -> None:
        self._columns: MutableSequence[Column] = columns
        self._orderings: MutableSequence[PyDoughExpressionAST] = (
            orderings if orderings else []
        )

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
    def orderings(self) -> MutableSequence["PyDoughExpressionAST"]:
        """
        Returns the PyDoughExpressionAST that the relational expression is ordered by.
        Each PyDoughExpressionAST is a result computed relative to the given set of columns.

        Returns:
            MutableSequence[PyDoughExpressionAST]: The PyDoughExpressionAST that the relational expression is ordered by,
            possibly empty.
        """
        return self._orderings

    def orderings_match(
        self, other_orderings: MutableSequence["PyDoughExpressionAST"]
    ) -> bool:
        """
        Determine if two orderings match in a way that would be considered equivalent
        for the given node.

        Args:
            other_orderings (MutableSequence[PyDoughExpressionAST]): The orderings property
            of another relational node.

        Returns:
            bool: Can the two orderings be considered equivalent and therefore safely merged.
        """
        # TODO: Allow merging compatible orderings?
        return not self.orderings and not other_orderings

    @property
    def columns(self) -> MutableSequence["Column"]:
        """
        Returns the columns of the relational expression.

        Returns:
            MutableSequence[Column]: The columns of the relational expression.
        """
        return self._columns

    def columns_match(self, other_columns: MutableSequence["Column"]) -> bool:
        """
        Determine if two sets of columns match in a way that would be considered compatible
        for the given node. In general we current assume that columns are indexed by name
        and any columns with the same name must be equivalent.

        Args:
            other_columns (MutableSequence[Column]): The columns property
            of another relational node.

        Returns:
            bool: Can the two columns be considered equivalent and therefore safely merged.
        """
        first_keys = {col.name: col.expr for col in self.columns}
        second_keys = {col.name: col.expr for col in other_columns}
        for key, value in first_keys.items():
            if key in second_keys and value != second_keys[key]:
                return False
        return True

    def merge_columns(self, other_columns: MutableSequence["Column"]) -> list["Column"]:
        """
        Merge two sets of columns together, keeping the original ordering
        of self as much as possible. This eliminates any duplicates between
        the two sets of columns and assumes that if two columns have the same name
        then they must match (which is enforced by the columns_match method).

        Args:
            other_columns (MutableSequence[Column]): The columns property
            of another relational node.


        Returns:
            list["Column"]: The list of merged columns keeping the original ordering
            of self as much as possible.
        """
        cols = list(self.columns)
        col_set = set(cols)
        for col in other_columns:
            if col not in col_set:
                cols.append(col)
        return cols

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
        return (
            self.node_equals(other)
            and self.columns == other.columns
            and self.orderings == other.orderings
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Relational):
            return False
        return self.equals(other)

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

    def __repr__(self) -> str:
        return self.to_string()

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
