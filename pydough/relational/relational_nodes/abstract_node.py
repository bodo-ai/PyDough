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

from .relational_visitor import RelationalVisitor


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

        Returns:
            MutableMapping[str, RelationalExpression]: The columns of the relational expression.
                This does not have a defined ordering.
        """
        return self._columns

    @abstractmethod
    def node_equals(self, other: "Relational") -> bool:
        """
        Determine if two relational nodes are exactly identical,
        excluding column generic column details shared by every
        node. This should be extended to avoid duplicating equality
        logic shared across relational nodes.

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

    def make_column_string(
        self, columns: MutableMapping[str, Any], compact: bool
    ) -> str:
        """
        Converts the columns of the relational node to a deterministically
        ordered string (alphabetically).
        """
        pairs: list[str] = []
        for key in sorted(columns):
            expr = columns[key]
            assert isinstance(expr, RelationalExpression)
            pairs.append(f"{key!r}: {columns[key].to_string(compact)}")
        return f"{{{', '.join(pairs)}}}"

    @abstractmethod
    def to_string(self, compact: bool = False) -> str:
        """
        Convert the relational node to a string.

        Args:
            `compact`: if True, converts to a more minimal string form for the
            purposes of conversion to a tree string.

        Returns:
            str: A string representation of the relational tree
            with this node at the root.
        """

    def __repr__(self) -> str:
        return self.to_string()

    def to_tree_string(self) -> str:
        """
        Convert the relational node to a string, including the descendants
        of the node.

        Returns:
            str: A string representation of the relational tree
            with this node at the root.
        """
        from .tree_string_visitor import TreeStringVisitor

        visitor: TreeStringVisitor = TreeStringVisitor()
        self.accept(visitor)
        return visitor.make_tree_string()

    @abstractmethod
    def accept(self, visitor: RelationalVisitor) -> None:
        """
        Accept a visitor to traverse the relational tree.

        Args:
            visitor (RelationalVisitor): The visitor to traverse the tree.
        """
