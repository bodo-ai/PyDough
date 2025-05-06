"""
This file contains the abstract base classes for the relational
representation. This roughly maps to a Relational Algebra representation
but is not exact because it needs to maintain PyDough traits that define
ordering and other properties of the relational expression.
"""

from abc import ABC, abstractmethod
from typing import Any

from pydough.relational.relational_expressions import RelationalExpression


class RelationalNode(ABC):
    """
    The base class for any relational node. This interface defines the basic
    structure of all relational nodes in the PyDough system.
    """

    def __init__(self, columns: dict[str, RelationalExpression]) -> None:
        self._columns: dict[str, RelationalExpression] = columns

    @property
    @abstractmethod
    def inputs(self) -> list["RelationalNode"]:
        """
        Returns any inputs to the current relational expression.

        Returns:
            list["RelationalNode"]: The list of inputs, each of which must
            be a relational expression.
        """

    @property
    def default_input_aliases(self) -> list[str | None]:
        """
        Provide the default aliases for each input
        to this node. This is used when remapping the
        names of each input for differentiating columns.
        By default we don't use an alias, which maps to None
        for each input.

        Note: The lowering steps are not required to use this alias
        and can choose any name they want.
        """
        return [None for i in range(len(self.inputs))]

    @property
    def columns(self) -> dict[str, RelationalExpression]:
        """
        Returns the columns of the relational expression.

        Returns:
            dict[str, RelationalExpression]: The columns of the relational expression.
                This does not have a defined ordering.
        """
        return self._columns

    @abstractmethod
    def node_equals(self, other: "RelationalNode") -> bool:
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

    def equals(self, other: "RelationalNode") -> bool:
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
        return isinstance(other, RelationalNode) and self.equals(other)

    def make_column_string(self, columns: dict[str, Any], compact: bool) -> str:
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
    def accept(self, visitor: "RelationalVisitor") -> None:  # type: ignore # noqa
        """
        Accept a visitor to traverse the relational tree.

        Args:
            visitor (RelationalVisitor): The visitor to traverse the tree.
        """

    @abstractmethod
    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list["RelationalNode"],
    ) -> "RelationalNode":
        """
        Copy the given relational node with the provided columns and/or
        inputs. This copy maintains any additional properties of the
        given node in the output. Every node is required to implement
        this directly.

        Args:
            columns (dict[str, RelationalExpression]): The columns
                to copy.
            inputs (list[RelationalNode]): The inputs to copy.

        Returns:
            Relational: The copied relational node.
        """

    def copy(
        self,
        columns: dict[str, RelationalExpression] | None = None,
        inputs: list["RelationalNode"] | None = None,
    ) -> "RelationalNode":
        """
        Copy the given relational node with the provided columns and/or
        inputs. This copy maintains any additional properties of the
        given node in the output. If any of the inputs are None, then we
        will grab those fields from the current node.

        Args:
            columns (dict[str, RelationalExpression] | None): The
                columns to copy.
            inputs (list[RelationalNode] | None): The inputs to copy.

        Returns:
            Relational: The copied relational node.
        """
        columns = self.columns if columns is None else columns
        inputs = self.inputs if inputs is None else inputs
        return self.node_copy(columns, inputs)
