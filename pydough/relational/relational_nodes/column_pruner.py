"""
Module responsible for pruning columns from relational expressions.
"""

from pydough.relational.relational_expressions import (
    ColumnReference,
    ColumnReferenceFinder,
)

from .abstract_node import Relational
from .project import Project
from .relational_expression_dispatcher import RelationalExpressionDispatcher
from .relational_root import RelationalRoot


class ColumnPruner:
    def __init__(self) -> None:
        self._column_finder: ColumnReferenceFinder = ColumnReferenceFinder()
        # Note: We set recurse=False so we only check the expressions in the current
        # node.
        self._dispatcher = RelationalExpressionDispatcher(
            self._column_finder, recurse=False
        )

    def _prune_identity_project(self, node: Relational) -> Relational:
        """
        Remove a projection and return the input if it is an
        identity projection.

        Args:
            node (Relational): The node to check for identity projection.

        Returns:
            Relational: The new node with the identity projection removed.
        """
        if isinstance(node, Project) and node.is_identity():
            return node.inputs[0]
        else:
            return node

    def _prune_node_columns(
        self, node: Relational, kept_columns: set[str]
    ) -> Relational:
        """
        Prune the columns for a subtree starting at this node.

        Args:
            node (Relational): The node to prune columns from.
            kept_columns (set[str]): The columns to keep.

        Returns:
            Relational: The new node with pruned columns. Its input may also
                be changed if columns were pruned from it.
        """
        # Prune columns from the node.
        columns = {
            name: expr for name, expr in node.columns.items() if name in kept_columns
        }
        # Update the columns.
        new_node = node.copy(columns=columns)
        self._dispatcher.reset()
        # Visit the current identifiers.
        new_node.accept(self._dispatcher)
        found_identifiers: set[ColumnReference] = (
            self._column_finder.get_column_references()
        )
        # Determine which identifiers to pass to each input.
        new_inputs: list[Relational] = []
        for i, default_input_name in enumerate(new_node.default_input_aliases):
            s: set[str] = set()
            for identifier in found_identifiers:
                if identifier.input_name == default_input_name:
                    s.add(identifier.name)
            new_inputs.append(self._prune_node_columns(node.inputs[i], s))
        # Determine the new node.
        output = new_node.copy(inputs=new_inputs)
        return self._prune_identity_project(output)

    def prune_unused_columns(self, root: RelationalRoot) -> RelationalRoot:
        """
        Prune columns that are unused in each relational expression.

        Args:
            root (RelationalRoot): The tree root to prune columns from.

        Returns:
            RelationalRoot: The root after updating all inputs.
        """
        new_root: Relational = self._prune_node_columns(root, set(root.columns.keys()))
        assert isinstance(new_root, RelationalRoot), "Expected a root node."
        return new_root
