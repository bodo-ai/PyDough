"""
Module responsible for pruning columns from relational expressions.
"""

from pydough.relational.relational_expressions import (
    ColumnReference,
    ColumnReferenceFinder,
    CorrelatedReference,
)

from .abstract_node import Relational
from .aggregate import Aggregate
from .join import Join
from .project import Project
from .relational_expression_dispatcher import RelationalExpressionDispatcher
from .relational_root import RelationalRoot

__all__ = ["ColumnPruner"]


class ColumnPruner:
    def __init__(self) -> None:
        self._column_finder: ColumnReferenceFinder = ColumnReferenceFinder()
        # Note: We set recurse=False so we only check the expressions in the
        # current node.
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
    ) -> tuple[Relational, set[CorrelatedReference]]:
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
        if isinstance(node, Aggregate):
            # Avoid pruning keys from an aggregate node. In the future we may
            # want to decouple the keys from the columns so not all keys need to
            # be present in the output.
            required_columns = set(node.keys.keys())
        else:
            required_columns = set()
        columns = {
            name: expr
            for name, expr in node.columns.items()
            if name in kept_columns or name in required_columns
        }
        # Update the columns.
        new_node = node.copy(columns=columns)
        self._dispatcher.reset()
        # Visit the current identifiers.
        new_node.accept(self._dispatcher)
        found_identifiers: set[ColumnReference] = (
            self._column_finder.get_column_references()
        )
        # If the node is an aggregate but doesn't use any of the inputs
        # (e.g. a COUNT(*)), arbitrarily mark one of them as used.
        # TODO: (gh #196) optimize this functionality so it doesn't keep an
        # unnecessary column.
        if isinstance(node, Aggregate) and len(found_identifiers) == 0:
            arbitrary_column_name: str = min(node.input.columns)
            found_identifiers.add(
                ColumnReference(
                    arbitrary_column_name,
                    node.input.columns[arbitrary_column_name].data_type,
                )
            )
        # Determine which identifiers to pass to each input.
        new_inputs: list[Relational] = []
        # Note: The ColumnPruner should only be run when all input names are
        # still present in the columns.
        # Iterate over the inputs in reverse order so that the source of
        # correlated data is pruned last.
        correl_refs: set[CorrelatedReference] = set()
        for i, default_input_name in reversed(
            list(enumerate(new_node.default_input_aliases))
        ):
            s: set[str] = set()
            input_node: Relational = node.inputs[i]
            for identifier in found_identifiers:
                if identifier.input_name == default_input_name:
                    s.add(identifier.name)
            if isinstance(input_node, Join) and i == 0:
                for correl_ref in correl_refs:
                    if correl_ref.correl_name == input_node.correl_name:
                        s.add(correl_ref.name)
            new_input_node, new_correl_refs = self._prune_node_columns(input_node, s)
            correl_refs.update(new_correl_refs)
            new_inputs.append(new_input_node)
        new_inputs.reverse()
        # Determine the new node.
        output = new_node.copy(inputs=new_inputs)
        return self._prune_identity_project(output), correl_refs

    def prune_unused_columns(self, root: RelationalRoot) -> RelationalRoot:
        """
        Prune columns that are unused in each relational expression.

        Args:
            root (RelationalRoot): The tree root to prune columns from.

        Returns:
            RelationalRoot: The root after updating all inputs.
        """
        new_root, _ = self._prune_node_columns(root, set(root.columns.keys()))
        assert isinstance(new_root, RelationalRoot), "Expected a root node."
        return new_root
