"""
Logic for applying de-correlation to hybrid trees before relational conversion
if the correlate is not a semi/anti join.
"""

__all__ = ["run_hybrid_decorrelation"]


import copy

from .hybrid_tree import (
    ConnectionType,
    HybridBackRefExpr,
    HybridCalculate,
    HybridChildPullUp,
    HybridChildRefExpr,
    HybridColumnExpr,
    HybridConnection,
    HybridCorrelExpr,
    HybridExpr,
    HybridFilter,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridNoop,
    HybridPartition,
    HybridRefExpr,
    HybridTree,
    HybridWindowExpr,
)


class Decorrelater:
    """
    Class that encapsulates the logic used for de-correlation of hybrid trees.
    """

    def make_decorrelate_parent(
        self, hybrid: HybridTree, child_idx: int, required_steps: int
    ) -> tuple[HybridTree, int]:
        """
        Creates a snapshot of the ancestry of the hybrid tree that contains
        a correlated child, without any of its children, its descendants, or
        any pipeline operators that do not need to be there.

        Args:
            `hybrid`: The hybrid tree to create a snapshot of in order to aid
            in the de-correlation of a correlated child.
            `child_idx`: The index of the correlated child of hybrid that the
            snapshot is being created to aid in the de-correlation of.
            `required_steps`: The index of the last pipeline operator that
            needs to be included in the snapshot in order for the child to be
            derivable.

        Returns:
            A tuple where the first entry is a snapshot of `hybrid` and its
            ancestry in the hybrid tree, without without any of its children or
            pipeline operators that occur during or after the derivation of the
            correlated child, or without any of its descendants. The second
            entry is the number of ancestor layers that should be skipped due
            to the PARTITION edge case.
        """
        if isinstance(hybrid.pipeline[0], HybridPartition) and child_idx == 0:
            # Special case: if the correlated child is the data argument of a
            # partition operation, then the parent to snapshot is actually the
            # parent of the level containing the partition operation. In this
            # case, all of the parent's children & pipeline operators should be
            # included in the snapshot.
            if hybrid.parent is None:
                raise ValueError(
                    "Malformed hybrid tree: partition data input to a partition node cannot contain a correlated reference to the partition node."
                )
            result = self.make_decorrelate_parent(
                hybrid.parent, len(hybrid.parent.children), len(hybrid.pipeline)
            )
            return result[0], result[1] + 1
        # Temporarily detach the successor of the current level, then create a
        # deep copy of the current level (which will include its ancestors),
        # then reattach the successor back to the original. This ensures that
        # the descendants of the current level are not included when providing
        # the parent to the correlated child as its new ancestor.
        successor: HybridTree | None = hybrid.successor
        hybrid._successor = None
        new_hybrid: HybridTree = copy.deepcopy(hybrid)
        hybrid._successor = successor
        # Ensure the new parent only includes the children & pipeline operators
        # that is has to.
        new_hybrid._children = new_hybrid._children[:child_idx]
        new_hybrid._pipeline = new_hybrid._pipeline[: required_steps + 1]
        return new_hybrid, 0

    def remove_correl_refs(
        self, expr: HybridExpr, parent: HybridTree, child_height: int
    ) -> HybridExpr:
        """
        Recursively & destructively removes correlated references within a
        hybrid expression if they point to a specific correlated ancestor
        hybrid tree, and replaces them with corresponding BACK references.

        Args:
            `expr`: The hybrid expression to remove correlated references from.
            `parent`: The correlated ancestor hybrid tree that the correlated
            references should point to when they are targeted for removal.
            `child_height`: The height of the correlated child within the
            hybrid tree that the correlated references is point to. This is
            the number of BACK indices to shift by when replacing the
            correlated reference with a BACK reference.

        Returns:
            The hybrid expression with all correlated references to `parent`
            replaced with corresponding BACK references. The replacement also
            happens in-place.
        """
        match expr:
            case HybridCorrelExpr():
                # If the correlated reference points to the parent, then
                # replace it with a BACK reference. Otherwise, recursively
                # transform its input expression in case it contains another
                # correlated reference.
                if expr.hybrid is parent:
                    result: HybridExpr | None = expr.expr.shift_back(child_height)
                    assert result is not None
                    return result
                else:
                    expr.expr = self.remove_correl_refs(expr.expr, parent, child_height)
                    return expr
            case HybridFunctionExpr():
                # For regular functions, recursively transform all of their
                # arguments.
                for idx, arg in enumerate(expr.args):
                    expr.args[idx] = self.remove_correl_refs(arg, parent, child_height)
                return expr
            case HybridWindowExpr():
                # For window functions, recursively transform all of their
                # arguments, partition keys, and order keys.
                for idx, arg in enumerate(expr.args):
                    expr.args[idx] = self.remove_correl_refs(arg, parent, child_height)
                for idx, arg in enumerate(expr.partition_args):
                    expr.partition_args[idx] = self.remove_correl_refs(
                        arg, parent, child_height
                    )
                for order_arg in expr.order_args:
                    order_arg.expr = self.remove_correl_refs(
                        order_arg.expr, parent, child_height
                    )
                return expr
            case (
                HybridBackRefExpr()
                | HybridRefExpr()
                | HybridChildRefExpr()
                | HybridLiteralExpr()
                | HybridColumnExpr()
            ):
                # All other expression types do not require any transformation
                # to de-correlate since they cannot contain correlations.
                return expr
            case _:
                raise NotImplementedError(
                    f"Unsupported expression type: {expr.__class__.__name__}."
                )

    def correl_ref_purge(
        self,
        level: HybridTree | None,
        old_parent: HybridTree,
        new_parent: HybridTree,
        child_height: int,
    ) -> None:
        """
        The recursive procedure to remove correlated references from the
        expressions of a hybrid tree or any of its ancestors or children if
        they refer to a specific correlated ancestor that is being removed.

        Args:
            `level`: The current level of the hybrid tree to remove correlated
            references from.
            `old_parent`: The correlated ancestor hybrid tree that the correlated
            references should point to when they are targeted for removal.
            `new_parent`: The ancestor of `level` that removal should stop at
            because it is the transposed snapshot of `old_parent`, and
            therefore it & its ancestors cannot contain any more correlated
            references that would be targeted for removal.
            `child_height`: The height of the correlated child within the
            hybrid tree that the correlated references is point to. This is
            the number of BACK indices to shift by when replacing the
            correlated reference with a BACK
        """
        while level is not None and level is not new_parent:
            # First, recursively remove any targeted correlated references from
            # the children of the current level.
            for child in level.children:
                self.correl_ref_purge(
                    child.subtree, old_parent, new_parent, child_height
                )
            # Then, remove any correlated references from the pipeline
            # operators of the current level. Usually this just means
            # transforming the terms/orderings/unique keys of the operation,
            # but specific operation types will require special casing if they
            # have additional expressions stored in other field that need to be
            # transformed.
            for operation in level.pipeline:
                for name, expr in operation.terms.items():
                    operation.terms[name] = self.remove_correl_refs(
                        expr, old_parent, child_height
                    )
                for ordering in operation.orderings:
                    ordering.expr = self.remove_correl_refs(
                        ordering.expr, old_parent, child_height
                    )
                for idx, expr in enumerate(operation.unique_exprs):
                    operation.unique_exprs[idx] = self.remove_correl_refs(
                        expr, old_parent, child_height
                    )
                if isinstance(operation, HybridCalculate):
                    for str, expr in operation.new_expressions.items():
                        operation.new_expressions[str] = self.remove_correl_refs(
                            expr, old_parent, child_height
                        )
                if isinstance(operation, HybridFilter):
                    operation.condition = self.remove_correl_refs(
                        operation.condition, old_parent, child_height
                    )
            # Repeat the process on the ancestor until either loop guard
            # condition is no longer True.
            level = level.parent
            child_height -= 1

    def decorrelate_child(
        self,
        old_parent: HybridTree,
        child_idx: int,
        new_parent: HybridTree,
        skipped_levels: int,
    ) -> int:
        """
        Runs the logic to de-correlate a child of a hybrid tree that contains
        a correlated reference. This involves linking the child to a new parent
        as its ancestor, the parent being a snapshot of the original hybrid
        tree that contained the correlated child as a child. The transformed
        child can now replace correlated references with BACK references that
        point to terms in its newly expanded ancestry, and the original hybrid
        tree can now join onto this child using its uniqueness keys.

        Args:
            `old_parent`: The correlated ancestor hybrid tree that the correlated
            references should point to when they are targeted for removal.
            `child_idx`: Which child of the hybrid tree the child is.
            `new_parent`: The ancestor of `level` that removal should stop at.
            `skipped_levels`: The number of ancestor layers that should be
            ignored when deriving backshifts of join/agg keys.

        Returns:
            The index of the child that was de-correlated, which is usually
            the same as `child_idx` but could have been shifted.
        """
        # First, find the height of the child subtree & its top-most level.
        child: HybridConnection = old_parent.children[child_idx]
        child_root: HybridTree = child.subtree
        child_height: int = 1
        while child_root.parent is not None:
            child_height += 1
            child_root = child_root.parent
        # Link the top level of the child subtree to the new parent.
        new_parent.add_successor(child_root)
        # Replace any correlated references to the original parent with BACK references.
        self.correl_ref_purge(child.subtree, old_parent, new_parent, child_height)
        # Update the join keys to join on the unique keys of all the ancestors.
        new_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        additional_levels: int = 0
        current_level: HybridTree | None = old_parent
        new_agg_keys: list[HybridExpr] = []
        while current_level is not None:
            skip_join: bool = (
                isinstance(current_level.pipeline[0], HybridPartition)
                and child is current_level.children[0]
            )
            for unique_key in sorted(current_level.pipeline[-1].unique_exprs, key=str):
                lhs_key: HybridExpr | None = unique_key.shift_back(additional_levels)
                rhs_key: HybridExpr | None = unique_key.shift_back(
                    additional_levels + child_height - skipped_levels
                )
                assert lhs_key is not None and rhs_key is not None
                if not skip_join:
                    new_join_keys.append((lhs_key, rhs_key))
                new_agg_keys.append(rhs_key)
            current_level = current_level.parent
            additional_levels += 1
        child.subtree.join_keys = new_join_keys
        child.subtree.general_join_condition = None
        # If aggregating, update the aggregation keys accordingly.
        if child.connection_type.is_aggregation:
            child.subtree.agg_keys = new_agg_keys
        # If the child is such that we don't need to keep rows from the parent
        # without a match, replace the parent & its ancestors with a
        # HybridPullUp node (and replace any other deleted nodes with no-ops).
        # This is done in-place, but only if the child is the first child of
        # the parent.
        if child.connection_type.is_semi and child_idx == min(
            old_parent.correlated_children
        ):
            old_parent._parent = None
            old_parent.pipeline[0] = HybridChildPullUp(
                old_parent, child_idx, child_height
            )
            for i in range(1, child.required_steps + 1):
                old_parent.pipeline[i] = HybridNoop(old_parent.pipeline[i - 1])
            child_idx = self.remove_dead_children(old_parent, child_idx)
        return child_idx

    def identify_children_used(
        self, expr: HybridExpr, unused_children: set[int]
    ) -> None:
        """
        Find all child indices used in an expression and remove them from
        a set of indices.

        Args:
            `expr`: the expression being checked for child reference indices.
            `unused_children`: the set of all children that are unused. This
            starts out as the set of all children, and whenever a child
            reference is found within `expr`, it is removed from the set.
        """
        match expr:
            case HybridChildRefExpr():
                unused_children.discard(expr.child_idx)
            case HybridFunctionExpr():
                for arg in expr.args:
                    self.identify_children_used(arg, unused_children)
            case HybridWindowExpr():
                for arg in expr.args:
                    self.identify_children_used(arg, unused_children)
                for part_arg in expr.partition_args:
                    self.identify_children_used(part_arg, unused_children)
                for order_arg in expr.order_args:
                    self.identify_children_used(order_arg.expr, unused_children)
            case HybridCorrelExpr():
                self.identify_children_used(expr.expr, unused_children)

    def renumber_children_indices(
        self, expr: HybridExpr, child_remapping: dict[int, int]
    ) -> None:
        """
        Replaces all child reference indices in a hybrid expression in-place
        when the children list was shifted, therefore the index-to-child
        correspondence must be re-numbered.

        Args:
            `expr`: the expression having its child references modified.
            `child_remapping`: the mapping of old->new indices for child
            references.
        """
        match expr:
            case HybridChildRefExpr():
                assert expr.child_idx in child_remapping
                expr.child_idx = child_remapping[expr.child_idx]
            case HybridFunctionExpr():
                for arg in expr.args:
                    self.renumber_children_indices(arg, child_remapping)
            case HybridWindowExpr():
                for arg in expr.args:
                    self.renumber_children_indices(arg, child_remapping)
                for part_arg in expr.partition_args:
                    self.renumber_children_indices(part_arg, child_remapping)
                for order_arg in expr.order_args:
                    self.renumber_children_indices(order_arg.expr, child_remapping)
            case HybridCorrelExpr():
                self.renumber_children_indices(expr.expr, child_remapping)

    def remove_dead_children(self, hybrid: HybridTree, pullup_child_idx: int) -> int:
        """
        Deletes any children of a hybrid tree that are no longer referenced
        after de-correlation.

        Args:
            `hybrid`: The hybrid tree to remove unused children from.
            `pullup_child_idx`: The index of the child that became a pull-up
            node causing the removal.

        Returns:
            The index of the child that the pullup operation corresponds to.
        """
        # Identify which children are no longer used
        children_to_delete: set[int] = set(range(len(hybrid.children)))
        for operation in hybrid.pipeline:
            match operation:
                case HybridChildPullUp():
                    children_to_delete.discard(operation.child_idx)
                case HybridFilter():
                    self.identify_children_used(operation.condition, children_to_delete)
                case HybridCalculate():
                    for term in operation.new_expressions.values():
                        self.identify_children_used(term, children_to_delete)
                case _:
                    for term in operation.terms.values():
                        self.identify_children_used(term, children_to_delete)
        if len(children_to_delete) == 0:
            return pullup_child_idx
        # Build a renumbering of the remaining children
        child_remapping: dict[int, int] = {}
        for i in range(len(hybrid.children)):
            if i not in children_to_delete:
                child_remapping[i] = len(child_remapping)
        # Remove all the unused children (starting from the end)
        for child_idx in sorted(children_to_delete, reverse=True):
            hybrid.children.pop(child_idx)
        for operation in hybrid.pipeline:
            match operation:
                case HybridChildPullUp():
                    operation.child_idx = child_remapping[operation.child_idx]
                case HybridFilter():
                    self.renumber_children_indices(operation.condition, child_remapping)
                case HybridCalculate():
                    for term in operation.new_expressions.values():
                        self.renumber_children_indices(term, child_remapping)
                case _:
                    continue
        # Renumber the correlated children
        new_correlated_children: set[int] = set()
        for correlated_idx in hybrid.correlated_children:
            if correlated_idx in child_remapping:
                new_correlated_children.add(child_remapping[correlated_idx])
        hybrid._correlated_children = new_correlated_children

        return child_remapping[pullup_child_idx]

    def decorrelate_hybrid_tree(self, hybrid: HybridTree) -> HybridTree:
        """
        The recursive procedure to remove unwanted correlated references from
        the entire hybrid tree, called from the bottom and working upwards
        to the top layer, and having each layer also de-correlate its children.

        Args:
            `hybrid`: The hybrid tree to remove correlated references from.

        Returns:
            The hybrid tree with all invalid correlated references removed as the
            tree structure is re-written to allow them to be replaced with BACK
            references. The transformation is also done in-place.
        """
        # Recursively decorrelate the ancestors of the current level of the
        # hybrid tree.
        if hybrid.parent is not None:
            hybrid._parent = self.decorrelate_hybrid_tree(hybrid.parent)
            hybrid._parent._successor = hybrid
        # Iterate across all the children, identify any that are correlated,
        # and transform any of the correlated ones that require decorrelation
        # due to the type of connection.
        child_idx: int = len(hybrid.children) - 1
        original_parent: HybridTree
        if len(hybrid.correlated_children) > 0:
            original_parent = copy.deepcopy(hybrid)
        while child_idx >= 0:
            child = hybrid.children[child_idx]
            if child_idx not in hybrid.correlated_children:
                child_idx -= 1
                continue
            match child.connection_type:
                case (
                    ConnectionType.SINGULAR
                    | ConnectionType.SINGULAR_ONLY_MATCH
                    | ConnectionType.AGGREGATION
                    | ConnectionType.AGGREGATION_ONLY_MATCH
                ):
                    new_parent, skipped_levels = self.make_decorrelate_parent(
                        original_parent,
                        child_idx,
                        hybrid.children[child_idx].required_steps,
                    )
                    child_idx = self.decorrelate_child(
                        hybrid,
                        child_idx,
                        new_parent,
                        skipped_levels,
                    )
                case ConnectionType.NDISTINCT | ConnectionType.NDISTINCT_ONLY_MATCH:
                    raise NotImplementedError(
                        f"PyDough does not yet support correlated references with the {child.connection_type.name} pattern."
                    )
                case (
                    ConnectionType.SEMI
                    | ConnectionType.ANTI
                    | ConnectionType.NO_MATCH_SINGULAR
                    | ConnectionType.NO_MATCH_AGGREGATION
                    | ConnectionType.NO_MATCH_NDISTINCT
                ):
                    # These patterns do not require decorrelation since they
                    # are supported via correlated SEMI/ANTI joins.
                    pass
            child_idx -= 1
        # Iterate across all the children and recursively decorrelate them.
        for child in hybrid.children:
            child.subtree = self.decorrelate_hybrid_tree(child.subtree)
        return hybrid


def run_hybrid_decorrelation(hybrid: HybridTree) -> HybridTree:
    """
    Invokes the procedure to remove correlated references from a hybrid tree
    before relational conversion if those correlated references are invalid
    (e.g. not from a semi/anti join).

    Args:
        `hybrid`: The hybrid tree to remove correlated references from.

    Returns:
        The hybrid tree with all invalid correlated references removed as the
        tree structure is re-written to allow them to be replaced with BACK
        references. The transformation is also done in-place.
    """
    decorr: Decorrelater = Decorrelater()
    return decorr.decorrelate_hybrid_tree(hybrid)
