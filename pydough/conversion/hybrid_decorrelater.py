"""
Logic for applying de-correlation to hybrid trees before relational conversion
if the correlate is not a semi/anti join.
"""

__all__ = ["HybridDecorrelater"]


import copy

import pydough.pydough_operators as pydop
from pydough.types import BooleanType

from .hybrid_connection import ConnectionType, HybridConnection
from .hybrid_expressions import (
    HybridBackRefExpr,
    HybridChildRefExpr,
    HybridColumnExpr,
    HybridCorrelExpr,
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridRefExpr,
    HybridWindowExpr,
)
from .hybrid_operations import (
    HybridCalculate,
    HybridChildPullUp,
    HybridFilter,
    HybridNoop,
    HybridPartition,
)
from .hybrid_tree import HybridTree


class HybridDecorrelater:
    """
    Class that encapsulates the logic used for de-correlation of hybrid trees.
    """

    def __init__(self) -> None:
        self.stack: list[HybridTree] = []
        self.children_indices: list[int] = []

    def make_decorrelate_parent(
        self, hybrid: HybridTree, child_idx: int, max_steps: int
    ) -> tuple[HybridTree, int, int]:
        """
        Creates a snapshot of the ancestry of the hybrid tree that contains
        a correlated child, without any of its children, its descendants, or
        any pipeline operators that do not need to be there.

        Args:
            `hybrid`: The hybrid tree to create a snapshot of in order to aid
            in the de-correlation of a correlated child.
            `child_idx`: The index of the correlated child of hybrid that the
            snapshot is being created to aid in the de-correlation of.
            `max_steps`: The index of the first pipeline operator that cannot
            occur because it depends on the correlated child.

        Returns:
            A tuple where the first entry is a snapshot of `hybrid` and its
            ancestry in the hybrid tree, without without any of its children or
            pipeline operators that occur during or after the derivation of the
            correlated child, or without any of its descendants. The second
            entry is the number of ancestor layers that should be skipped due
            to the PARTITION edge case. The third entry is how many operators
            in the pipeline were copied over from the root.
        """
        if (
            isinstance(hybrid.pipeline[0], HybridPartition)
            and hybrid.children[child_idx].max_steps == 1
        ):
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
                hybrid.parent,
                -1,
                len(hybrid.parent.pipeline),
            )
            return result[0], result[1] + 1, 1
        # Temporarily detach the successor of the current level, then create a
        # deep copy of the current level (which will include its ancestors),
        # then reattach the successor back to the original. This ensures that
        # the descendants of the current level are not included when providing
        # the parent to the correlated child as its new ancestor.
        successor: HybridTree | None = hybrid.successor
        hybrid._successor = None
        new_hybrid: HybridTree = copy.deepcopy(hybrid)
        hybrid._successor = successor
        # Ensure the new parent only includes the children and pipeline
        # operators that are required to exist first.
        new_children: list[HybridConnection] = []
        original_max_steps: int = max_steps
        new_correlated_children: set[int] = set()
        for idx, child in enumerate(new_hybrid.children):
            if child.max_steps < original_max_steps:
                new_children.append(child)
                if idx in hybrid.correlated_children:
                    new_correlated_children.add(len(new_children) - 1)
            else:
                max_steps = min(max_steps, child.max_steps)
        new_hybrid._correlated_children = new_correlated_children
        new_hybrid._children = new_children
        new_hybrid._pipeline = new_hybrid._pipeline[:max_steps]
        return new_hybrid, 0, max_steps

    def remove_correl_refs(
        self,
        expr: HybridExpr,
        parent: HybridTree,
        child_height: int,
        correl_level: int,
        new_parent_uni_keys: list[HybridExpr],
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
            `correl_level`: The level of correlation nesting required for the
            correlated reference to be removed. This is used to ensure that
            only references that are at the specified level of correlation
            are removed, and all others are left intact.
            `new_parent_uni_keys`: The unique keys of the new parent with
            regards to the current level. This is used to augment window calls
            that require new partition keys to remain correct.

        Returns:
            The hybrid expression with all correlated references to `parent`
            replaced with corresponding BACK references. The replacement also
            happens in-place.
        """
        match expr:
            case HybridCorrelExpr():
                # Unwrap the correlated expression to get the expression it
                # refers to (and shift it back to account for the fact that
                # the expression it points to is now above it in the hybrid
                # tree) but only if the correlated expression has enough
                # layers of correlation nesting to indicate that it refers to
                # the level of correlation that we are trying to remove.
                if expr.count_correlated_levels() >= correl_level:
                    return expr.expr.shift_back(child_height)
                else:
                    return expr
            case HybridFunctionExpr():
                # For regular functions, recursively transform all of their
                # arguments.
                for idx, arg in enumerate(expr.args):
                    expr.args[idx] = self.remove_correl_refs(
                        arg, parent, child_height, correl_level, new_parent_uni_keys
                    )
                return expr
            case HybridWindowExpr():
                # For window functions, recursively transform all of their
                # arguments, partition keys, and order keys.
                for idx, arg in enumerate(expr.args):
                    expr.args[idx] = self.remove_correl_refs(
                        arg, parent, child_height, correl_level, new_parent_uni_keys
                    )
                req_aug: bool = True
                for idx, arg in enumerate(expr.partition_args):
                    if (
                        expr.partition_args[idx].count_correlated_levels()
                        >= correl_level
                    ):
                        req_aug = False
                    expr.partition_args[idx] = self.remove_correl_refs(
                        arg, parent, child_height, correl_level, new_parent_uni_keys
                    )
                for order_arg in expr.order_args:
                    order_arg.expr = self.remove_correl_refs(
                        order_arg.expr,
                        parent,
                        child_height,
                        correl_level,
                        new_parent_uni_keys,
                    )
                # Augment the partition arguments, if needed (unless the
                # partition args already contains correlated references)
                if req_aug:
                    expr.partition_args.extend(
                        [expr.shift_back(child_height) for expr in new_parent_uni_keys]
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
        correl_level: int,
        new_parent_uni_keys: list[HybridExpr],
        top_level: bool = True,
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
            correlated reference with a BACK.
            `correl_level`: The level of correlation nesting required for the
            correlated reference to be removed. This is used to ensure that
            only references that are at the specified level of correlation
            nesting are removed, and all others are left intact.
            `new_parent_uni_keys`: The unique keys of the new parent with
            regards to the current level. This is used to augment window calls
            that require new partition keys to remain correct.
            `top_level`: Whether this is the top level of the hybrid tree that
            is being de-correlated.
        """
        while level is not None and level is not new_parent:
            # First, recursively remove any targeted correlated references from
            # the children of the current level.
            for child in level.children:
                self.correl_ref_purge(
                    child.subtree,
                    old_parent,
                    new_parent,
                    child_height,
                    correl_level + 1,
                    [],
                    top_level=False,
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
                        expr,
                        old_parent,
                        child_height,
                        correl_level,
                        new_parent_uni_keys,
                    )
                for ordering in operation.orderings:
                    ordering.expr = self.remove_correl_refs(
                        ordering.expr,
                        old_parent,
                        child_height,
                        correl_level,
                        new_parent_uni_keys,
                    )
                for idx, expr in enumerate(operation.unique_exprs):
                    operation.unique_exprs[idx] = self.remove_correl_refs(
                        operation.unique_exprs[idx],
                        old_parent,
                        child_height,
                        correl_level,
                        new_parent_uni_keys,
                    )
                if isinstance(operation, HybridCalculate):
                    for name, expr in operation.new_expressions.items():
                        operation.new_expressions[name] = operation.terms[name]
                if isinstance(operation, HybridFilter):
                    operation.condition = self.remove_correl_refs(
                        operation.condition,
                        old_parent,
                        child_height,
                        correl_level,
                        new_parent_uni_keys,
                    )
            # Repeat the process on the ancestor until either loop guard
            # condition is no longer True. Only update the child height if we
            # are still making steps from the original tree, as opposed to from
            # inside a nested child.
            level = level.parent
            if top_level:
                child_height -= 1

    def decorrelate_child(
        self,
        old_parent: HybridTree,
        child_idx: int,
        new_parent: HybridTree,
        skipped_levels: int,
        preserved_steps: int,
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
            `preserved_steps`: The number of pipeline operators from old parent
            that were copied over into the new parent.

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
        original_join_keys: list[tuple[HybridExpr, HybridExpr]] | None = (
            child.subtree.join_keys
        )
        original_general_join: HybridExpr | None = child.subtree.general_join_condition
        new_parent.add_successor(child_root)
        # Update the join keys to join on the unique keys of all the ancestors,
        # and the aggregation keys along with them.
        new_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        additional_levels: int = 0
        current_level: HybridTree | None = old_parent
        parent_agg_keys: list[HybridExpr] = []
        new_agg_keys: list[HybridExpr] = []
        rhs_shift: int = child_height - skipped_levels
        while current_level is not None:
            partition_edge_case: bool = (
                isinstance(current_level.pipeline[0], HybridPartition)
                and child is current_level.children[0]
            )
            for unique_key in sorted(current_level.pipeline[-1].unique_exprs, key=str):
                lhs_key: HybridExpr = unique_key.shift_back(additional_levels)
                rhs_key: HybridExpr = (
                    lhs_key if partition_edge_case else lhs_key.shift_back(rhs_shift)
                )
                new_join_keys.append((lhs_key, rhs_key))
                new_agg_keys.append(rhs_key)
                parent_agg_keys.append(lhs_key)
            current_level = current_level.parent
            additional_levels += 1

        # Copy over all existing join/general conditions into a new filter at
        # the bottom of the child subtree, in case any new filters were pushed
        # into those join connections that are now being deleted by the use of
        # a new parent link.
        new_conds: list[HybridExpr] = []
        if original_join_keys is not None:
            for lhs_key, rhs_key in original_join_keys:
                if (lhs_key, rhs_key) in new_join_keys:
                    continue
                new_conds.append(
                    HybridFunctionExpr(
                        pydop.EQU,
                        [lhs_key.shift_back(rhs_shift), rhs_key],
                        BooleanType(),
                    )
                )
        if original_general_join is not None:
            new_conds.append(original_general_join.expand_sided(rhs_shift))
        if len(new_conds) > 0:
            conjunction: HybridExpr
            if len(new_conds) == 1:
                conjunction = new_conds[0]
            else:
                conjunction = HybridFunctionExpr(pydop.BAN, new_conds, BooleanType())
            child.subtree.add_operation(
                HybridFilter(child.subtree.pipeline[-1], conjunction)
            )

        # Replace the original parent link with the new one using the uniqueness
        # keys of the parent to link it to the de-correlated child.
        child.subtree.join_keys = new_join_keys
        child.subtree.general_join_condition = None
        # Replace any correlated references to the original parent with BACK references.
        self.correl_ref_purge(
            child.subtree,
            old_parent,
            new_parent,
            child_height - skipped_levels,
            1,
            parent_agg_keys,
        )
        # If aggregating, update the aggregation keys accordingly.
        is_faux_agg: bool = (
            child.connection_type in (ConnectionType.SEMI, ConnectionType.ANTI)
            and not child.subtree.is_singular()
        )
        if child.connection_type.is_aggregation or is_faux_agg:
            child.subtree.agg_keys = new_agg_keys
        # If the child is such that we don't need to keep rows from the parent
        # without a match, replace the parent & its ancestors with a
        # HybridPullUp node (and replace any other deleted nodes with no-ops).
        # This is done in-place, but only if the child is the first child of
        # the parent.
        if child.connection_type.is_semi and child_idx == min(
            old_parent.correlated_children
        ):
            if child.connection_type == ConnectionType.SEMI:
                child.connection_type = (
                    ConnectionType.AGGREGATION_ONLY_MATCH
                    if is_faux_agg
                    else ConnectionType.SINGULAR_ONLY_MATCH
                )
            old_parent._parent = None
            old_parent.pipeline[0] = HybridChildPullUp(
                old_parent, child_idx, child_height - skipped_levels
            )
            for i in range(1, preserved_steps):
                old_parent.pipeline[i] = HybridNoop(old_parent.pipeline[i - 1])
            must_remove: set[int] = set()
            for idx, other_child in enumerate(old_parent.children):
                if other_child.max_steps < child.max_steps:
                    must_remove.add(idx)
            child_remapping: dict[int, int] = old_parent.remove_dead_children(
                must_remove
            )
            child_idx = child_remapping[child_idx]
        # Mark the child as no longer correlated, for printing purposes
        old_parent.correlated_children.discard(child_idx)
        return child_idx

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
        original_parent: HybridTree | None = None
        while child_idx >= 0:
            child = hybrid.children[child_idx]
            if child_idx not in hybrid.correlated_children:
                child_idx -= 1
                continue
            match child.connection_type:
                case (
                    ConnectionType.SINGULAR
                    | ConnectionType.AGGREGATION
                    | ConnectionType.SEMI
                    | ConnectionType.AGGREGATION_ONLY_MATCH
                    | ConnectionType.SINGULAR_ONLY_MATCH
                    | ConnectionType.ANTI
                    | ConnectionType.NO_MATCH_SINGULAR
                    | ConnectionType.NO_MATCH_AGGREGATION
                    | ConnectionType.NO_MATCH_NDISTINCT
                ):
                    if original_parent is None:
                        original_parent = copy.deepcopy(hybrid)
                    new_parent, skipped_levels, preserved_steps = (
                        self.make_decorrelate_parent(
                            original_parent,
                            child_idx,
                            hybrid.children[child_idx].max_steps,
                        )
                    )
                    child_idx = self.decorrelate_child(
                        hybrid,
                        child_idx,
                        new_parent,
                        skipped_levels,
                        preserved_steps,
                    )
                case (
                    ConnectionType.NDISTINCT
                    | ConnectionType.NDISTINCT_ONLY_MATCH
                    | ConnectionType.NO_MATCH_SINGULAR
                ):
                    raise NotImplementedError(
                        f"PyDough does not yet support correlated references with the {child.connection_type.name} pattern."
                    )
            child_idx -= 1
        # Iterate across all the children and recursively decorrelate them.
        for child in hybrid.children:
            child.subtree = self.decorrelate_hybrid_tree(child.subtree)
        return hybrid

    def find_correlated_children(self, hybrid: HybridTree) -> None:
        """
        Recursively finds all correlated children of a hybrid tree and stores
        them in the hybrid tree.

        Args:
            `hybrid`: The hybrid tree to find correlated children in.
        """
        correl_levels: int = 0
        for operation in hybrid.pipeline:
            if isinstance(operation, HybridCalculate):
                for term in operation.new_expressions.values():
                    correl_levels = max(correl_levels, term.count_correlated_levels())
            if isinstance(operation, HybridFilter):
                correl_levels = max(
                    correl_levels, operation.condition.count_correlated_levels()
                )

        assert correl_levels <= len(self.stack)
        for i in range(-1, -correl_levels - 1, -1):
            self.stack[i].correlated_children.add(self.children_indices[i])

        self.stack.append(hybrid)
        for idx, child in enumerate(hybrid.children):
            self.children_indices.append(idx)
            self.find_correlated_children(child.subtree)
            self.children_indices.pop()
        self.stack.pop()
        if hybrid.parent is not None:
            self.find_correlated_children(hybrid.parent)
