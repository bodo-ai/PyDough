"""
The definitions of the hybrid classes used as an intermediary representation
during QDAG to Relational conversion, as well as the conversion logic from QDAG
nodes to said hybrid nodes.

Definition of the HybridTree class, used as a intermediary representation
between QDAG nodes and the relational tree structure. Each hybrid tree can be
linked to other hybrid trees in a parent-successor chain, contains a pipeline
of 1+ hybrid operations, and can have a list of children which are hybrid
connections pointing to other hybrid trees.
"""

__all__ = ["HybridTree"]

from typing import Optional

import pydough.pydough_operators as pydop
from pydough.metadata import (
    SubcollectionRelationshipMetadata,
)
from pydough.qdag import (
    Literal,
    SubCollection,
    TableCollection,
)
from pydough.types import BooleanType, NumericType

from .hybrid_connection import ConnectionType, HybridConnection
from .hybrid_expressions import (
    HybridChildRefExpr,
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
    HybridCollectionAccess,
    HybridFilter,
    HybridLimit,
    HybridNoop,
    HybridOperation,
    HybridPartition,
    HybridPartitionChild,
    HybridRoot,
)


class HybridTree:
    """
    The datastructure class used to keep track of the overall computation in
    a tree structure where each level has a pipeline of operations, possibly
    has a singular predecessor and/or successor, and can have children that
    the operations in the pipeline can access.
    """

    def __init__(
        self,
        root_operation: HybridOperation,
        ancestral_mapping: dict[str, int],
        is_hidden_level: bool = False,
        is_connection_root: bool = False,
    ):
        self._pipeline: list[HybridOperation] = [root_operation]
        self._children: list[HybridConnection] = []
        self._ancestral_mapping: dict[str, int] = dict(ancestral_mapping)
        self._successor: HybridTree | None = None
        self._parent: HybridTree | None = None
        self._is_hidden_level: bool = is_hidden_level
        self._is_connection_root: bool = is_connection_root
        self._agg_keys: list[HybridExpr] | None = None
        self._join_keys: list[tuple[HybridExpr, HybridExpr]] | None = None
        self._general_join_condition: HybridExpr | None = None
        self._correlated_children: set[int] = set()
        self._blocking_idx: int = 0
        if isinstance(root_operation, HybridPartition):
            self._join_keys = []

    def to_string(self, verbose: bool = False) -> str:
        """
        TODO
        """
        lines = []
        if self.parent is not None:
            lines.extend(self.parent.to_string(verbose).splitlines())
        lines.append(
            " -> ".join(
                repr(operation)
                for operation in self.pipeline
                if (verbose or not operation.is_hidden)
            )
        )
        prefix = " " if self.successor is None else "↓"
        for idx, child in enumerate(self.children):
            lines.append(f"{prefix} child #{idx} ({child.connection_type.name}):")
            if verbose:
                lines.append(
                    f"{prefix}  definition range: ({child.min_steps}, {child.max_steps})"
                )
            if child.connection_type.is_aggregation:
                if child.subtree.agg_keys is not None:
                    lines.append(f"{prefix}  aggregate: {child.subtree.agg_keys}")
                if len(child.aggs):
                    lines.append(f"{prefix}  aggs: {child.aggs}:")
            if child.subtree.join_keys is not None:
                lines.append(f"{prefix}  join: {child.subtree.join_keys}")
            if child.subtree.general_join_condition is not None:
                lines.append(f"{prefix}  join: {child.subtree.general_join_condition}")
            for line in repr(child.subtree).splitlines():
                lines.append(f"{prefix} {line}")
        return "\n".join(lines)

    def __repr__(self):
        return self.to_string(True)

    def __eq__(self, other):
        return type(self) is type(other) and self.to_string(False) == other.to_string(
            False
        )

    @property
    def pipeline(self) -> list[HybridOperation]:
        """
        The sequence of operations done in the current level of the hybrid
        tree.
        """
        return self._pipeline

    @property
    def children(self) -> list[HybridConnection]:
        """
        The child operations evaluated so that they can be used by operations
        in the pipeline.
        """
        return self._children

    @property
    def ancestral_mapping(self) -> dict[str, int]:
        """
        The mapping used to identify terms that are references to an alias
        defined in an ancestor.
        """
        return self._ancestral_mapping

    @property
    def correlated_children(self) -> set[int]:
        """
        The set of indices of children that contain correlated references to
        the current hybrid tree.
        """
        return self._correlated_children

    @property
    def successor(self) -> Optional["HybridTree"]:
        """
        The next level below in the HybridTree, if present.
        """
        return self._successor

    @property
    def parent(self) -> Optional["HybridTree"]:
        """
        The previous level above in the HybridTree, if present.
        """
        return self._parent

    @property
    def is_hidden_level(self) -> bool:
        """
        True if the current level should be disregarded when converting
        PyDoughQDAG BACK terms to HybridExpr BACK terms.
        """
        return self._is_hidden_level

    @property
    def is_connection_root(self) -> bool:
        """
        True if the current level is the top of a subtree located inside of
        a HybridConnection.
        """
        return self._is_connection_root

    @property
    def agg_keys(self) -> list[HybridExpr] | None:
        """
        The list of keys used to aggregate this HybridTree relative to its
        ancestor, if it is the base of a HybridConnection.
        """
        return self._agg_keys

    @agg_keys.setter
    def agg_keys(self, agg_keys: list[HybridExpr]) -> None:
        """
        Assigns the aggregation keys to a hybrid tree.
        """
        self._agg_keys = agg_keys

    @property
    def join_keys(self) -> list[tuple[HybridExpr, HybridExpr]] | None:
        """
        The list of keys used to join this HybridTree relative to its
        ancestor, if it is the base of a HybridConnection.
        """
        return self._join_keys

    @join_keys.setter
    def join_keys(self, join_keys: list[tuple[HybridExpr, HybridExpr]]) -> None:
        """
        Assigns the join keys to a hybrid tree.
        """
        self._join_keys = join_keys

    @property
    def general_join_condition(self) -> HybridExpr | None:
        """
        A hybrid expression used as a general join condition joining this
        HybridTree to its ancestor, if it is the base of a HybridConnection.
        """
        return self._general_join_condition

    @general_join_condition.setter
    def general_join_condition(self, condition: HybridExpr) -> None:
        """
        Assigns the general join condition to a hybrid tree.
        """
        self._general_join_condition = condition

    def add_operation(self, operation: HybridOperation) -> None:
        """
        TODO
        """
        blocking_idx: int = len(self.pipeline)
        self.pipeline.append(operation)
        is_blocking_operation: bool = False
        if isinstance(operation, HybridCalculate):
            is_blocking_operation = any(
                term.contains_window_functions()
                for term in operation.new_expressions.values()
            )
        elif isinstance(operation, HybridFilter):
            is_blocking_operation = operation.condition.contains_window_functions()
        elif isinstance(operation, HybridLimit):
            is_blocking_operation = True
        if is_blocking_operation:
            self._blocking_idx = blocking_idx

    def insert_count_filter(self, child_idx: int) -> None:
        """
        TODO
        """
        hybrid_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.COUNT, [], NumericType()
        )
        child_connection: HybridConnection = self.children[child_idx]
        # If the aggregation already exists in the child, use a child reference
        # to it.
        agg_name: str
        if hybrid_call in child_connection.aggs.values():
            agg_name = child_connection.fetch_agg_name(hybrid_call)
        else:
            # Otherwise, Generate a unique name for the agg call to push into the
            # child connection.
            agg_idx: int = 0
            while True:
                agg_name = f"agg_{agg_idx}"
                if agg_name not in child_connection.aggs:
                    break
                agg_idx += 1
            child_connection.aggs[agg_name] = hybrid_call
        result_ref: HybridExpr = HybridChildRefExpr(agg_name, child_idx, NumericType())
        condition: HybridExpr = HybridFunctionExpr(
            pydop.GRT,
            [result_ref, HybridLiteralExpr(Literal(0, NumericType()))],
            BooleanType(),
        )
        self.add_operation(HybridFilter(self.pipeline[-1], condition))

    def get_correlate_names(self, levels: int) -> set[str]:
        """
        TODO
        """
        result: set[str] = set()
        for child in self.children:
            result.update(child.subtree.get_correlate_names(levels + 1))
        if self.parent is not None:
            result.update(self.parent.get_correlate_names(levels))
        for operation in self.pipeline:
            if isinstance(operation, HybridCalculate):
                for term in operation.new_expressions.values():
                    result.update(term.get_correlate_names(levels))
            elif isinstance(operation, HybridFilter):
                result.update(operation.condition.get_correlate_names(levels))
        return result

    def has_correlated_window_function(self, levels: int) -> bool:
        """
        TODO
        """
        for operation in self.pipeline:
            if isinstance(operation, HybridCalculate):
                for term in operation.new_expressions.values():
                    if term.has_correlated_window_function(levels):
                        return True
            elif isinstance(operation, HybridFilter):
                if operation.condition.has_correlated_window_function(levels):
                    return True
        for child in self.children:
            if child.subtree.has_correlated_window_function(levels + 1):
                return True
        return self.parent is not None and self.parent.has_correlated_window_function(
            levels
        )

    def add_child(
        self,
        child: "HybridTree",
        connection_type: ConnectionType,
        min_steps: int,
        max_steps: int,
    ) -> int:
        """
        Adds a new child operation to the current level so that operations in
        the pipeline can make use of it.

        Args:
            `child`: the subtree to be connected to `self` as a child
            (starting at the bottom of the subtree).
            `connection_type`: enum indicating what kind of connection is to be
            used to link `self` to `child`.
            `min_steps`: the index of the step in the pipeline that must
            be completed before the child can be defined.
            `max_steps`: the index of the step in the pipeline that the child
            must be defined before.

        Returns:
            The index of the newly inserted child (or the index of an existing
            child that matches it).
        """
        for idx, existing_connection in enumerate(self.children):
            if (
                child == existing_connection.subtree
                and (child.join_keys, child.general_join_condition)
                == (
                    existing_connection.subtree.join_keys,
                    existing_connection.subtree.general_join_condition,
                )
            ) or (
                idx == 0
                and isinstance(self.pipeline[0], HybridPartition)
                and (child.parent is None)
                and all(
                    operation in existing_connection.subtree.pipeline
                    for operation in child.pipeline[1:]
                )
                and all(
                    grandchild in existing_connection.subtree.children
                    for grandchild in child.children
                )
                and all(
                    (c1.subtree, c1.connection_type) == (c2.subtree, c2.connection_type)
                    for c1, c2 in zip(
                        child.children, existing_connection.subtree.children
                    )
                )
            ):
                # Skip if re-using the child would break the min/max bounds and
                # have filtering issues.
                if min_steps >= existing_connection.max_steps:
                    if connection_type.is_anti:
                        continue
                    if connection_type.is_semi:
                        if not (
                            child.always_exists()
                            or existing_connection.connection_type.is_semi
                        ):
                            # Special case: if adding a SEMI onto AGGREGATION,
                            # add a COUNT to the aggregation then add a filter
                            # to the parent tree on the count being positive.
                            if (
                                existing_connection.connection_type
                                == ConnectionType.AGGREGATION
                            ):
                                self.insert_count_filter(idx)
                                return idx
                            continue
                connection_type = connection_type.reconcile_connection_types(
                    existing_connection.connection_type
                )
                existing_connection.connection_type = connection_type
                if existing_connection.subtree.agg_keys is None:
                    existing_connection.subtree.agg_keys = child.agg_keys
                return idx
        connection: HybridConnection = HybridConnection(
            self, child, connection_type, min_steps, max_steps, {}
        )
        self._children.append(connection)
        return len(self.children) - 1

    def add_successor(self, successor: "HybridTree") -> None:
        """
        Marks two hybrid trees in a predecessor-successor relationship.

        Args:
            `successor`: the HybridTree to be marked as one level below `self`.
        """
        if self._successor is not None:
            raise Exception("Duplicate successor")
        self._successor = successor
        successor._parent = self
        # Shift the aggregation keys and rhs of join keys back by 1 level to
        # account for the fact that the successor must use the same aggregation
        # and join keys as `self`, but they have now become backreferences.
        # Do the same for the general join condition, if one is present.
        if self.agg_keys is not None:
            successor_agg_keys: list[HybridExpr] = []
            for key in self.agg_keys:
                successor_agg_keys.append(key.shift_back(1))
            successor.agg_keys = successor_agg_keys
        if self.join_keys is not None:
            successor_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
            for lhs_key, rhs_key in self.join_keys:
                successor_join_keys.append((lhs_key, rhs_key.shift_back(1)))
            successor.join_keys = successor_join_keys
        if self.general_join_condition is not None:
            successor.general_join_condition = self.general_join_condition.shift_back(1)

    def always_exists(self) -> bool:
        """
        Returns whether the hybrid tree & its ancestors always exist with
        regards to the parent context. This is true if all of the level
        changing operations (e.g. sub-collection accesses) are guaranteed to
        always have a match, and all other pipeline operations are guaranteed
        to not filter out any records.

        There is no need to check the children data (except for partitions &
        pull-ups) since the only way a child could cause the current context
        to reduce records is if there is a HAS/HASNOT somewhere, which
        would mean there is a filter in the pipeline.
        """
        # Verify that the first operation in the pipeline guarantees a match
        # with every record from the previous level (or parent context if it
        # is the top level)
        start_operation: HybridOperation = self.pipeline[0]
        match start_operation:
            case HybridRoot():
                return True
            case HybridCollectionAccess():
                if isinstance(start_operation.collection, TableCollection):
                    # Regular table collection accesses always exist.
                    pass
                else:
                    # Sub-collection accesses are only guaranteed to exist if
                    # the metadata property has `always matches` set to True.
                    assert isinstance(start_operation.collection, SubCollection)
                    meta: SubcollectionRelationshipMetadata = (
                        start_operation.collection.subcollection_property
                    )
                    if not meta.always_matches:
                        return False
            case HybridPartition():
                # For partition nodes, verify the data being partitioned always
                # exists.
                if not self.children[0].subtree.always_exists():
                    return False
            case HybridChildPullUp():
                # For pull-up nodes, make sure the data being pulled up always
                # exists.
                if not start_operation.child.subtree.always_exists():
                    return False
            case HybridPartitionChild():
                # Stepping into a partition child always has a matching data
                # record for each parent, by definition.
                pass
            case _:
                raise NotImplementedError(
                    f"Invalid start of pipeline: {start_operation.__class__.__name__}"
                )
        # Check the operations after the start of the pipeline, returning False if
        # there are any operations that could remove a row.
        for operation in self.pipeline[1:]:
            match operation:
                case HybridCalculate() | HybridNoop() | HybridRoot():
                    continue
                case HybridFilter():
                    if not operation.condition.always_true():
                        return False
                case HybridLimit():
                    return False
                case operation:
                    raise NotImplementedError(
                        f"Invalid intermediary pipeline operation: {operation.__class__.__name__}"
                    )

        for child in self.children:
            if child.connection_type.is_anti:
                return False
            if child.connection_type.is_semi and not child.subtree.always_exists():
                return False

        # The current level is fine, so check any levels above it next.
        return self.parent is None or self.parent.always_exists()

    def is_singular(self) -> bool:
        """
        TODO
        """
        match self.pipeline[0]:
            case HybridCollectionAccess():
                if isinstance(self.pipeline[0].collection, TableCollection):
                    pass
                else:
                    assert isinstance(self.pipeline[0].collection, SubCollection)
                    meta: SubcollectionRelationshipMetadata = self.pipeline[
                        0
                    ].collection.subcollection_property
                    if not meta.singular:
                        return False
            case HybridChildPullUp():
                if not self.children[self.pipeline[0].child_idx].subtree.is_singular():
                    return False
            case _:
                return False
        # The current level is fine, so check any levels above it next.
        return True if self.parent is None else self.parent.always_exists()

    def contains_correlates(self) -> bool:
        """
        TODO
        """
        for operation in self.pipeline:
            for expr in operation.terms.values():
                if expr.contains_correlates():
                    return True
            if (
                isinstance(operation, HybridFilter)
                and operation.condition.contains_correlates()
            ):
                return True
        if any(child.subtree.contains_correlates() for child in self.children):
            return True
        return self.parent is not None and self.parent.contains_correlates()

    def equalsIgnoringSuccessors(self, other: "HybridTree") -> bool:
        """
        Compares two hybrid trees without taking into account their
        successors.

        Args:
            `other`: the other HybridTree to compare to.

        Returns:
            True if the two trees are equal, False otherwise.
        """
        successor1: HybridTree | None = self.successor
        successor2: HybridTree | None = other.successor
        self._successor = None
        other._successor = None
        result: bool = self == other and (
            self.join_keys,
            self.general_join_condition,
        ) == (other.join_keys, other.general_join_condition)
        self._successor = successor1
        other._successor = successor2
        return result

    @staticmethod
    def identify_children_used(expr: HybridExpr, unused_children: set[int]) -> None:
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
                    HybridTree.identify_children_used(arg, unused_children)
            case HybridWindowExpr():
                for arg in expr.args:
                    HybridTree.identify_children_used(arg, unused_children)
                for part_arg in expr.partition_args:
                    HybridTree.identify_children_used(part_arg, unused_children)
                for order_arg in expr.order_args:
                    HybridTree.identify_children_used(order_arg.expr, unused_children)
            case HybridCorrelExpr():
                HybridTree.identify_children_used(expr.expr, unused_children)

    @staticmethod
    def renumber_children_indices(
        expr: HybridExpr, child_remapping: dict[int, int]
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
                    HybridTree.renumber_children_indices(arg, child_remapping)
            case HybridWindowExpr():
                for arg in expr.args:
                    HybridTree.renumber_children_indices(arg, child_remapping)
                for part_arg in expr.partition_args:
                    HybridTree.renumber_children_indices(part_arg, child_remapping)
                for order_arg in expr.order_args:
                    HybridTree.renumber_children_indices(
                        order_arg.expr, child_remapping
                    )
            case HybridCorrelExpr():
                HybridTree.renumber_children_indices(expr.expr, child_remapping)

    def remove_dead_children(self, must_remove: set[int]) -> dict[int, int]:
        """
        Deletes any children of a hybrid tree that are no longer referenced
        after de-correlation.

        Args:
            `must_remove`: the set of indices of children that must be removed
            if possible, even if their join type filters the current level.

        Returns:
            The mapping of children before vs after other children are deleted.
        """
        # Identify which children are no longer used
        children_to_delete: set[int] = set(range(len(self.children)))
        for operation in self.pipeline:
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

        for child_idx in range(len(self.children)):
            if child_idx in must_remove:
                continue
            if (
                self.children[child_idx].connection_type.is_semi
                and not self.children[child_idx].subtree.always_exists()
            ) or self.children[child_idx].connection_type.is_anti:
                children_to_delete.discard(child_idx)

        if len(children_to_delete) == 0:
            return {i: i for i in range(len(self.children))}

        # Build a renumbering of the remaining children
        child_remapping: dict[int, int] = {}
        for i in range(len(self.children)):
            if i not in children_to_delete:
                child_remapping[i] = len(child_remapping)

        # Remove all the unused children (starting from the end)
        for child_idx in sorted(children_to_delete, reverse=True):
            self.children.pop(child_idx)

        for operation in self.pipeline:
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

        return child_remapping

    def get_min_child_idx(self, child_subtree: "HybridTree") -> int:
        """
        TODO
        """
        correl_names: set[str] = child_subtree.get_correlate_names(1)
        has_correlated_window_function: bool = (
            child_subtree.has_correlated_window_function(1)
        )
        if correl_names:
            for pipeline_idx in range(len(self.pipeline) - 1, self._blocking_idx, -1):
                operation: HybridOperation = self.pipeline[pipeline_idx]
                if (
                    isinstance(operation, (HybridFilter, HybridLimit))
                    and has_correlated_window_function
                ):
                    return pipeline_idx
                if isinstance(operation, HybridCalculate):
                    for name in correl_names:
                        if name in operation.new_expressions:
                            term: HybridExpr = operation.new_expressions[name]
                            if not isinstance(term, HybridRefExpr) or term.name != name:
                                return pipeline_idx
        return self._blocking_idx

    def squish_backrefs_into_correl(
        self, levels_up: int | None, levels_out: int
    ) -> None:
        """
        TODO
        """
        for operation in self.pipeline:
            for term_name, term in operation.terms.items():
                operation.terms[term_name] = term.squish_backrefs_into_correl(
                    levels_up, levels_out
                )
            if isinstance(operation, HybridFilter):
                operation.condition = operation.condition.squish_backrefs_into_correl(
                    levels_up, levels_out
                )
            if isinstance(operation, HybridCalculate):
                for term_name, term in operation.new_expressions.items():
                    operation.new_expressions[term_name] = operation.terms[term_name]
        for child in self.children:
            child.subtree.squish_backrefs_into_correl(None, levels_out + 1)
        if self.parent is not None:
            self.parent.squish_backrefs_into_correl(levels_up, levels_out)
