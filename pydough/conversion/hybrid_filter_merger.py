"""
Logic to merge multiple subtrees in the hybrid tree into one if they are the
same except one of them has more filters than the other and is only used in
a COUNT aggregation, meaning the filter can be implemented by doing a SUM on
the less-filtered subtree where the SUM argument is the additional filters.
"""

import copy
from typing import TYPE_CHECKING

import pydough.pydough_operators as pydop
from pydough.qdag import Literal
from pydough.types import BooleanType, NumericType

from .hybrid_connection import ConnectionType
from .hybrid_expressions import (
    HybridChildRefExpr,
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    make_condition,
)
from .hybrid_operations import (
    HybridCalculate,
    HybridFilter,
    HybridLimit,
)
from .hybrid_tree import HybridTree

if TYPE_CHECKING:
    from .hybrid_translator import HybridTranslator


class HybridFilterMerger:
    """
    TODO
    """

    def __init__(self, translator: "HybridTranslator") -> None:
        self.translator: HybridTranslator = translator

    def merge_filters(self, tree: HybridTree) -> None:
        """
        The main protocol that runs the filter merging procedure on the given
        tree with regards to its children, then recursively invokes the same
        procedure on the rest of the tree.
        """
        # Keep a set of all children that are marked for certain deletion.
        must_delete: set[int] = set()

        # Run the main procedure on subtrees with multiple children.
        if len(tree.children) > 1:
            # Identify which children are only used by a COUNT aggregation that
            # is not ONLY_MATCH.
            mergeable_children: set[int] = self.identify_mergeable_children(tree)

            # Extract the set of filters in the bottom level of each child tree,
            # only considering filters after a critical point (limits, windows).
            child_filters: list[set[HybridExpr]] = [
                self.get_final_filters(child.subtree) for child in tree.children
            ]

            # Obtain a mapping from each child to the set of all other children
            # in the tree that are isomorphic to it excluding filters after
            # the critical point.
            child_isomorphisms: list[set[int]] = self.get_child_isomorphisms(tree)

            # Create a DAG of each mergeable child to another child that it is
            # isomorphic to except that the other child has a strict subset of
            # its filters, if such a child exists, otherwise None. Path
            # compression is used to ensure there is no daisy chain.
            filter_dag: list[int | None] = self.make_filter_dag(
                mergeable_children, child_filters, child_isomorphisms
            )

            # Create a secondary mapping to indicate pools of children that were
            # not merged by the dag because there was no child with a filter
            # subset relationship, but are still isomorphic to one another.
            # These are stored in the form of a pool of isomorphic children,
            # where one member of the pool is the key and the rest are the
            # value.
            secondary_merges: dict[int, set[int]] = self.make_secondary_merges(
                mergeable_children, child_isomorphisms, filter_dag
            )

            # Build up a dictionary indicating all COUNT(*) references in the
            # tree that have been replaced with a SUM(cond) reference in a
            # different child of the tree.
            replacement_map: dict[HybridExpr, HybridExpr] = {}

            # For each pair (source -> target) in the filter DAG, run the
            # basic merging procedure.
            for source_idx, target_idx in enumerate(filter_dag):
                # Make sure the source maps to a target, as opposed to None.
                if target_idx is None:
                    continue
                # Identify all the filters in the source and not the child,
                # and vice versa. There should be at least 1 extra filter in
                # the source, but no extra filters in the target since it is
                # a subset relationship.
                extra_source_filters: set[HybridExpr] = (
                    child_filters[source_idx] - child_filters[target_idx]
                )
                extra_target_filters: set[HybridExpr] = (
                    child_filters[target_idx] - child_filters[source_idx]
                )
                assert len(extra_source_filters) > 0 and len(extra_target_filters) == 0
                # Run the merge subset filter procedure for this source and
                # target, updating the replacement map and deletion set.
                self.merge_subset_filters(
                    tree,
                    source_idx,
                    target_idx,
                    extra_source_filters,
                    replacement_map,
                    must_delete,
                )

            # For each (target <- source_pool), run the more advanced algorithm
            # which combines multiple children with distinct sets of filters.
            for target_idx, source_idxs in secondary_merges.items():
                self.merge_partial_disjoint_filters(
                    tree,
                    target_idx,
                    source_idxs,
                    child_filters,
                    replacement_map,
                    must_delete,
                )

            # Replace all of the COUNT(*) terms in the current tree from a
            # remapped child with the new SUM expression.
            for operation in tree.pipeline:
                operation.replace_expressions(replacement_map)

        # Before moving on, we need to remove any dead children from the tree.
        tree.remove_dead_children(must_delete)

        # Run the procedure recursively on the parent tree and the child
        # subtrees.
        if tree.parent is not None:
            self.merge_filters(tree.parent)
        for child in tree.children:
            self.merge_filters(child.subtree)

    def merge_subset_filters(
        self,
        tree: HybridTree,
        source_idx: int,
        target_idx: int,
        extra_source_filters: set[HybridExpr],
        replacement_map: dict[HybridExpr, HybridExpr],
        must_delete: set[int],
    ) -> None:
        """
        Run the merging procedure on a source and target child where the source
        has a strict superset of filters compared to the target, and the same
        underlying aggregation structure, meaning the source can be merged into
        the target.

        Args:
            `tree`: The tree whose children are being merged.
            `source_idx`: The index of the source child that is being merged.
            `target_idx`: The index of the target child that is being merged
            into.
            `extra_source_filters`: The set of filters in the source child that
            are not in the target child.
            `replacement_map`: A mapping that must be updated to indicate any
            references to the old source child and a new reference in the target
            child that the should be remapped to.
            `must_delete`: A set of child indices that must be updated to
            include the source child index, since it will be merged into the
            target and therefore removed.
        """
        # Build a new aggregation SUM(IFF(conj, 1, 0)) where conj is the
        # conjunction of all the extra filters from the source subtree.
        new_cond: HybridExpr = make_condition(extra_source_filters, True)
        numeric_expr: HybridExpr = HybridFunctionExpr(
            pydop.IFF,
            [
                new_cond,
                HybridLiteralExpr(Literal(1, NumericType())),
                HybridLiteralExpr(Literal(0, NumericType())),
            ],
            NumericType(),
        )
        sum_expr: HybridFunctionExpr = HybridFunctionExpr(
            pydop.SUM,
            [numeric_expr],
            NumericType(),
        )
        # Insert the new aggregation into the target subtree, and update the
        # replacement map to point from the old COUNT(*) reference in the source
        # subtree to the new SUM expression reference in the target subtree.
        agg_name: str = self.translator.gen_agg_name(tree.children[target_idx])
        tree.children[target_idx].aggs[agg_name] = sum_expr
        agg_ref: HybridExpr = HybridChildRefExpr(agg_name, target_idx, NumericType())
        old_agg_ref = HybridChildRefExpr(
            next(
                name
                for name, expr in tree.children[source_idx].aggs.items()
                if repr(expr) == "COUNT()"
            ),
            source_idx,
            NumericType(),
        )
        replacement_map[old_agg_ref] = agg_ref

        # Update the min/max steps of the target subtree to indicate overlap
        # with the source subtree.
        tree.children[target_idx].max_steps = min(
            tree.children[target_idx].max_steps,
            tree.children[source_idx].max_steps,
        )
        tree.children[target_idx].min_steps = min(
            tree.children[target_idx].min_steps,
            tree.children[source_idx].min_steps,
        )

        # Add a new filter for the extra conditions from the source
        # subtree if it was an ONLY_MATCH, checking whether the SUM
        # is not  zero, indicating that there was a match.
        if (
            tree.children[source_idx].connection_type
            == ConnectionType.AGGREGATION_ONLY_MATCH
        ):
            tree.add_operation(
                HybridFilter(
                    tree.pipeline[-1],
                    HybridFunctionExpr(
                        pydop.NEQ,
                        [agg_ref, HybridLiteralExpr(Literal(0, NumericType()))],
                        BooleanType(),
                    ),
                )
            )

        # Finally, mark the source child for deletion since it has now been
        # merged into the target child, unless they are the same child (see
        # the special case in `merge_partial_disjoint_filters`).
        if source_idx != target_idx:
            must_delete.add(source_idx)

    def merge_partial_disjoint_filters(
        self,
        tree: HybridTree,
        target_idx: int,
        source_idxs: set[int],
        all_filters: list[set[HybridExpr]],
        replacement_map: dict[HybridExpr, HybridExpr],
        must_delete: set[int],
    ) -> None:
        """
        Run the merging procedure on a pool of multiple source children that
        are isomorphic to the target child, but where there is no subset
        relationship. This is done by transforming the target child to have
        a disjunction of all the filters from the source children, then
        making all of the COUNT(*) calls from the different target/sources
        be on all the filters from that specific child that are not in all of
        the other children.

        Args:
            `tree`: The tree whose children are being merged.
            `target_idx`: The index of the target child that is being merged
            into.
            `source_idxs`: The set of indices of the source children that are
            being merged into the target.
            `all_filters`: A list of the sets of filters in each child subtree
            after the critical point.
            `replacement_map`: A mapping that must be updated to indicate any
            references to the old source child and a new reference in the target
            child that the should be remapped to.
            `must_delete`: A set of child indices that must be updated to
            include the source child indices, since they will be merged into the
            target and therefore removed.
        """
        # Identify any filters that are in all the children, since these can be
        # ignored when creating the new conditions for the aggregations.
        intersection = set.intersection(
            *(all_filters[source_idx] for source_idx in source_idxs),
            all_filters[target_idx],
        )

        # For each of the source children, merge it onto the target child as if
        # it were a subset merge.
        for source_idx in sorted(source_idxs):
            extra_source_filters: set[HybridExpr] = (
                all_filters[source_idx] - intersection
            )
            self.merge_subset_filters(
                tree,
                source_idx,
                target_idx,
                extra_source_filters,
                replacement_map,
                must_delete,
            )

        # Merge the target child onto itself using the extra filters it has, so
        # that its own COUNT(*) is replaced with a SUM over the filters that it
        # has that are not in all the other children.
        extra_target_filters: set[HybridExpr] = all_filters[target_idx] - intersection
        self.merge_subset_filters(
            tree,
            target_idx,
            target_idx,
            extra_target_filters,
            replacement_map,
            must_delete,
        )

        # Build up a list of the conjunctions for each source child, which will
        # be used to create the new disjunctive condition for the target
        # subtree.
        source_conjunctions: list[HybridExpr] = []
        for source_idx in sorted(source_idxs):
            source_cond: HybridExpr = make_condition(all_filters[source_idx], True)
            source_conjunctions.append(source_cond)

        # Build a disjunction of the conjunctions from the source children.
        new_disjunction: HybridExpr = make_condition(source_conjunctions, False)

        # Now go back through the target subtree, find any existing filters
        # after any window/limit, and make them a disjunction of the existing
        # filters and the disjunction of source conjunctions.
        for operation in reversed(tree.children[target_idx].subtree.pipeline):
            if isinstance(operation, HybridFilter):
                if operation.condition.contains_window_functions():
                    break
                operation.condition = HybridFunctionExpr(
                    pydop.BOR,
                    [operation.condition, new_disjunction],
                    BooleanType(),
                )
            elif isinstance(operation, HybridLimit):
                break
            elif isinstance(operation, HybridCalculate):
                if any(
                    expr.contains_window_functions()
                    for expr in operation.new_expressions.values()
                ):
                    break

    def identify_mergeable_children(self, tree: HybridTree) -> set[int]:
        """
        Identify the subset of child indices from a hybrid tree where the child
        is an aggregation where the only aggregate is a single COUNT(*).

        Args:
            `tree`: The tree whose children we are checking.

        Returns:
            A set of the indices of the children that are only used by a COUNT
            aggregation that is not ONLY_MATCH.
        """
        return {
            idx
            for idx, child in enumerate(tree.children)
            if (
                child.connection_type
                in (ConnectionType.AGGREGATION, ConnectionType.AGGREGATION_ONLY_MATCH)
                and {repr(v) for v in child.aggs.values()} == {"COUNT()"}
            )
        }

    def get_final_filters(self, tree: HybridTree) -> set[HybridExpr]:
        """
        Identify the set of all filter conditions that appear in the current
        tree's pipeline after any critical points (limits or window functions).

        Args:
            `tree`: The tree whose pipeline we are checking.

        Returns:
            The set of filters.
        """
        result: set[HybridExpr] = set()
        for operation in reversed(tree.pipeline):
            if isinstance(operation, HybridFilter):
                if operation.condition.contains_window_functions():
                    break
                result.update(operation.condition.get_conjunction())
            elif isinstance(operation, HybridLimit):
                break
            elif isinstance(operation, HybridCalculate):
                if any(
                    expr.contains_window_functions()
                    for expr in operation.new_expressions.values()
                ):
                    break
        return result

    def get_child_isomorphisms(self, tree: HybridTree) -> list[set[int]]:
        """
        Return a datastructure mapping each child index to the set of all other
        child indices that have the same canonical form after stripping away all
        filters after any critical points.

        Args:
            `tree`: The tree whose children we are checking.

        Returns:
            A list where the i'th element is the set of all other child indices
            that are isomorphic to the i'th child after stripping away all
            filters after any critical points.
        """
        # Extract the canonical forms
        filter_stripped_forms: list[str] = [
            self.get_filter_stripped_form(child.subtree) for child in tree.children
        ]
        result: list[set[int]] = []
        for i, form in enumerate(filter_stripped_forms):
            alternatives: set[int] = set()
            for j, other_form in enumerate(filter_stripped_forms):
                if i != j and form == other_form:
                    alternatives.add(j)
            result.append(alternatives)
        return result

    def get_filter_stripped_form(self, tree: HybridTree) -> str:
        """
        Create a canonical string representation of the tree structure for the
        hybrid tree after stripping away all filters after any critical
        points (limits or window functions). Also includes the join keys, so
        as to ensure that the canonical form reflects the same join
        conditions.

        Args:
            `tree`: The tree whose canonical form we are computing.

        Returns:
            The canonical form as a string.
        """
        # Make a clone of the tree
        stripped_tree = copy.deepcopy(tree)

        # Go backwards in the tree pipeline and remove all filters until
        # reaching a window function or limit.
        for idx, operation in reversed(list(enumerate(stripped_tree.pipeline))):
            if isinstance(operation, HybridFilter):
                if operation.condition.contains_window_functions():
                    break
                stripped_tree.pipeline.pop(idx)
            elif isinstance(operation, HybridLimit):
                break
            elif isinstance(operation, HybridCalculate):
                if any(
                    expr.contains_window_functions()
                    for expr in operation.new_expressions.values()
                ):
                    break

        # Return the string form of the transformed tree along with its
        # join keys.
        return repr(stripped_tree) + f" {stripped_tree.join_keys}"

    def make_filter_dag(
        self,
        mergeable_children: set[int],
        child_filters: list[set[HybridExpr]],
        child_isomorphisms: list[set[int]],
    ) -> list[int | None]:
        """
        Create a DAG mapping each child onto another child in the subtree such
        that the source child is mergeable, the target child has a subset of the
        filters of the source child, and the two children are isomorphic after
        stripping away filters. If no such mapping exists for a child, it maps to
        None. Path compression is used to ensure there are no daisy chains, so
        that if A maps to B and B maps to C, then A will map directly to C.

        Args:
            `mergeable_children`: The set of child indices that are mergeable.
            `child_filters`: A list of the sets of filters in each child subtree
            after the critical point.
            `child_isomorphisms`: A list where the i'th element is the set of all
            other child indices that are isomorphic to the i'th child after
            stripping away all filters after any critical points.

        Returns:
            A list where the i'th element is either None if there is no child
            that the i'th child can be merged into, or the index of a child that
            the i'th child can be merged into, meaning that the i'th child has a
            strict superset of filters compared to that child, and they are
            isomorphic after stripping away filters.
        """
        # Build up the initial DAG as all-None, then fill in as connections
        # are formed.
        dag: list[int | None] = [None for _ in range(len(child_filters))]

        # Build initial edges from each mergeable child to another isomorphic
        # child that is a subset of its filter list.
        for idx in mergeable_children:
            for other_idx in sorted(child_isomorphisms[idx]):
                if child_filters[other_idx] < child_filters[idx]:
                    dag[idx] = other_idx
                    break

        # Collapse transitive edges with path compression.
        for idx in range(len(dag)):
            if dag[idx] is not None:
                while True:
                    target_idx: int | None = dag[idx]
                    if target_idx is None or dag[target_idx] is None:
                        break
                    dag[idx] = dag[target_idx]
        return dag

    def make_secondary_merges(
        self,
        mergeable_children: set[int],
        child_isomorphisms: list[set[int]],
        filter_dag: list[int | None],
    ) -> dict[int, set[int]]:
        """
        Form the datastructure for the secondary merges, which is a mapping
        from a child index serving as a target, to a set of child indices
        serving as a pool of sources to merge into it, where the target and
        all sources must be mergeable, isomorphic, and not be used as a
        source or sink in the DAG.

        Args:
            `mergeable_children`: The set of child indices that are mergeable.
            `child_isomorphisms`: A list where the i'th element is the set of
            all other child indices that are isomorphic to the i'th child after
            stripping away all filters after any critical points.
            `filter_dag`: A list where the i'th element is either None if there
            is no child that the i'th child can be merged into, or the index of
            a child that the i'th child can be merged into, meaning that the
            i'th child has a strict superset of filters compared to that child,
            and they are isomorphic after stripping away filters.

        Returns:
            A mapping from a child index serving as a target, to a set of child
            indices serving as a pool of sources to merge into it, where the
            target and all sources must be mergeable, isomorphic, and not be
            used as a source or sink in the DAG.
        """
        secondary_merges: dict[int, set[int]] = {}

        # Form secondary edges between island nodes that are not subsets of
        # one another but where both of them are mergeable, and neither one is
        # the sink of an edge yet, or has been used as a source yet.
        existing_sinks: set[int | None] = set(filter_dag)
        already_merged: set[int] = set()
        for idx in mergeable_children:
            for other_idx in sorted(child_isomorphisms[idx]):
                if (
                    other_idx in mergeable_children
                    and filter_dag[idx] is None
                    and filter_dag[other_idx] is None
                    and idx not in existing_sinks
                    and other_idx not in existing_sinks
                    and other_idx not in secondary_merges
                    and other_idx not in already_merged
                ):
                    secondary_merges[idx] = secondary_merges.get(idx, set())
                    secondary_merges[idx].add(other_idx)
                    already_merged.add(other_idx)

        return secondary_merges
