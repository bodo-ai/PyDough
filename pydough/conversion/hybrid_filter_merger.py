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
        TODO
        """
        # Keep a set of all children that are marked for certain deletion.
        must_delete: set[int] = set()

        # Run the main procedure on subtrees with multiple children.
        if len(tree.children) > 1:
            # Identify which children are only used by a COUNT aggregation that is
            # not ONLY_MATCH.
            mergeable_children: set[int] = self.identify_mergeable_children(tree)

            # TODO ADD COMMENT
            child_filters: list[set[HybridExpr]] = [
                self.get_final_filters(child.subtree) for child in tree.children
            ]

            # TODO ADD COMMENT
            child_isomorphisms: list[set[int]] = self.get_child_isomorphisms(tree)

            # TODO ADD COMMENT
            filter_dag: list[int | None] = self.make_filter_dag(
                mergeable_children, child_filters, child_isomorphisms
            )

            # TODO ADD COMMENT
            replacement_map: dict[HybridExpr, HybridExpr] = {}
            for source_idx, target_idx in enumerate(filter_dag):
                if target_idx is None:
                    continue
                extra_source_filters: set[HybridExpr] = (
                    child_filters[source_idx] - child_filters[target_idx]
                )
                extra_target_filters: set[HybridExpr] = (
                    child_filters[target_idx] - child_filters[source_idx]
                )
                assert len(extra_source_filters) > 0
                if len(extra_target_filters) == 0:
                    self.merge_subset_filters(
                        tree,
                        source_idx,
                        target_idx,
                        extra_source_filters,
                        replacement_map,
                        must_delete,
                    )
                else:
                    self.merge_partial_disjoint_filters(
                        tree,
                        source_idx,
                        target_idx,
                        extra_source_filters,
                        extra_target_filters,
                        replacement_map,
                        must_delete,
                    )

            # TODO ADD COMMENT
            for operation in tree.pipeline:
                operation.replace_expressions(replacement_map)

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
        TODO
        """
        new_cond: HybridExpr
        if len(extra_source_filters) == 1:
            new_cond = next(iter(extra_source_filters))
        else:
            new_cond = HybridFunctionExpr(
                pydop.BAN,
                sorted(extra_source_filters, key=repr),
                BooleanType(),
            )
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

        if source_idx != target_idx:
            must_delete.add(source_idx)

    def merge_partial_disjoint_filters(
        self,
        tree: HybridTree,
        source_idx: int,
        target_idx: int,
        extra_source_filters: set[HybridExpr],
        extra_target_filters: set[HybridExpr],
        replacement_map: dict[HybridExpr, HybridExpr],
        must_delete: set[int],
    ) -> None:
        """
        TODO
        """
        # TODO ADD COMMENTS
        self.merge_subset_filters(
            tree,
            source_idx,
            target_idx,
            extra_source_filters,
            replacement_map,
            must_delete,
        )
        self.merge_subset_filters(
            tree,
            target_idx,
            target_idx,
            extra_target_filters,
            replacement_map,
            must_delete,
        )

        new_cond: HybridExpr
        if len(extra_source_filters) == 1:
            new_cond = next(iter(extra_source_filters))
        else:
            new_cond = HybridFunctionExpr(
                pydop.BAN,
                sorted(extra_source_filters, key=repr),
                BooleanType(),
            )

        # Now go back through the target subtree, find any existing filters
        # after any window/limit, and make them a disjunction of the existing
        # filter and the new filter conditions.
        for operation in reversed(tree.children[target_idx].subtree.pipeline):
            if isinstance(operation, HybridFilter):
                if operation.condition.contains_window_functions():
                    break
                operation.condition = HybridFunctionExpr(
                    pydop.BOR,
                    [operation.condition, new_cond],
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
        TODO
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
        TODO
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
        TODO
        """
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
        TODO
        """
        stripped_tree = copy.deepcopy(tree)
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
        return repr(stripped_tree)

    def make_filter_dag(
        self,
        mergeable_children: set[int],
        child_filters: list[set[HybridExpr]],
        child_isomorphisms: list[set[int]],
    ) -> list[int | None]:
        """
        TODO
        """
        dag: list[int | None] = [None for _ in range(len(child_filters))]
        # Build initial edges from each mergeable child to another isomorphic
        # child that is a subset of its filter list.
        for idx in mergeable_children:
            for other_idx in sorted(child_isomorphisms[idx]):
                if child_filters[other_idx] < child_filters[idx]:
                    dag[idx] = other_idx
                    break

        # Form secondary edges between island nodes that are not subsets of
        # one another but where both of them are mergeable, and neither one is
        # the sink of an edge yet.
        existing_sinks: set[int | None] = set(dag)
        for idx in mergeable_children:
            for other_idx in sorted(child_isomorphisms[idx]):
                if (
                    other_idx in mergeable_children
                    and dag[idx] is None
                    and dag[other_idx] is None
                    and idx not in existing_sinks
                    and other_idx not in existing_sinks
                ):
                    dag[idx] = other_idx
                    break

        # Collapse transitive edges
        for idx in range(len(dag)):
            if dag[idx] is not None:
                while True:
                    target_idx: int | None = dag[idx]
                    if target_idx is None or dag[target_idx] is None:
                        break
                    dag[idx] = dag[target_idx]
        return dag
