"""
Logic to merge multiple subtrees in the hybrid tree into one if they are the
same except one of them has more filters than the other and is only used in
a COUNT aggregation, meaning the filter can be implemented by doing a SUM on
the less-filtered subtree where the SUM argument is the additional filters.
"""

import copy

import pydough.pydough_operators as pydop
from pydough.qdag import Literal
from pydough.types import BooleanType, NumericType

from .hybrid_connection import ConnectionType
from .hybrid_expressions import (
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


class HybridFilterMerger:
    """
    TODO
    """

    def merge_filters(self, tree: HybridTree) -> None:
        """
        TODO
        """
        # Run the main procedure on subtrees with multiple children.
        if len(tree.children) > 1:
            # Identify which children are only used by a COUNT aggregation that is
            # not ONLY_MATCH.
            mergeable_children: set[int] = self.identify_mergeable_children(tree)

            child_filters: list[set[HybridExpr]] = [
                self.get_final_filters(child.subtree) for child in tree.children
            ]

            child_isomorphisms: list[set[int]] = self.get_child_isomorphisms(tree)

            filter_dag: list[int | None] = self.make_filter_dag(
                mergeable_children, child_filters, child_isomorphisms
            )

            print()
            print(tree)
            print(mergeable_children)
            print(child_filters)
            print(child_isomorphisms)
            print(filter_dag)

            for source_idx, target_idx in enumerate(filter_dag):
                if target_idx is None:
                    continue
                print(source_idx, "->", target_idx)
                extra_filters: set[HybridExpr] = (
                    child_filters[source_idx] - child_filters[target_idx]
                )
                print(extra_filters)
                assert len(extra_filters) > 0
                new_cond: HybridExpr
                if len(extra_filters) == 1:
                    new_cond = next(iter(extra_filters))
                else:
                    new_cond = HybridFunctionExpr(
                        pydop.BAN,
                        sorted(extra_filters, key=repr),
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
                sum_expr: HybridExpr = HybridFunctionExpr(
                    pydop.SUM,
                    [numeric_expr],
                    BooleanType(),
                )
                print(sum_expr)
                # agg_name: str = tree.gen_agg_name(tree.children[target_idx])

        # Run the procedure recursively on the parent tree and the child
        # subtrees.
        if tree.parent is not None:
            self.merge_filters(tree.parent)
        for child in tree.children:
            self.merge_filters(child.subtree)

    def identify_mergeable_children(self, tree: HybridTree) -> set[int]:
        """
        TODO
        """
        return {
            idx
            for idx, child in enumerate(tree.children)
            if (
                child.connection_type == ConnectionType.AGGREGATION
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
                result.update(operation.condition.get_conjunction())
                if operation.condition.contains_window_functions():
                    break
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
                stripped_tree.pipeline.pop(idx)
                if operation.condition.contains_window_functions():
                    break
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
        for idx in mergeable_children:
            for other_idx in child_isomorphisms[idx]:
                if child_filters[other_idx] < child_filters[idx]:
                    dag[idx] = other_idx
                    break
        for idx in range(len(dag)):
            if dag[idx] is not None:
                while True:
                    target_idx: int | None = dag[idx]
                    if target_idx is not None:
                        dag[idx] = dag[target_idx]
        return dag
