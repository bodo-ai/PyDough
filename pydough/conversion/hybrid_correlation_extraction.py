"""
Logic for pulling correlated filters at the bottom of a hybrid subtree up into
the join conditions of the hybrid connection, to eliminate correlation where
possible.
"""

__all__ = ["HybridCorrelationExtractor"]

from typing import TYPE_CHECKING

import pydough.pydough_operators as pydop
from pydough.qdag import Literal
from pydough.types import BooleanType

from .hybrid_connection import ConnectionType, HybridConnection
from .hybrid_expressions import (
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridSidedRefExpr,
)
from .hybrid_operations import (
    HybridCalculate,
    HybridFilter,
    HybridLimit,
    HybridOperation,
    HybridRefExpr,
)
from .hybrid_tree import HybridTree

if TYPE_CHECKING:
    from .hybrid_translator import HybridTranslator


class HybridCorrelationExtractor:
    """
    Class encapsulating the correlation extraction procedure for hybrid trees.
    """

    def __init__(self, translator: "HybridTranslator"):
        self.translator: HybridTranslator = translator

    def extract_equijoin_condition(
        self,
        condition: HybridExpr,
        new_equi_filters: list[tuple[HybridExpr, HybridExpr]],
        rhs_subtree: HybridTree,
        reserved_rhs_names: set[str],
        levels_from_bottom: int,
    ) -> bool:
        """
        Attempts to extract an equijoin condition from the given condition.
        If successful, appends the extracted condition to `new_equi_filters`
        and returns True. Otherwise, returns False.

        Args:
            `condition`: the condition to extract the equijoin from.
            `new_equi_filters`: a list to append the extracted equijoin
                conditions to.
            `rhs_subtree`: the hybrid subtree of the right-hand side of the
                connection.
            `reserved_rhs_names`: a set of names that are reserved for the
                right-hand side of the connection, to avoid name conflicts.
            `levels_from_bottom`: the number of levels from the bottom of the
                hybrid tree to the current subtree, used to determine the shifts
                required for the right-hand side expressions.

        Returns:
            True if an equijoin condition was extracted, False otherwise.
        """
        if (
            isinstance(condition, HybridFunctionExpr)
            and condition.operator == pydop.EQU
            and len(condition.args) == 2
        ):
            lhs_expr: HybridExpr
            rhs_expr: HybridExpr
            if (
                condition.args[0].count_correlated_levels() == 0
                and condition.args[1].count_correlated_levels() == 1
            ):
                lhs_expr = condition.args[1].strip_correl(False, 0)
                rhs_expr = condition.args[0]
            elif (
                condition.args[1].count_correlated_levels() == 0
                and condition.args[0].count_correlated_levels() == 1
            ):
                lhs_expr = condition.args[0].strip_correl(False, 0)
                rhs_expr = condition.args[1]
            else:
                return False
            if not isinstance(rhs_expr, HybridRefExpr):
                rhs_expr = self.translator.inject_expression(
                    rhs_subtree, rhs_expr, True
                )
                assert isinstance(rhs_expr, HybridRefExpr)
                reserved_rhs_names.add(rhs_expr.name)
            new_equi_filters.append((lhs_expr, rhs_expr.shift_back(levels_from_bottom)))
            return True
        return False

    def extract_general_condition(
        self,
        condition: HybridExpr,
        new_general_filters: list[HybridExpr],
        levels_from_bottom: int,
    ) -> bool:
        """
        Attempts to extract a general condition from the given condition.
        If successful, appends the extracted condition to `new_general_filters`
        and returns True. Otherwise, returns False.

        Args:
            `condition`: the condition to extract the general condition from.
            `new_general_filters`: a list to append the extracted general
                conditions to.
            `levels_from_bottom`: the number of levels from the bottom of the
                hybrid tree to the current subtree, used to determine the shifts
                required for the expressions.

        Returns:
            True if a general condition was extracted, False otherwise.
        """
        new_general_filters.append(
            condition.strip_correl(sided_ref=True, shift=levels_from_bottom)
        )
        return True

    def attempt_correlation_extraction(
        self, subtree: HybridTree, connection: HybridConnection, levels_from_bottom: int
    ) -> None:
        """
        Searches for any correlated references inside filters within the bottom
        level of the hybrid subtree of a connection, and attempts to move them
        to a join condition if possible, thus removing the correlation. The
        transformation is done in-place.

        Args:
            `subtree`: the hybrid subtree to attempt correlation extraction on.
            `connection`: the hybrid connection containing the subtree.
            `levels_from_bottom`: the number of levels from the bottom of the
                connection subtree to the current subtree, used to determine the
                shifts required for the right-hand side expressions.
        """
        bottom_subtree: HybridTree = connection.subtree
        is_equijoin: bool = bottom_subtree.general_join_condition is None
        non_aggregate: bool = not connection.connection_type.is_aggregation
        rhs_names: set[str] = set()
        rhs_names.update(connection.aggs)
        rhs_names.update(bottom_subtree.pipeline[-1].terms)

        # Identify the prefix of the pipeline that must be completed before
        # all the children are defined.
        min_idx: int = 0
        for child in subtree.children:
            min_idx = max(min_idx, child.max_steps)
        # Increase the min_idx to account for any steps that are a limit or
        # involve a window function, since filters can only be moved if they
        # occur after such operations.
        operation: HybridOperation
        for idx, operation in enumerate(subtree.pipeline):
            if (
                isinstance(operation, HybridLimit)
                or (
                    isinstance(operation, HybridCalculate)
                    and any(
                        expr.contains_window_functions()
                        for expr in operation.new_expressions.values()
                    )
                )
                or (
                    isinstance(operation, HybridFilter)
                    and operation.condition.contains_window_functions()
                )
            ):
                min_idx = max(min_idx, idx - 1)

        # Iterate through all of the operations in the pipeline, starting after
        # the min idx, looking for filters. For each such filter, break it up
        # into its conjunction and see if any of the components can be moved
        # into the join condition of the connection. If so, move it there and
        # replace that aspect of the conjunction with a True filter.
        for idx in range(min_idx + 1, len(subtree.pipeline)):
            operation = subtree.pipeline[idx]
            if (
                isinstance(operation, HybridFilter)
                and operation.condition.count_correlated_levels() > 0
            ):
                # Iterate through each term in the conjunction. If it can be
                # rewritten to pull out into the join condition, do so.
                # Otherwise, just place it in the new conjunction list for the
                # filter.
                conjunction: list[HybridExpr] = operation.condition.get_conjunction()
                new_conjunction: list[HybridExpr] = []
                new_equi_filters: list[tuple[HybridExpr, HybridExpr]] = []
                new_general_filters: list[HybridExpr] = []
                for cond in operation.condition.get_conjunction():
                    # Add the filter back to the original conjunction if it
                    # contains a window function, or contains no correlates, or
                    # has a correlation nesting level greater than 1. If none of
                    # those cases occur, see if any of the extraction patterns
                    # are successful, and if none of them are then add it back
                    # to the original conjunction. THe extraction patterns are
                    # pulling an equijoin condition into the join keys of an
                    # equijoin, or pulling an arbitrary condition into the
                    # general join condition of a non-equijoin.
                    if (
                        cond.contains_window_functions()
                        or cond.count_correlated_levels() != 1
                    ) or not (
                        (
                            is_equijoin
                            and self.extract_equijoin_condition(
                                cond,
                                new_equi_filters,
                                bottom_subtree,
                                rhs_names,
                                levels_from_bottom,
                            )
                        )
                        or (
                            non_aggregate
                            and not connection.connection_type.is_anti
                            and connection.connection_type != ConnectionType.SEMI
                            and self.extract_general_condition(
                                cond, new_general_filters, levels_from_bottom
                            )
                        )
                    ):
                        new_conjunction.append(cond)

                if len(new_equi_filters) > 0:
                    if bottom_subtree.join_keys is None:
                        bottom_subtree.join_keys = []
                    bottom_subtree.join_keys.extend(new_equi_filters)
                    if not non_aggregate:
                        assert bottom_subtree.agg_keys is not None
                        for _, rhs_key in new_equi_filters:
                            bottom_subtree.agg_keys.append(rhs_key)
                    connection.always_exists = False

                if len(new_general_filters) > 0:
                    if bottom_subtree.general_join_condition is not None:
                        new_general_filters.append(
                            bottom_subtree.general_join_condition
                        )
                    if bottom_subtree.join_keys is not None:
                        for lhs_key, rhs_key in bottom_subtree.join_keys:
                            new_general_filters.append(
                                HybridFunctionExpr(
                                    pydop.EQU,
                                    [HybridSidedRefExpr(lhs_key), rhs_key],
                                    BooleanType(),
                                )
                            )
                        bottom_subtree.join_keys = None
                        bottom_subtree.agg_keys = None
                    if len(new_general_filters) == 1:
                        bottom_subtree.general_join_condition = new_general_filters[0]
                    else:
                        bottom_subtree.general_join_condition = HybridFunctionExpr(
                            pydop.BAN, new_general_filters, BooleanType()
                        )
                    connection.always_exists = False

                # Update the filter condition with the new conjunction of terms
                if new_conjunction != conjunction:
                    if len(new_conjunction) == 0:
                        operation.condition = HybridLiteralExpr(
                            Literal(True, operation.condition.typ)
                        )
                    elif len(new_conjunction) == 1:
                        operation.condition = new_conjunction[0]
                    else:
                        operation.condition = HybridFunctionExpr(
                            pydop.BAN, new_conjunction, operation.condition.typ
                        )

    def correlation_extraction_traversal(
        self,
        hybrid: HybridTree,
        connection: HybridConnection | None,
        levels_from_bottom: int,
    ) -> None:
        """
        Main recursive traversal procedure for correlation extraction. The
        procedure traverses the hybrid tree in a bottom-up manner, attempting to
        extract correlated filters from child subtrees before processing the
        current hybrid subtree. The transformation is done in-place.

        Args:
            `hybrid`: the hybrid tree to process.
            `connection`: the hybrid connection containing the current hybrid
            subtree, or None if the current hybrid tree is at the root level.
            `levels_from_bottom`: the number of levels from the bottom of the
            connection subtree to the current subtree, used to determine the
            shifts required for the right-hand side expressions.
        """
        # First, recursively transform all of the children of the current tree,
        # transforming their subtrees relative to that child connection from the
        # bottom to the top.
        for child in hybrid.children:
            self.correlation_extraction_traversal(child.subtree, child, 0)

        # If we are inside a connection, attempt to extract correlated filters
        # from the current hybrid subtree level's pipeline into the connection.
        if connection is not None:
            self.attempt_correlation_extraction(hybrid, connection, levels_from_bottom)

        # If any of the operations in the current pipeline contain window
        # functions or limits, replace connection with None so that invocations
        # done on the parent levels within the hybrid tree do not attempt to
        # call attempt_correlation_extraction, since the filter cannot be pulled
        # out of the subtree without changing the behavior of the limit/window.
        for operation in hybrid.pipeline:
            if (
                (
                    isinstance(operation, HybridFilter)
                    and operation.condition.contains_window_functions()
                )
                or isinstance(operation, HybridLimit)
                or (
                    isinstance(operation, HybridCalculate)
                    and any(
                        expr.contains_window_functions()
                        for expr in operation.new_expressions.values()
                    )
                )
            ):
                connection = None

        # Recursively invoke the procedure on the parent of the hybrid tree.
        if hybrid.parent is not None:
            self.correlation_extraction_traversal(
                hybrid.parent, connection, levels_from_bottom + 1
            )

    def run_correlation_extraction(self, hybrid: HybridTree):
        """
        Run the correlation extraction procedure on the hybrid tree. The
        transformation is done in-place.

        Args:
            `hybrid`: the hybrid tree to run the correlation extraction on.
            The procedure is also run on ancestors and children of the tree.
        """
        self.correlation_extraction_traversal(hybrid, None, 0)
