"""
The logic for running the hybrid syncretization optimization, which identifies
children of a hybrid tree with a common prefix and combines them, thus removing
duplicate logic and improving performance.
"""

__all__ = ["HybridSyncretizer"]


import copy
from typing import TYPE_CHECKING

import pydough.pydough_operators as pydop
from pydough.qdag import (
    Literal,
)
from pydough.types import BooleanType, NumericType

from .hybrid_connection import ConnectionType, HybridConnection
from .hybrid_expressions import (
    HybridChildRefExpr,
    HybridExpr,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridRefExpr,
)
from .hybrid_operations import (
    HybridChildPullUp,
    HybridFilter,
)
from .hybrid_tree import HybridTree

if TYPE_CHECKING:
    from .hybrid_translator import HybridTranslator


class HybridSyncretizer:
    """
    Encapsulated logic for syncretizing subtrees of a hybrid tree to avoid
    duplicate logic being computed more than once. The core idea is as follows:
    1. For every tree T, find all candidates (base, extension) such that base
       and extension are children of T and base is a prefix of extension. Add
       all such candidates to a list.
    2. Sort the candidates (base, extension) first by the difference in height
       (so those with a smaller difference are first), then by the total height
       of extension (so the largest children are first).
    3. Iterate through the sorted candidates and attempt to syncretize each
       pair in order. Each pair will require additional checks at this time
       since other attempts that occur first may make later attempts invalid
       (e.g. because base or extension has already been syncretized onto
       a different child).
    4. When attempting to syncretize, verify that the following are true:
        -
    """

    supported_syncretize_operators: dict[
        pydop.PyDoughExpressionOperator,
        tuple[pydop.PyDoughExpressionOperator, pydop.PyDoughExpressionOperator],
    ] = {
        pydop.COUNT: (pydop.COUNT, pydop.SUM),
        pydop.SUM: (pydop.SUM, pydop.SUM),
        pydop.MIN: (pydop.MIN, pydop.MIN),
        pydop.MAX: (pydop.MAX, pydop.MAX),
        pydop.ANYTHING: (pydop.ANYTHING, pydop.MAX),
    }
    """
    A mapping of operators that can be split up into two rounds of aggregation
    as part of an AGG-AGG syncretization. The key is the aggregation operator
    being used in the original extension child and the value is a tuple of two
    operators,  one of which should be used to aggregate the new extension
    child of the base child, and the other which should be used to aggregate
    the first aggregation within the base child.

    For example, if the parent has children C0 and C1, C0 has agg terms
    {"agg_0": COUNT()}, and C1 has agg terms {"agg_1": COUNT()}, then
    after syncretization, C0 will have a child with the suffix from C1
    (lets call this C2) with agg terms {"agg_2": COUNT()}, and C0 will
    now have the following: {"agg_0": COUNT(), "agg_1": SUM(agg_2)}.

    If an operator is not in this mapping then such a syncretization is not
    allowed because the aggregation cannot be split up into two rounds. An
    example of this is computing NDISTINCT or MEDIAN, since there is no
    aggregation that can be computed on the extension child such that the
    result can be re-aggregated to obtain the final result. An exception to
    this rule is AVG, which is specially handled since it can be decomposed
    into SUM and COUNT, both of which can be split.
    """

    def __init__(self, translator: "HybridTranslator") -> None:
        self.translator: HybridTranslator = translator

    def make_extension_child(
        self, child: HybridTree, levels_up: int, new_base: HybridTree
    ) -> HybridTree:
        """
        Creates the suffix of the extension child by splitting the subtree at
        the specified number of levels so its root is now the first level of
        the extension child that is not in the base child.

        Args:
            `child`: The extension child being split.
            `levels_up`: The number of levels above from the current child
            to split the tree at.
            `new_base`: The new base tree that the extension child
            will be a child of.

        Returns:
            A new HybridTree that is the section of the extension child that
            will become a child of the new base tree as a child.
        """
        if levels_up <= 0:
            raise ValueError(f"Cannot make extension child with {levels_up} levels up")

        # Clone just the current level, detached from any parent or successor.
        parent: HybridTree | None = child.parent
        successor: HybridTree | None = child.successor
        child._parent = None
        child._successor = None
        new_child: HybridTree = copy.deepcopy(child)
        new_child.squish_backrefs_into_correl(levels_up, 1)
        child._parent = parent
        child._successor = successor

        # If at the level where the split occurs, add root link information to
        # new_child since it is now the root of the new subtree.
        if levels_up == 1:
            self.translator.define_root_link(new_base, new_child, True)
        else:
            # Otherwise, recursively transform the parent of the current level,
            # then link the copy to the result as a successor.
            assert parent is not None
            new_parent: HybridTree = self.make_extension_child(
                parent, levels_up - 1, new_base
            )
            new_parent.add_successor(new_child)
        return new_child

    @staticmethod
    def can_syncretize_subtrees(
        base_child: HybridConnection, extension_child: HybridConnection
    ) -> tuple[bool, int]:
        """
        Returns whether two children of a hybrid tree form a pair
        (base, extension) where the base is a prefix of the extension.
        Importantly, even if this function returns True, it does not
        necessarily mean that the two children can or will be syncretized, as
        there are additional checks that need to be performed based on the
        connection types of the two children and the aggregation operators, and
        there may be better combinations of children that can be syncretized
        onto one another.

        Args:
            `base_child`: The base child of the pair.
            `extension_child`: The extension child of the pair.

        Returns:
            A tuple where:
            - The first element is a boolean indicating whether
               the two children can be potentially syncretized.
            - The second element is the number of additional levels in the
              suffix of the extension child that are not present in the base child.
        """
        prefix_levels_up: int = 0
        base_subtree: HybridTree = base_child.subtree
        crawl_subtree: HybridTree = extension_child.subtree
        while True:
            if base_subtree.equals_ignoring_successors(crawl_subtree):
                break
            if crawl_subtree.parent is None:
                return False, -1
            crawl_subtree = crawl_subtree.parent
            prefix_levels_up += 1
        return prefix_levels_up > 0, prefix_levels_up

    def add_extension_semi_anti_count_filter(
        self, tree: HybridTree, extension_idx: int, is_semi: bool
    ) -> None:
        """
        Inserts a filter into the pipeline of the tree containing the base and
        extension children so that the syncretized pair computes the COUNT of
        the extension child and the parent can perform a filter on that COUNT
        (> 0 for SEMI, == 0 for ANTI).
        This is done when syncretizing a child
        that only allows matches onto a base child that is an aggregation,
        since the extension child must not filter rows of the base child lest
        it tamper with the base child's aggregation results.

        This transformation is done before the syncretization is completed, so
        the rest of the syncretization logic will deal with the splitting of
        COUNT into a COUNT and a SUM.

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `extension_idx`: The index of the extension child in the tree.
            `is_semi`: If True, insert a filter to emulate a SEMI join;
                               If False, emulates an ANTI join.
        """

        # Create the COUNT call and insert it into the extension child.
        extension_child: HybridConnection = tree.children[extension_idx]
        agg_call: HybridFunctionExpr = HybridFunctionExpr(
            pydop.COUNT, [], NumericType()
        )
        agg_name: str
        if agg_call in extension_child.aggs.values():
            agg_name = extension_child.fetch_agg_name(agg_call)
        else:
            agg_name = self.translator.gen_agg_name(extension_child)
            extension_child.aggs[agg_name] = agg_call

        # Create the filter condition from the perspective of the parent tree.
        agg_ref: HybridExpr = HybridChildRefExpr(agg_name, extension_idx, NumericType())
        literal_zero: HybridExpr = HybridLiteralExpr(Literal(0, NumericType()))
        if not is_semi:
            agg_ref = HybridFunctionExpr(
                pydop.DEFAULT_TO, [agg_ref, literal_zero], BooleanType()
            )

        # Insert the new filter at the location before which the semi/anti filter must
        # be applied
        insert_idx: int = extension_child.max_steps
        tree.pipeline.insert(
            insert_idx,
            HybridFilter(
                tree.pipeline[-1],
                HybridFunctionExpr(
                    pydop.GRT if is_semi else pydop.EQU,
                    [agg_ref, literal_zero],
                    BooleanType(),
                ),
            ),
        )

        # Shift the min/max steps of all the children to account for the
        # the new operation inserted into the middle of the pipeline
        for child in tree.children:
            if child.min_steps > insert_idx:
                child.min_steps += 1
            if child.max_steps > insert_idx:
                child.max_steps += 1

    def syncretize_agg_onto_agg(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        Runs the syncretization logic between two children of a hybrid tree that
        are confirmed to be syncretizable, where both the base the extension
        child are aggregations. This is only possible when the
        extension's aggregations can be split into two rounds of aggregation.

        ```
        # Consider the following example:
        nations.CALCULATE(x=COUNT(customers), y=COUNT(customers.orders))

        # Can be syncretized into the equivalent of:
        child = customers.CALCULATE(z=COUNT(orders))
        nations.CALCULATE(x=COUNT(child), y=SUM(child.z))
        ```

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `base_idx`: The index of the base child in the tree.
            `extension_idx`: The index of the extension child in the tree.
            `extension_subtree`: The newly created suffix subtree of the
            extension child that will be inserted as a child of the base subtree.
            `remapping`: A dictionary that will be populated with references
            to the new aggregations in the base child, so that any references
            to the old aggregations in the extension child can be remapped to
            the new aggregations in the base child.
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        if new_connection_type.is_semi or extension_subtree.always_exists():
            base_child.connection_type = (
                base_child.connection_type.reconcile_connection_types(
                    ConnectionType.SEMI
                )
            )

        if extension_subtree.always_exists() and new_connection_type not in (
            ConnectionType.SEMI,
            ConnectionType.ANTI,
        ):
            # If the extension child always exists with regards to the base
            # child, promote it to a SEMI join (unless it already is, or it is
            # an ANTI).
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )
        elif new_connection_type.is_semi or new_connection_type.is_anti:
            # If the extension child does not always exist but the parent
            # must not preserve non-matching records, then convert it to a
            # regular aggregation and add a filter to the parent where COUNT
            # is > 0, and allow this count to be split by the extension child.
            # If the extension child is an anti join, do the same but with a
            # COUNT() == 0 filter.
            self.add_extension_semi_anti_count_filter(
                tree, extension_idx, new_connection_type.is_semi
            )
            new_connection_type = ConnectionType.AGGREGATION

        # If an aggregation is being added to a SEMI join, switch the SEMI
        # join to either a singular-only-match or aggregation-only-match.
        if base_child.connection_type == ConnectionType.SEMI:
            if base_subtree.is_singular():
                base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH
            else:
                base_child.connection_type = ConnectionType.AGGREGATION_ONLY_MATCH

        # Insert the new extension child as a child of the base subtree.
        min_steps: int = base_subtree.get_min_child_idx(
            extension_subtree, new_connection_type
        )
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        new_extension_child: HybridConnection = base_subtree.children[new_child_idx]

        # Iterate through all the aggregations in the original extension child
        # and create equivalents by creating a bottom aggregation call in the
        # extension child, a reference to it in the base child, then a new
        # aggregation in the base child that aggregates the reference to pass
        # the combined aggregation result up to the parent.
        is_first: bool = True
        old_child_ref: HybridExpr
        for agg_name, agg in extension_child.aggs.items():
            # Special Case: decompose AVG into SUM / COUNT
            if agg.operator == pydop.AVG:
                # Insert the SUM and COUNT aggregation calls into the extension
                sum_agg_name: str = self.translator.gen_agg_name(extension_child)
                count_agg_name: str = self.translator.gen_agg_name(extension_child)
                sum_agg: HybridFunctionExpr = HybridFunctionExpr(
                    pydop.SUM, agg.args, agg.typ
                )
                count_agg: HybridFunctionExpr = HybridFunctionExpr(
                    pydop.COUNT, agg.args, agg.typ
                )
                new_extension_child.aggs[sum_agg_name] = sum_agg
                new_extension_child.aggs[count_agg_name] = count_agg

                sum_child_expr: HybridExpr = HybridChildRefExpr(
                    sum_agg_name, new_child_idx, sum_agg.typ
                )
                count_child_expr: HybridExpr = HybridChildRefExpr(
                    count_agg_name, new_child_idx, count_agg.typ
                )
                sum_switch_ref: HybridExpr = self.translator.inject_expression(
                    base_subtree, sum_child_expr, is_first
                )
                count_switch_ref: HybridExpr = self.translator.inject_expression(
                    base_subtree, count_child_expr, False
                )

                # Insert the top SUM calls & division into the base child to compute AVG transformation.
                base_sum_agg_name: str = self.translator.gen_agg_name(base_child)
                base_count_agg_name: str = self.translator.gen_agg_name(base_child)
                base_sum_agg: HybridFunctionExpr = HybridFunctionExpr(
                    pydop.SUM, [sum_switch_ref], agg.typ
                )
                base_count_agg: HybridFunctionExpr = HybridFunctionExpr(
                    pydop.SUM, [count_switch_ref], agg.typ
                )
                base_child.aggs[base_sum_agg_name] = base_sum_agg
                base_child.aggs[base_count_agg_name] = base_count_agg

                old_child_ref = HybridChildRefExpr(agg_name, extension_idx, agg.typ)
                new_child_sum_ref: HybridExpr = HybridChildRefExpr(
                    base_sum_agg_name, base_idx, agg.typ
                )
                new_child_count_ref: HybridExpr = HybridChildRefExpr(
                    base_count_agg_name, base_idx, agg.typ
                )
                quotient: HybridExpr = HybridFunctionExpr(
                    pydop.DIV, [new_child_sum_ref, new_child_count_ref], agg.typ
                )
                remapping[old_child_ref] = quotient

            else:
                # Identify which operators to use for the bottom/top
                # aggregations.
                extension_op, base_op = self.supported_syncretize_operators[
                    agg.operator
                ]

                # Insert the bottom aggregation call into the extension
                extension_agg_name: str = self.translator.gen_agg_name(extension_child)
                extension_agg: HybridFunctionExpr = HybridFunctionExpr(
                    extension_op, agg.args, agg.typ
                )
                new_extension_child.aggs[extension_agg_name] = extension_agg

                child_expr: HybridExpr = HybridChildRefExpr(
                    extension_agg_name, new_child_idx, extension_agg.typ
                )
                switch_ref: HybridExpr = self.translator.inject_expression(
                    base_subtree, child_expr, is_first
                )

                # Insert the top aggregation call into the base
                base_agg_name: str = self.translator.gen_agg_name(base_child)
                base_agg: HybridFunctionExpr = HybridFunctionExpr(
                    base_op, [switch_ref], agg.typ
                )
                base_child.aggs[base_agg_name] = base_agg

                old_child_ref = HybridChildRefExpr(agg_name, extension_idx, agg.typ)
                new_child_ref: HybridExpr = HybridChildRefExpr(
                    base_agg_name, base_idx, agg.typ
                )
                remapping[old_child_ref] = new_child_ref
            is_first = False

    def syncretize_agg_onto_singular(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        Runs the syncretization logic for two children of a hybrid tree that
        are confirmed to be syncretizable, where the base child is singular and
        the extension child is an aggregation. This can be done with any
        aggregation functions since the aggregation can be done per record of
        the base child which just passes the results up as singular values to
        the parent.

        ```
        # Consider the following example:
        customers.CALCULATE(x=nation.name, y=COUNT(nation.suppliers))

        # Can be syncretized into the equivalent of:
        child = nation.CALCULATE(z=COUNT(suppliers))
        customers.CALCULATE(x=child.name, y=child.z)
        ```

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `base_idx`: The index of the base child in the tree.
            `extension_idx`: The index of the extension child in the tree.
            `extension_subtree`: The newly created suffix subtree of the
            extension child that will be inserted as a child of the base.
            `remapping`: A dictionary that will be populated with references
            to the new terms in the base child, so that any references
            to the old aggregations in the extension child can be remapped to
            the new terms in the base child.
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        # If the extension child only preserves matches, promote the base to a
        # SEMI join.
        if new_connection_type.is_semi:
            base_child.connection_type = (
                base_child.connection_type.reconcile_connection_types(
                    ConnectionType.SEMI
                )
            )

        if (
            extension_subtree.always_exists()
            and new_connection_type != ConnectionType.SEMI
        ):
            # If the extension child always exists with regards to the base,
            # promote it to a SEMI join (unless it already is).
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )
        elif new_connection_type.is_semi:
            # If the extension child does not always exist but the parent
            # must not preserve non-matching records, then convert the
            # base child into one that only preserves matches.
            base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH

        # If an aggregation is being added to a SEMI join, switch the SEMI
        # join to either a singular-only-match or aggregation-only-match.
        if base_child.connection_type == ConnectionType.SEMI:
            if base_subtree.is_singular():
                base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH
            else:
                base_child.connection_type = ConnectionType.AGGREGATION_ONLY_MATCH

        # Insert the new extension child as a child of the base subtree.
        min_steps: int = base_subtree.get_min_child_idx(
            extension_subtree, new_connection_type
        )
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        new_extension_child: HybridConnection = base_subtree.children[new_child_idx]

        # Iterate through all the aggregations in the original extension child.
        # For each one, copy the aggregation into the new extension child, add
        # a reference to the agg call to the base child, and map the old child
        # reference to the old extension child to a new child reference to the
        # new reference in the base child.
        idx: int = 0
        for agg_name, agg in extension_child.aggs.items():
            # Insert the aggregation call into the new child
            new_extension_child.aggs[agg_name] = agg

            child_expr: HybridExpr = HybridChildRefExpr(
                agg_name, new_child_idx, agg.typ
            )
            switch_ref: HybridExpr = self.translator.inject_expression(
                base_subtree, child_expr, idx == 0
            )
            assert isinstance(switch_ref, HybridRefExpr)

            # Make a child reference to the reference to the aggregation call
            old_child_ref: HybridExpr = HybridChildRefExpr(
                agg_name, extension_idx, agg.typ
            )
            new_child_ref: HybridExpr = HybridChildRefExpr(
                switch_ref.name, base_idx, agg.typ
            )
            remapping[old_child_ref] = new_child_ref
            idx += 1

    def syncretize_singular_onto_singular(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        Runs the syncretization logic for two children of a hybrid tree that
        are confirmed to be syncretizable, where the base child is singular and
        the extension child is also singular.

        ```
        # Consider the following example:
        customers.CALCULATE(x=nation.name, y=nation.region.name)

        # Can be syncretized into the equivalent of:
        child = nation.CALCULATE(z=region.name)
        customers.CALCULATE(x=child.name, y=child.z)
        ```

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `base_idx`: The index of the base child in the tree.
            `extension_idx`: The index of the extension child in the tree.
            `extension_subtree`: The newly created suffix subtree of the
            extension child that will be inserted as a child of the base.
            `remapping`: A dictionary that will be populated with references
            to the new terms in the base child, so that any references
            to the old terms in the extension child can be remapped to
            the new terms in the base child.
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        # If the extension child only preserves matches, promote the base to a
        # SEMI join.
        if new_connection_type.is_semi:
            base_child.connection_type = (
                base_child.connection_type.reconcile_connection_types(
                    ConnectionType.SEMI
                )
            )

        # If the extension child always exists with regards to the base,
        # promote it to a SEMI join.
        if extension_subtree.always_exists():
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )

        # If a singular is being added to a pure SEMI join, switch the SEMI
        # join to an singular-only-match.
        if (
            base_child.connection_type == ConnectionType.SEMI
            and extension_child.connection_type != ConnectionType.SEMI
        ):
            base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH

        # Insert the new extension child as a child of the base subtree.
        min_steps: int = base_subtree.get_min_child_idx(
            extension_subtree, new_connection_type
        )
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )

        # For every term in the extension child, add a child reference to pull
        # it into the base child. Skip this step if the extension child is just
        # a pure SEMI/ANTI join.
        if new_connection_type in (ConnectionType.SEMI, ConnectionType.ANTI):
            return
        for idx, term_name in enumerate(sorted(extension_subtree.pipeline[-1].terms)):
            old_term: HybridExpr = extension_subtree.pipeline[-1].terms[term_name]
            child_expr: HybridExpr = HybridChildRefExpr(
                term_name, new_child_idx, old_term.typ
            )
            switch_ref: HybridExpr = self.translator.inject_expression(
                base_subtree, child_expr, idx == 0
            )
            assert isinstance(switch_ref, HybridRefExpr)
            old_child_ref: HybridExpr = HybridChildRefExpr(
                term_name, extension_idx, old_term.typ
            )
            new_child_ref: HybridExpr = HybridChildRefExpr(
                switch_ref.name, base_idx, old_term.typ
            )
            remapping[old_child_ref] = new_child_ref

    def syncretize_singular_onto_agg(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        Runs the syncretization logic for two children of a hybrid tree that
        are confirmed to be syncretizable, where the base child is an
        aggregation and the extension child is singular.

        ```
        # Consider the following example:
        customers.CALCULATE(x=COUNT(orders), y=orders.lines.BEST(by=order_date.ASC(), per="customers").part.name)

        # Can be syncretized into the equivalent of:
        child = orders.CALCULATE(z=lines.BEST(by=order_date.ASC(), per="customers").part.name)
        customers.CALCULATE(x=COUNT(child), y=MAX(child.z))
        # (MAX will select only the value from the one desired row of `z`, ignoring all NULL rows)
        ```

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `base_idx`: The index of the base child in the tree.
            `extension_idx`: The index of the extension child in the tree.
            `extension_subtree`: The newly created suffix subtree of the
            extension child that will be inserted as a child of the base.
            `remapping`: A dictionary that will be populated with references
            to the new terms in the base child, so that any references
            to the old terms in the extension child can be remapped to
            the new terms in the base child.
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        # If the extension child only preserves matches, promote the base to a
        # SEMI join.
        if new_connection_type.is_semi:
            base_child.connection_type = (
                base_child.connection_type.reconcile_connection_types(
                    ConnectionType.SEMI
                )
            )

        if extension_subtree.always_exists():
            # If the extension child always exists with regards to the base,
            # promote it to a SEMI join.
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )
        elif new_connection_type.is_semi:
            # If the extension child does not always exist but the parent
            # must not preserve non-matching records, then convert it to a
            # regular aggregation and add a filter to the parent where COUNT
            # is > 0.
            for term_name in sorted(extension_subtree.pipeline[-1].terms):
                old_term: HybridExpr = extension_subtree.pipeline[-1].terms[term_name]
                passthrough_agg: HybridFunctionExpr = HybridFunctionExpr(
                    pydop.ANYTHING,
                    [HybridRefExpr(term_name, old_term.typ)],
                    old_term.typ,
                )
                extension_child.aggs[term_name] = passthrough_agg
            self.add_extension_semi_anti_count_filter(tree, extension_idx, True)
            extension_child.connection_type = ConnectionType.AGGREGATION
            self.syncretize_agg_onto_agg(
                tree,
                base_idx,
                extension_idx,
                extension_subtree,
                remapping,
            )
            return

        # Insert the new extension child as a child of the base subtree.
        min_steps: int = base_subtree.get_min_child_idx(
            extension_subtree, new_connection_type
        )
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )

        # Iterate through every term in the extension child and add a child
        # reference to pull it into the base child. Then, add a MAX aggregation
        # to the base child that references the new reference, thus pulling the
        # one desired row into the parent tree.
        for idx, term_name in enumerate(sorted(extension_subtree.pipeline[-1].terms)):
            old_term = extension_subtree.pipeline[-1].terms[term_name]
            # Insert a reference to the child into the base
            child_expr: HybridExpr = HybridChildRefExpr(
                term_name, new_child_idx, old_term.typ
            )
            switch_ref: HybridExpr = self.translator.inject_expression(
                base_subtree, child_expr, idx == 0
            )

            # Insert a pass-through aggregation call into the base, but
            # explicitly use MAX to ensure any null records from the base
            # are not chosen.
            base_agg_name: str = self.translator.gen_agg_name(base_child)
            base_agg: HybridFunctionExpr = HybridFunctionExpr(
                pydop.MAX, [switch_ref], old_term.typ
            )
            base_child.aggs[base_agg_name] = base_agg

            old_child_ref: HybridExpr = HybridChildRefExpr(
                term_name, extension_idx, old_term.typ
            )
            new_child_ref: HybridExpr = HybridChildRefExpr(
                base_agg_name, base_idx, old_term.typ
            )
            remapping[old_child_ref] = new_child_ref

    def syncretize_subtrees(
        self, tree: HybridTree, base_idx: int, extension_idx: int, extension_height: int
    ) -> bool:
        """
        Runs the main syncretization logic to attempt to combine two children
        of a hybrid tree, forming a (base, extension) pair, into a single child
        by splitting the extension child into a subtree using only the suffix
        that is not present in the base child, which is then added as a child
        of the base child (instead of the parent tree), whose terms in turn get
        referenced by the base child then pulled up into the parent tree. First
        needs to verify that the two children are syncretizable, then the rest
        of the logic is dispatched onto helper functions.

        Args:
            `tree`: The hybrid tree containing the base and extension children.
            `base_idx`: The index of the base child in the tree.
            `extension_idx`: The index of the extension child in the tree.
            `extension_height`: The number of levels of the suffix of the
            extension that are not in the common prefix with the base child.

        Returns:
            `True` if the syncretization was successful, `False` otherwise. If
            successful, the transformation of the base/extension children is
            done in-place, but the now-dead extension child will still need to
            be pruned.
        """
        remapping: dict[HybridExpr, HybridExpr] = {}
        base_child: HybridConnection = tree.children[base_idx]
        base_subtree: HybridTree = base_child.subtree
        extension_child: HybridConnection = tree.children[extension_idx]

        # ANTI-ANTI pairs are automatically syncretized since the base not
        # being present implies the extension is not present, so we can just
        # have the extension child be pruned without modifying the base.
        if (
            base_child.connection_type.is_anti
            and extension_child.connection_type.is_anti
        ):
            return True

        # Do not syncretize subtrees if their acceptable step ranges do not
        # overlap.
        if (
            base_child.max_steps <= extension_child.min_steps
            or extension_child.max_steps <= base_child.min_steps
        ):
            return False

        # Contract the range of valid definition locations for the base child
        # to account for any additional restrictions of the extension child.
        base_child.min_steps = max(base_child.min_steps, extension_child.min_steps)
        base_child.max_steps = min(base_child.max_steps, extension_child.max_steps)

        # Build the new subtree for the extension child which will be a child
        # of the base subtree instead of the parent tree.
        extension_subtree: HybridTree = self.make_extension_child(
            extension_child.subtree, extension_height, base_subtree
        )

        # Dispatch onto one of the four main helper procedures based on the
        # exact combination of connection types, with all other connection
        # type combinations resulting in a refusal to syncretize.
        match (base_child.connection_type, extension_child.connection_type):
            case (
                (ConnectionType.AGGREGATION, ConnectionType.AGGREGATION)
                | (ConnectionType.AGGREGATION, ConnectionType.AGGREGATION_ONLY_MATCH)
                | (ConnectionType.AGGREGATION, ConnectionType.SEMI)
                | (ConnectionType.AGGREGATION, ConnectionType.ANTI)
                | (ConnectionType.AGGREGATION_ONLY_MATCH, ConnectionType.AGGREGATION)
                | (
                    ConnectionType.AGGREGATION_ONLY_MATCH,
                    ConnectionType.AGGREGATION_ONLY_MATCH,
                )
                | (ConnectionType.AGGREGATION_ONLY_MATCH, ConnectionType.SEMI)
                | (ConnectionType.AGGREGATION_ONLY_MATCH, ConnectionType.ANTI)
                | (ConnectionType.SEMI, ConnectionType.AGGREGATION)
                | (ConnectionType.SEMI, ConnectionType.AGGREGATION_ONLY_MATCH)
            ):
                # If in the AGG-AGG syncretization case, we need to ensure that
                # all aggregations in the extension child can be split into a
                # top/bottom aggregation call.
                if not all(
                    agg.operator in self.supported_syncretize_operators
                    or agg.operator == pydop.AVG
                    for agg in extension_child.aggs.values()
                ):
                    return False
                self.syncretize_agg_onto_agg(
                    tree,
                    base_idx,
                    extension_idx,
                    extension_subtree,
                    remapping,
                )
            case (
                (ConnectionType.SINGULAR, ConnectionType.SINGULAR)
                | (ConnectionType.SINGULAR, ConnectionType.SINGULAR_ONLY_MATCH)
                | (ConnectionType.SINGULAR, ConnectionType.SEMI)
                | (ConnectionType.SINGULAR, ConnectionType.ANTI)
                | (ConnectionType.SINGULAR_ONLY_MATCH, ConnectionType.SINGULAR)
                | (
                    ConnectionType.SINGULAR_ONLY_MATCH,
                    ConnectionType.SINGULAR_ONLY_MATCH,
                )
                | (ConnectionType.SINGULAR_ONLY_MATCH, ConnectionType.SEMI)
                | (ConnectionType.SINGULAR_ONLY_MATCH, ConnectionType.ANTI)
                | (ConnectionType.SEMI, ConnectionType.SINGULAR)
                | (ConnectionType.SEMI, ConnectionType.SINGULAR_ONLY_MATCH)
                | (ConnectionType.SEMI, ConnectionType.SEMI)
            ):
                self.syncretize_singular_onto_singular(
                    tree,
                    base_idx,
                    extension_idx,
                    extension_subtree,
                    remapping,
                )
            case (
                (ConnectionType.AGGREGATION, ConnectionType.SINGULAR)
                | (ConnectionType.AGGREGATION, ConnectionType.SINGULAR_ONLY_MATCH)
                | (ConnectionType.AGGREGATION_ONLY_MATCH, ConnectionType.SINGULAR)
                | (
                    ConnectionType.AGGREGATION_ONLY_MATCH,
                    ConnectionType.SINGULAR_ONLY_MATCH,
                )
            ):
                self.syncretize_singular_onto_agg(
                    tree,
                    base_idx,
                    extension_idx,
                    extension_subtree,
                    remapping,
                )
            case (
                (ConnectionType.SINGULAR, ConnectionType.AGGREGATION)
                | (ConnectionType.SINGULAR, ConnectionType.AGGREGATION_ONLY_MATCH)
                | (ConnectionType.SINGULAR_ONLY_MATCH, ConnectionType.AGGREGATION)
                | (
                    ConnectionType.SINGULAR_ONLY_MATCH,
                    ConnectionType.AGGREGATION_ONLY_MATCH,
                )
            ):
                self.syncretize_agg_onto_singular(
                    tree,
                    base_idx,
                    extension_idx,
                    extension_subtree,
                    remapping,
                )
            case _:
                return False

        # Once the syncretization is complete, all references to the extension
        # child within the parent tree must be remapped to different references
        # to the base child.
        for operation in tree.pipeline:
            operation.replace_expressions(remapping)

        return True

    def syncretize_children(self, tree: HybridTree) -> None:
        """
        Entry point for the syncretization algorithm that reverses the
        entire tree, finds potential candidate pairs for syncretization, and
        runs the main syncretization logic on these pairs. The transformations
        are done in-place.

        Args:
            `tree`: The hybrid tree to check for syncretization opportunities,
            (along with its ancestors and children)
        """
        # First, run syncretization on the parent level, if it exists.
        if tree.parent is not None:
            self.syncretize_children(tree.parent)

        # Store the index of any children that should not be syncretized onto
        # anything else (-1 if not applicable). This is used so the child used
        # by a pull up node is not syncretized onto anything else.
        ignore_idx: int = -1
        if isinstance(tree.pipeline[0], HybridChildPullUp):
            ignore_idx = tree.pipeline[0].child_idx

        # Iterate across every combination of (base, extension) and find all
        # of the candidate pairs that can potentially be syncretized. Store
        # them in a list of tuples so they can be sorted later.
        syncretize_options: list[tuple[int, int, int, int]] = []
        for base_idx in range(len(tree.children)):
            for extension_idx in range(len(tree.children)):
                if extension_idx in (base_idx, ignore_idx):
                    continue
                can_syncretize, extension_height = self.can_syncretize_subtrees(
                    tree.children[base_idx], tree.children[extension_idx]
                )
                if can_syncretize:
                    total_height: int = 1
                    subtree: HybridTree = tree.children[extension_idx].subtree
                    while subtree.parent is not None:
                        subtree = subtree.parent
                        total_height += 1
                    syncretize_options.append(
                        (-total_height, extension_height, extension_idx, base_idx)
                    )

        # If there was at least one syncretization opportunity, then sort them
        # using the tuple so that largest extension children go first, with
        # ties broken in favor of the pair with the largest extension child
        # suffix size going first. When iterating through each pair, attempt to
        # run syncretization on the duo.
        if len(syncretize_options) > 0:
            children_to_delete: set[int] = set()
            syncretize_options.sort()
            for _, extension_height, extension_idx, base_idx in syncretize_options:
                if (
                    extension_idx in children_to_delete
                    or base_idx in children_to_delete
                ):
                    continue

                # If the syncretization was successful, then mark the extension
                # child as dead so that it can be pruned later and will not be
                # used in any more syncretization attempts (as the base or
                # child).
                if self.syncretize_subtrees(
                    tree, base_idx, extension_idx, extension_height
                ):
                    children_to_delete.add(extension_idx)

            # At the end, delete any extension children that were successfully
            # syncretized onto a base child.
            tree.remove_dead_children(children_to_delete)

        # Finally, recursively run the syncretization algorithm on each child
        # of the tree.
        for child in tree.children:
            self.syncretize_children(child.subtree)
