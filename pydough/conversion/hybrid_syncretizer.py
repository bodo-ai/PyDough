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
    TODO
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
    TODO
    """

    def __init__(self, translator: "HybridTranslator") -> None:
        self.translator: HybridTranslator = translator

    def make_extension_child(
        self, child: HybridTree, levels_up: int, new_base: HybridTree
    ) -> HybridTree:
        """
        TODO
        """
        if levels_up <= 0:
            raise ValueError(f"Cannot make extension child with {levels_up} levels up")

        # Clone just the current level, detached from any parent or successor.
        parent: HybridTree | None = child.parent
        successor: HybridTree | None = child.successor
        child._parent = None
        child._successor = None
        new_child: HybridTree = copy.deepcopy(child)
        new_child.squish_backrefs_into_correl(levels_up)
        child._parent = parent
        child._successor = successor

        if levels_up == 1:
            (
                new_child._join_keys,
                new_child._agg_keys,
                new_child._general_join_condition,
            ) = self.translator.extract_link_root_info(
                new_base, new_child, True, len(new_base.children)
            )
        else:
            assert parent is not None
            new_parent: HybridTree = self.make_extension_child(
                parent, levels_up - 1, new_base
            )
            new_parent.add_successor(new_child)
        return new_child

    def can_syncretize_subtrees(
        self, base_child: HybridConnection, extension_child: HybridConnection
    ) -> tuple[bool, int]:
        """
        TODO
        """
        prefix_levels_up: int = 0
        base_subtree: HybridTree = base_child.subtree
        crawl_subtree: HybridTree = extension_child.subtree
        while True:
            if base_subtree.equalsIgnoringSuccessors(crawl_subtree):
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
        TODO
        """
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
        agg_ref: HybridExpr = HybridChildRefExpr(agg_name, extension_idx, NumericType())
        literal_zero: HybridExpr = HybridLiteralExpr(Literal(0, NumericType()))
        if not is_semi:
            agg_ref = HybridFunctionExpr(
                pydop.DEFAULT_TO, [agg_ref, literal_zero], BooleanType()
            )
        # Insert the new filter right after the required steps index,
        # and update other children accordingly.
        insert_idx: int = extension_child.min_steps + 1
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
        for child in tree.children:
            if child.min_steps > insert_idx:
                child.min_steps += 1

    def syncretize_agg_onto_agg(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        TODO
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        if extension_subtree.always_exists() and new_connection_type not in (
            ConnectionType.SEMI,
            ConnectionType.ANTI,
        ):
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
        # join to an aggregation-only-match.
        if base_child.connection_type == ConnectionType.SEMI:
            base_child.connection_type = ConnectionType.AGGREGATION

        min_steps: int = base_subtree.get_min_child_idx(extension_subtree)
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        new_extension_child: HybridConnection = base_subtree.children[new_child_idx]

        idx: int = 0
        for agg_name, agg in extension_child.aggs.items():
            extension_op, base_op = self.supported_syncretize_operators[agg.operator]

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
                base_subtree, child_expr, idx == 0
            )

            # Insert the top aggregation call into the base
            base_agg_name: str = self.translator.gen_agg_name(base_child)
            base_agg: HybridFunctionExpr = HybridFunctionExpr(
                base_op, [switch_ref], agg.typ
            )
            base_child.aggs[base_agg_name] = base_agg

            old_child_ref: HybridExpr = HybridChildRefExpr(
                agg_name, extension_idx, agg.typ
            )
            new_child_ref: HybridExpr = HybridChildRefExpr(
                base_agg_name, base_idx, agg.typ
            )
            remapping[old_child_ref] = new_child_ref
            idx += 1

    def syncretize_agg_onto_singular(
        self,
        tree: HybridTree,
        base_idx: int,
        extension_idx: int,
        extension_subtree: HybridTree,
        remapping: dict[HybridExpr, HybridExpr],
    ) -> None:
        """
        TODO
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        if (
            extension_subtree.always_exists()
            and new_connection_type != ConnectionType.SEMI
        ):
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )
        elif new_connection_type.is_semi:
            # If the extension child does not always exist but the parent
            # must not preserve non-matching records, then convert the
            # base child into one that only preserves matches.
            base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH
        # If an aggregation is being added to a SEMI join, switch the SEMI
        # join to an aggregation-only-match.
        if base_child.connection_type == ConnectionType.SEMI:
            base_child.connection_type = ConnectionType.AGGREGATION

        min_steps: int = base_subtree.get_min_child_idx(extension_subtree)
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        new_extension_child: HybridConnection = base_subtree.children[new_child_idx]

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

            # Make a child reference to the reference to the aggregaiton call
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
        TODO
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        if extension_subtree.always_exists():
            new_connection_type = new_connection_type.reconcile_connection_types(
                ConnectionType.SEMI
            )

        # If a singular is being added to a SEMI join, switch the SEMI
        # join to an singular-only-match.
        if (
            base_child.connection_type == ConnectionType.SEMI
            and extension_child.connection_type != ConnectionType.SEMI
        ):
            base_child.connection_type = ConnectionType.SINGULAR_ONLY_MATCH

        min_steps: int = base_subtree.get_min_child_idx(extension_subtree)
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        base_subtree.children[new_child_idx]

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
        TODO
        """
        base_child: HybridConnection = tree.children[base_idx]
        extension_child: HybridConnection = tree.children[extension_idx]
        base_subtree: HybridTree = base_child.subtree

        new_connection_type: ConnectionType = extension_child.connection_type

        if extension_subtree.always_exists():
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

        min_steps: int = base_subtree.get_min_child_idx(extension_subtree)
        max_steps: int = len(base_subtree.pipeline)
        new_child_idx: int = base_subtree.add_child(
            extension_subtree, new_connection_type, min_steps, max_steps
        )
        base_subtree.children[new_child_idx]

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
        TODO
        """
        remapping: dict[HybridExpr, HybridExpr] = {}
        base_child: HybridConnection = tree.children[base_idx]
        base_subtree: HybridTree = base_child.subtree
        extension_child: HybridConnection = tree.children[extension_idx]
        # ANTI are automatically syncretized since the base not being
        # present implies the extension is not present, so we can just
        # have the extension child be pruned without modifying the
        # base.
        if (
            base_child.connection_type.is_anti
            and extension_child.connection_type.is_anti
        ):
            return True

        all_aggs_syncretizable: bool = all(
            agg.operator in self.supported_syncretize_operators
            for agg in extension_child.aggs.values()
        )

        # Do not syncretize subtrees if their acceptable step ranges do not
        # overlap.
        if (
            base_child.max_steps is not None
            and base_child.max_steps <= extension_child.min_steps
        ) or (
            extension_child.max_steps is not None
            and extension_child.max_steps <= base_child.min_steps
        ):
            return False

        new_min_steps: int = base_child.min_steps
        new_max_steps: int | None = base_child.max_steps
        if extension_child.min_steps > new_min_steps:
            new_min_steps = extension_child.min_steps
        else:
            if extension_child.subtree.contains_correlates():
                return False
        if extension_child.max_steps is not None and (
            new_max_steps is None or extension_child.max_steps < new_max_steps
        ):
            new_max_steps = extension_child.max_steps

        base_child.min_steps = new_min_steps
        base_child.max_steps = new_max_steps

        # Build the new subtree for the extension child which will be a child
        # of the base subtree instead of the parent tree.
        extension_subtree: HybridTree = self.make_extension_child(
            extension_child.subtree, extension_height, base_subtree
        )
        # extension_subtree.squish_backrefs_into_correl(extension_height)

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
                if not all_aggs_syncretizable:
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

        for operation in tree.pipeline:
            operation.replace_expressions(remapping)

        return True

    def syncretize_children(self, tree: HybridTree) -> None:
        """
        TODO
        """
        if tree.parent is not None:
            self.syncretize_children(tree.parent)
        syncretize_options: list[tuple[int, int, int, int]] = []
        ignore_idx: int = -1
        if isinstance(tree.pipeline[0], HybridChildPullUp):
            ignore_idx = tree.pipeline[0].child_idx
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
                        (extension_height, -total_height, extension_idx, base_idx)
                    )
        children_to_delete: set[int] = set()
        if len(syncretize_options) > 0:
            syncretize_options.sort()
            for extension_height, _, extension_idx, base_idx in syncretize_options:
                if (
                    extension_idx in children_to_delete
                    or base_idx in children_to_delete
                ):
                    continue
                success: bool = self.syncretize_subtrees(
                    tree, base_idx, extension_idx, extension_height
                )
                if success:
                    children_to_delete.add(extension_idx)
            tree.remove_dead_children(children_to_delete)
        for child in tree.children:
            self.syncretize_children(child.subtree)
