"""
Logic for applying decorrelation to hybrid trees before relational conversion
if the correlate is not a semi/anti join.
"""

__all__ = ["run_hybrid_decorrelation"]


import copy

from .hybrid_tree import (
    ConnectionType,
    HybridBackRefExpr,
    HybridCalc,
    HybridChildRefExpr,
    HybridColumnExpr,
    HybridConnection,
    HybridCorrelExpr,
    HybridExpr,
    HybridFilter,
    HybridFunctionExpr,
    HybridLiteralExpr,
    HybridPartition,
    HybridRefExpr,
    HybridTree,
    HybridWindowExpr,
)


class Decorrelater:
    """
    TODO
    """

    def make_decorrelate_parent(
        self, hybrid: HybridTree, child_idx: int, required_steps: int
    ) -> HybridTree:
        """
        TODO
        """
        if isinstance(hybrid.pipeline[0], HybridPartition) and child_idx == 0:
            assert hybrid.parent is not None
            return self.make_decorrelate_parent(
                hybrid.parent, len(hybrid.parent.children), required_steps
            )
        successor: HybridTree | None = hybrid.successor
        hybrid._successor = None
        new_hybrid: HybridTree = copy.deepcopy(hybrid)
        hybrid._successor = successor
        new_hybrid._children = new_hybrid._children[:child_idx]
        new_hybrid._pipeline = new_hybrid._pipeline[: required_steps + 1]
        return new_hybrid

    def remove_correl_refs(
        self, expr: HybridExpr, parent: HybridTree, child_height: int
    ) -> HybridExpr:
        """
        TODO
        """
        match expr:
            case HybridCorrelExpr():
                result: HybridExpr | None = expr.expr.shift_back(child_height)
                assert result is not None
                return result
            case HybridFunctionExpr():
                for idx, arg in enumerate(expr.args):
                    expr.args[idx] = self.remove_correl_refs(arg, parent, child_height)
                return expr
            case HybridWindowExpr():
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
                return expr
            case _:
                raise NotImplementedError(
                    f"Unsupported expression type: {expr.__class__.__name__}."
                )

    def decorrelate_singular(
        self, old_parent: HybridTree, new_parent: HybridTree, child: HybridConnection
    ) -> None:
        """
        TODO
        """
        # First, find the height of the child subtree & its top-most level.
        child_root: HybridTree = child.subtree
        child_height: int = 1
        while child_root.parent is not None:
            child_height += 1
            child_root = child_root.parent
        # Link the top level of the child subtree to the new parent.
        new_parent.add_successor(child_root)
        # Replace any correlated references to the original parent with BACK references.
        level: HybridTree = child.subtree
        while level.parent is not None and level is not new_parent:
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
                if isinstance(operation, HybridCalc):
                    for str, expr in operation.new_expressions.items():
                        operation.new_expressions[str] = self.remove_correl_refs(
                            expr, old_parent, child_height
                        )
                if isinstance(operation, HybridFilter):
                    operation.condition = self.remove_correl_refs(
                        operation.condition, old_parent, child_height
                    )
            level = level.parent
        # Update the join keys to join on the unique keys of all the ancestors.
        new_join_keys: list[tuple[HybridExpr, HybridExpr]] = []
        additional_levels: int = 0
        current_level: HybridTree | None = old_parent
        while current_level is not None:
            for unique_key in current_level.pipeline[0].unique_exprs:
                lhs_key: HybridExpr | None = unique_key.shift_back(additional_levels)
                rhs_key: HybridExpr | None = unique_key.shift_back(
                    additional_levels + child_height
                )
                assert lhs_key is not None and rhs_key is not None
                new_join_keys.append((lhs_key, rhs_key))
            current_level = current_level.parent
            additional_levels += 1
        child.subtree.join_keys = new_join_keys

    def decorrelate_aggregate(
        self, old_parent: HybridTree, new_parent: HybridTree, child: HybridConnection
    ) -> None:
        """
        TODO
        """
        self.decorrelate_singular(old_parent, new_parent, child)
        new_agg_keys: list[HybridExpr] = []
        assert child.subtree.join_keys is not None
        for _, rhs_key in child.subtree.join_keys:
            new_agg_keys.append(rhs_key)
        child.subtree.agg_keys = new_agg_keys

    def decorrelate_hybrid_tree(self, hybrid: HybridTree) -> HybridTree:
        """
        TODO
        """
        # Recursively decorrelate the ancestors of the current level of the
        # hybrid tree.
        if hybrid.parent is not None:
            hybrid._parent = self.decorrelate_hybrid_tree(hybrid.parent)
        # Iterate across all the children and recursively decorrelate them.
        for child in hybrid.children:
            child.subtree = self.decorrelate_hybrid_tree(child.subtree)
        # Iterate across all the children, identify any that are correlated,
        # and transform any of the correlated ones that require decorrelation
        # due to the type of connection.
        for idx, child in enumerate(hybrid.children):
            if idx not in hybrid.correlated_children:
                continue
            new_parent: HybridTree = self.make_decorrelate_parent(
                hybrid, idx, hybrid.children[idx].required_steps + 1
            )
            match child.connection_type:
                case ConnectionType.SINGULAR | ConnectionType.SINGULAR_ONLY_MATCH:
                    self.decorrelate_singular(hybrid, new_parent, child)
                case ConnectionType.AGGREGATION | ConnectionType.AGGREGATION_ONLY_MATCH:
                    self.decorrelate_aggregate(hybrid, new_parent, child)
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
                    continue
        return hybrid


def run_hybrid_decorrelation(hybrid: HybridTree) -> HybridTree:
    """
    TODO
    """
    decorr: Decorrelater = Decorrelater()
    return decorr.decorrelate_hybrid_tree(hybrid)
