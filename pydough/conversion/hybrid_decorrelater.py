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
    HybridRefExpr,
    HybridTree,
    HybridWindowExpr,
)


class Decorrelater:
    """
    TODO
    """

    def make_decorrelate_parent(self, hybrid: HybridTree, child_idx: int) -> HybridTree:
        """
        TODO
        """
        successor: HybridTree | None = hybrid.successor
        hybrid._successor = None
        new_hybrid: HybridTree = copy.deepcopy(hybrid)
        hybrid._successor = successor
        new_hybrid._children = new_hybrid._children[:child_idx]
        new_hybrid._pipeline = new_hybrid._pipeline[
            : hybrid.children[child_idx].required_steps + 1
        ]
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
            child_root = child_root.parent
            child_height += 1
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
            new_parent: HybridTree = self.make_decorrelate_parent(hybrid, idx)
            match child.connection_type:
                case ConnectionType.SINGULAR | ConnectionType.SINGULAR_ONLY_MATCH:
                    self.decorrelate_singular(hybrid, new_parent, child)
                case (
                    ConnectionType.AGGREGATION
                    | ConnectionType.AGGREGATION_ONLY_MATCH
                    | ConnectionType.NDISTINCT
                    | ConnectionType.NDISTINCT_ONLY_MATCH
                ):
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
