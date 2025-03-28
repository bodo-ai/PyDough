"""
Logic used to merge adjacent projections in relational trees when convenient.
"""

__all__ = ["merge_projects"]

from collections import defaultdict
from collections.abc import Iterable

from pydough.relational import (
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    WindowCallExpression,
)
from pydough.relational.rel_util import (
    transpose_expression,
)


def add_expr_uses(
    expr: RelationalExpression, n_uses: defaultdict[RelationalExpression, int]
) -> None:
    """
    Count the number of times nontrivial expressions are used in an expression
    and add them to a mapping of suchc ounts.

    Args:
        `expr`: The expression to count the nontrivial expressions of.
        `n_uses`: A dictionary mapping column names to their reference counts.
        This is modified in-place by the function call.
    """
    if isinstance(expr, CallExpression):
        n_uses[expr] += 1
        for arg in expr.inputs:
            add_expr_uses(arg, n_uses)
    if isinstance(expr, WindowCallExpression):
        n_uses[expr] += 1
        for arg in expr.inputs:
            add_expr_uses(arg, n_uses)
        for partition_arg in expr.partition_inputs:
            add_expr_uses(partition_arg, n_uses)
        for order_arg in expr.order_inputs:
            add_expr_uses(order_arg.expr, n_uses)


def all_refs_single_use(exprs: Iterable[RelationalExpression]) -> bool:
    """
    TODO
    """
    n_uses: defaultdict[RelationalExpression, int] = defaultdict(int)
    for expr in exprs:
        add_expr_uses(expr, n_uses)
    return len(n_uses) == 0 or max(n_uses.values()) == 1


def merge_projects(node: RelationalNode) -> RelationalNode:
    """
    Merge adjacent projections when beneficial.

    Args:
        `node`: The current node of the relational tree.

    Returns:
        The transformed version of `node` with adjacent projections merged
        into one when the top project never references nodes from the bottom
        more than once.
    """
    for idx, input in enumerate(node.inputs):
        node.inputs[idx] = merge_projects(input)
    expr: RelationalExpression
    new_expr: RelationalExpression
    if isinstance(node, (RelationalRoot, Project)):
        while isinstance(node.input, Project):
            child_project: Project = node.input
            if isinstance(node, RelationalRoot):
                # TODO: add comment
                if all(
                    isinstance(expr, ColumnReference)
                    for expr in child_project.columns.values()
                ) or (
                    all_refs_single_use(node.columns.values())
                    and len(node.orderings) == 0
                ):
                    for idx, (name, expr) in enumerate(node.ordered_columns):
                        assert isinstance(expr, ColumnReference)
                        new_expr = transpose_expression(expr, child_project.columns)
                        node.columns[name] = new_expr
                        node.ordered_columns[idx] = (name, new_expr)
                    for idx, sort_info in enumerate(node.orderings):
                        new_expr = transpose_expression(
                            sort_info.expr, child_project.columns
                        )
                        node.orderings[idx] = ExpressionSortInfo(
                            new_expr, sort_info.ascending, sort_info.nulls_first
                        )
                    node._input = child_project.input
                else:
                    break
            elif isinstance(node, Project):
                # TODO: add comment
                if all(
                    isinstance(expr, ColumnReference)
                    for expr in child_project.columns.values()
                ) or all_refs_single_use(node.columns.values()):
                    for name, expr in node.columns.items():
                        new_expr = transpose_expression(expr, child_project.columns)
                        node.columns[name] = new_expr
                    node._input = child_project.input
                else:
                    break
    return node
