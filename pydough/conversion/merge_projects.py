"""
Logic used to merge adjacent projections in relational trees when convenient.
"""

__all__ = ["merge_projects"]


from pydough.relational import (
    ColumnReference,
    ExpressionSortInfo,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
)
from pydough.relational.rel_util import (
    transpose_expression,
)


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
    child_project: Project
    while True:
        if isinstance(node, RelationalRoot) and isinstance(node.input, Project):
            # TODO: add comment
            child_project = node.input
            if all(
                isinstance(expr, ColumnReference)
                for expr in child_project.columns.values()
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
                continue
        elif isinstance(node, Project) and isinstance(node.input, Project):
            # TODO: add comment
            child_project = node.input
        break
    return node
