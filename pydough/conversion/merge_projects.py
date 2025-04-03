"""
Logic used to merge adjacent projections in relational trees when convenient.
"""

__all__ = ["merge_projects"]

from collections import defaultdict
from collections.abc import MutableMapping

from pydough.relational import (
    ColumnReference,
    ExpressionSortInfo,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.rel_util import (
    add_expr_uses,
    transpose_expression,
)


def merging_doesnt_create_convolution(
    columns_a: MutableMapping[str, RelationalExpression],
    columns_b: MutableMapping[str, RelationalExpression],
) -> bool:
    """
    Confirms whether merging two projections results in any complex expressions
    being calculated more than once, ignoring any top-level expressions.
    """
    n_uses: defaultdict[RelationalExpression, int] = defaultdict(int)
    for expr in columns_a.values():
        expr = transpose_expression(expr, columns_b)
        add_expr_uses(expr, n_uses, top_level=True)
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
    # Recursively invoke the procedure on all inputs to the node.
    for idx, input in enumerate(node.inputs):
        node.inputs[idx] = merge_projects(input)
    expr: RelationalExpression
    new_expr: RelationalExpression
    # Invoke the main merging step if the current node is a root/projection,
    # potentially multiple times if the projection below it that gets deleted
    # reveals another projection below it.
    if isinstance(node, (RelationalRoot, Project)):
        while isinstance(node.input, Project):
            child_project: Project = node.input
            if isinstance(node, RelationalRoot):
                # The columns of the projection can be sucked into the root
                # above it if they are all pass-through/renamings, or if there
                # is no convolution created (only allowed if there are no
                # ordering expressions).
                if all(
                    isinstance(expr, ColumnReference)
                    for expr in child_project.columns.values()
                ) or (
                    len(node.orderings) == 0
                    and merging_doesnt_create_convolution(
                        node.columns, child_project.columns
                    )
                ):
                    # Replace all column references in the root's columns with
                    # the expressions from the child projection..
                    for idx, (name, expr) in enumerate(node.ordered_columns):
                        assert isinstance(expr, ColumnReference)
                        new_expr = transpose_expression(expr, child_project.columns)
                        node.columns[name] = new_expr
                        node.ordered_columns[idx] = (name, new_expr)
                    # Do the same with the sort expressions.
                    for idx, sort_info in enumerate(node.orderings):
                        new_expr = transpose_expression(
                            sort_info.expr, child_project.columns
                        )
                        node.orderings[idx] = ExpressionSortInfo(
                            new_expr, sort_info.ascending, sort_info.nulls_first
                        )
                    # Delete the child projection from the tree, replacing it
                    # with its input.
                    node._input = child_project.input
                else:
                    # Otherwise, halt the merging process since it is no longer
                    # possible to merge the children of this root into it.
                    break
            elif isinstance(node, Project):
                # The columns of the projection can be sucked into the
                # projection above it if they are all pass-through/renamings
                # or if there is no convolution created.
                if all(
                    isinstance(expr, ColumnReference)
                    for expr in child_project.columns.values()
                ) or merging_doesnt_create_convolution(
                    node.columns, child_project.columns
                ):
                    for name, expr in node.columns.items():
                        new_expr = transpose_expression(expr, child_project.columns)
                        node.columns[name] = new_expr
                    # Delete the child projection from the tree, replacing it
                    # with its input.
                    node._input = child_project.input
                else:
                    # Otherwise, halt the merging process since it is no longer
                    # possible to merge the children of this project into it.
                    break
    # Final round: if there is a project on top of a scan that only does
    # column pruning/renaming, just push it into the scan.
    if (
        isinstance(node, Project)
        and isinstance(node.input, Scan)
        and all(isinstance(expr, ColumnReference) for expr in node.columns.values())
    ):
        return Scan(
            node.input.table_name,
            {
                name: transpose_expression(expr, node.input.columns)
                for name, expr in node.columns.items()
            },
        )
    return node
