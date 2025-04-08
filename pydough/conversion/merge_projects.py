"""
Logic used to merge adjacent projections in relational trees when convenient.
"""

__all__ = ["merge_projects"]

from collections import defaultdict

from pydough.relational import (
    ColumnReference,
    ColumnReferenceFinder,
    ExpressionSortInfo,
    Join,
    JoinType,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.rel_util import (
    add_expr_uses,
    contains_window,
    transpose_expression,
)


def merging_doesnt_create_convolution(
    columns_a: dict[str, RelationalExpression],
    columns_b: dict[str, RelationalExpression],
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


def project_join_transpose(project: Project) -> RelationalNode:
    """
    Pushes a projection down into the inputs of a join if possible.

    Args:
        `project`: The project to be pushed down, which must be on top of a
        join.

    Returns:
        The modified join with the pushed down project, or the original project
        if it cannot be pushed down.
    """
    if any(contains_window(expr) for expr in project.columns.values()):
        # If the project contains window functions, do not push it down
        # as it may create incorrect results.
        return project

    assert isinstance(project.input, Join), (
        "Input must be a Join node for project_join_transpose."
    )
    join: Join = project.input
    pushable_columns: list[list[tuple[str, RelationalExpression]]] = [
        [] for _ in range(len(join.inputs))
    ]
    finder: ColumnReferenceFinder = ColumnReferenceFinder()

    # For every column, identify which side of the join it references, and add
    # its name to that index of `pushable_columns` if there is exactly one such
    # input side it refers to, thus marking the column expression as pushable
    # into that side.
    join_input_index: int
    for name, expr in project.columns.items():
        finder.reset()
        expr.accept(finder)
        join_input_indices: set[int] = set()
        for input_ref in finder.get_column_references():
            join_input_col: RelationalExpression = join.columns[input_ref.name]
            if not isinstance(join_input_col, ColumnReference):
                join_input_indices.clear()
                break
            join_input_indices.add(
                join.default_input_aliases.index(join_input_col.input_name)
            )
        if len(join_input_indices) == 1:
            join_input_index = join_input_indices.pop()
            # If the corresponding join is an inner join, mark the input as
            # pushable into that side.
            if (
                join_input_index == 0
                or join.join_types[join_input_index - 1] == JoinType.INNER
            ):
                pushable_columns[join_input_index].append((name, expr))

    # If not every column can be pushed, abandon the attempt
    if sum(len(columns) for columns in pushable_columns) < len(project.columns):
        return project

    # Add all input references used in the join condition to a list of columns
    # that must be preserved for each join input.
    new_input_col_sets: list[dict[str, RelationalExpression]] = [
        {} for _ in range(len(join.inputs))
    ]
    finder.reset()
    for cond in join.conditions:
        cond.accept(finder)
    for input_ref in finder.get_column_references():
        join_input_index = join.default_input_aliases.index(input_ref.input_name)
        new_input_col_sets[join_input_index][input_ref.name] = ColumnReference(
            input_ref.name, input_ref.data_type
        )

    # The new columns of the join.
    new_columns: dict[str, RelationalExpression] = {}

    # For each join input, place all of the columns used from that input into
    # a join.
    for idx, join_input in enumerate(join.inputs):
        input_name: str | None = join.default_input_aliases[idx]
        new_input_cols: dict[str, RelationalExpression] = new_input_col_sets[idx]
        for name, expr in pushable_columns[idx]:
            input_expr_name: str = name
            counter: int = 0
            # If the name already exists, create a new name for the column
            # expression in the new projection that will be part of the input.
            while input_expr_name in new_input_cols:
                if expr == new_input_cols[name]:
                    break
                input_expr_name += f"_{counter}"
                counter += 1
            # Add the expression to the projection for the referenced input,
            # and a reference to the new column in the join's columns.
            new_input_cols[input_expr_name] = transpose_expression(expr, join.columns)
            new_columns[name] = ColumnReference(
                input_expr_name, expr.data_type, input_name=input_name
            )
        # Create the projection on top of the join's input, unless the
        # projection would be a no-op
        if new_input_cols != join_input.columns:
            join.inputs[idx] = Project(join_input, new_input_cols)

    # Replace the original columns with the new columns.
    join._columns = new_columns
    return join


def merge_adjacent_projects(node: RelationalRoot | Project) -> RelationalNode:
    """
    Attempts to merge the projection input of a root/projection node into it,
    repeatedly if possible, or returns the original node if not possible. At
    the end, if the node is a project on top of a scan that only does
    column pruning/renaming, it is pushed into the scan.

    Args:
        `node`: The current node of the relational tree, which must be a root
        or projection.

    Returns:
        The transformed version of `node` with the projection input merged into
        it, or the original node if the merging is not possible.
    """
    expr: RelationalExpression
    new_expr: RelationalExpression
    # Repeatedly attempt the merging protocol until the input of the node is
    # no longer a projection.
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
            ) or merging_doesnt_create_convolution(node.columns, child_project.columns):
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
        return node.input.copy(
            columns={
                name: transpose_expression(expr, node.input.columns)
                for name, expr in node.columns.items()
            },
        )
    return node


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
    # If there is a project on top of a join, attempt to push it down into the
    # inputs of the join.
    if isinstance(node, Project) and isinstance(node.input, Join):
        node = project_join_transpose(node)

    # Recursively invoke the procedure on all inputs to the node.
    node = node.copy(inputs=[merge_projects(input) for input in node.inputs])

    # Invoke the main merging step if the current node is a root/projection,
    # potentially multiple times if the projection below it that gets deleted
    # reveals another projection below it.
    if isinstance(node, (RelationalRoot, Project)):
        node = merge_adjacent_projects(node)

    return node
