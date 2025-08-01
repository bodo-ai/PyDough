"""
Logic used to merge adjacent projections in relational trees when convenient.
"""

__all__ = ["merge_projects"]


from pydough.relational import (
    Aggregate,
    ColumnReference,
    ColumnReferenceFinder,
    ExpressionSortInfo,
    Filter,
    Join,
    JoinType,
    Limit,
    Project,
    RelationalExpression,
    RelationalExpressionShuttle,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.rel_util import (
    ExpressionTranspositionShuttle,
    contains_window,
)


class JoinRemapShuttle(RelationalExpressionShuttle):
    """
    Same idea as `ExpressionTranspositionShuttle`, but for remapping
    column references in a join condition.
    """

    def __init__(
        self,
        left_renamings: dict[str, RelationalExpression],
        right_renamings: dict[str, RelationalExpression],
        input_names: list[str | None],
    ) -> None:
        self.left_renamings: dict[str, RelationalExpression] = left_renamings
        self.right_renamings: dict[str, RelationalExpression] = right_renamings
        self.input_names: list[str | None] = input_names

    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        if column_reference.input_name == self.input_names[0]:
            return self.left_renamings.get(column_reference.name, column_reference)
        elif column_reference.input_name == self.input_names[1]:
            return self.right_renamings.get(column_reference.name, column_reference)
        else:
            raise ValueError(f"Unexpected input name: {column_reference.input_name}")


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
            if join_input_index == 0 or join.join_type == JoinType.INNER:
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
    join.condition.accept(finder)
    col_references: set[ColumnReference] = finder.get_column_references()
    for input_ref in col_references:
        join_input_index = join.default_input_aliases.index(input_ref.input_name)
        new_ref: RelationalExpression = ColumnReference(
            input_ref.name, input_ref.data_type
        )
        new_input_col_sets[join_input_index][input_ref.name] = new_ref
    for name, expr in join.columns.items():
        if expr in col_references:
            assert isinstance(expr, ColumnReference)
            join_input_index = join.default_input_aliases.index(expr.input_name)
            pushable_columns[join_input_index].append(
                (name, ColumnReference(name, expr.data_type))
            )

    left_renamings: dict[str, RelationalExpression] = {}
    right_renamings: dict[str, RelationalExpression] = {}

    # The new columns of the join.
    new_columns: dict[str, RelationalExpression] = {}
    # For each join input, place all of the columns used from that input into
    # a join.
    transposer: ExpressionTranspositionShuttle = ExpressionTranspositionShuttle(
        join, False
    )
    for idx, join_input in enumerate(join.inputs):
        renaming: dict[str, RelationalExpression] = (left_renamings, right_renamings)[
            idx
        ]
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
            transposed_expr: RelationalExpression = expr.accept_shuttle(transposer)
            new_input_cols[input_expr_name] = transposed_expr
            new_columns[name] = ColumnReference(
                input_expr_name, expr.data_type, input_name=input_name
            )
            if isinstance(transposed_expr, ColumnReference):
                renaming[transposed_expr.name] = new_columns[name]
        # Create the projection on top of the join's input, unless the
        # projection would be a no-op
        if new_input_cols != join_input.columns:
            join.inputs[idx] = Project(join_input, new_input_cols)

    # Replace the original columns with the new columns, and update the join condition
    join_remap_shuttle: JoinRemapShuttle = JoinRemapShuttle(
        left_renamings, right_renamings, join.default_input_aliases
    )
    join.condition = join.condition.accept_shuttle(join_remap_shuttle)
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
    transposer: ExpressionTranspositionShuttle
    # Repeatedly attempt the merging protocol until the input of the node is
    # no longer a projection.
    while isinstance(node.input, Project):
        child_project: Project = node.input
        # The columns of the projection can be sucked into the parent
        # above it unless there is a window function in both.
        if not (
            any(contains_window(expr) for expr in child_project.columns.values())
            and any(contains_window(expr) for expr in node.columns.values())
        ):
            transposer = ExpressionTranspositionShuttle(child_project, False)
            if isinstance(node, RelationalRoot):
                # Replace all column references in the root's columns with
                # the expressions from the child projection.
                node._ordered_columns = [
                    (name, expr.accept_shuttle(transposer))
                    for name, expr in node.ordered_columns
                ]
                node._columns = dict(node.ordered_columns)
                # Do the same with the sort expressions.
                for sort_info in node.orderings:
                    sort_info.expr = sort_info.expr.accept_shuttle(transposer)
                # Delete the child projection from the tree, replacing it
                # with its input.
                node._input = child_project.input
                continue
            elif isinstance(node, Project):
                for name, expr in node.columns.items():
                    node.columns[name] = expr.accept_shuttle(transposer)
                # Delete the child projection from the tree, replacing it
                # with its input.
                node._input = child_project.input
                continue
        # Otherwise, halt the merging process since it is no longer
        # possible to merge the children of this project into it.
        break
    # Final round: if there is a project on top of a scan, aggregate, filter,
    # or limit that only does  column pruning/renaming, just push it into the
    # node.
    if (
        isinstance(node, Project)
        and isinstance(node.input, (Scan, Aggregate, Filter, Limit))
        and all(isinstance(expr, ColumnReference) for expr in node.columns.values())
    ):
        transposer = ExpressionTranspositionShuttle(node.input, False)
        # If the input is an aggregate, make sure to include its keys in the result.
        keys_used: set[str] = set()
        new_columns: dict[str, RelationalExpression] = {}
        for name, expr in node.columns.items():
            transposed_expr: RelationalExpression = expr.accept_shuttle(transposer)
            new_columns[name] = transposed_expr
            if isinstance(node.input, Aggregate) and isinstance(expr, ColumnReference):
                keys_used.add(expr.name)
        if isinstance(node.input, Aggregate):
            for key_name in node.input.keys:
                if key_name not in keys_used:
                    new_columns[key_name] = node.input.columns[key_name]
        return node.input.copy(columns=new_columns)
    # Alternatively: if the node is a root and it is on top of a limit, try to
    # suck the limit into the root.
    if isinstance(node, RelationalRoot) and isinstance(node.input, Limit):
        transposer = ExpressionTranspositionShuttle(node.input, False)
        new_orderings: list[ExpressionSortInfo] = [
            ExpressionSortInfo(
                ordering.expr.accept_shuttle(transposer),
                ordering.ascending,
                ordering.nulls_first,
            )
            for ordering in node.orderings
        ]
        if node.input.orderings == new_orderings:
            # If the orderings are the same, pull in the limit into the root.
            # Replace all column references in the root's columns with
            # the expressions from the child projection.
            node._ordered_columns = [
                (name, expr.accept_shuttle(transposer))
                for name, expr in node.ordered_columns
            ]
            node._columns = dict(node.ordered_columns)
            node._orderings = new_orderings
            node._limit = node.input.limit
            # Delete the child projection from the tree, replacing it
            # with its input.
            node._input = node.input.input
    return node


def merge_projects(
    node: RelationalNode, push_into_joins: bool = True
) -> RelationalNode:
    """
    Merge adjacent projections when beneficial.

    Args:
        `node`: The current node of the relational tree.
        `push_into_joins`: If True, push projections into joins when possible.

    Returns:
        The transformed version of `node` with adjacent projections merged
        into one when the top project never references nodes from the bottom
        more than once.
    """
    # If there is a project on top of a join, attempt to push it down into the
    # inputs of the join.
    if isinstance(node, Project) and isinstance(node.input, Join) and push_into_joins:
        node = project_join_transpose(node)

    # Recursively invoke the procedure on all inputs to the node.
    node = node.copy(
        inputs=[merge_projects(input, push_into_joins) for input in node.inputs]
    )

    # Invoke the main merging step if the current node is a root/projection,
    # potentially multiple times if the projection below it that gets deleted
    # reveals another projection below it.
    if isinstance(node, (RelationalRoot, Project)):
        node = merge_adjacent_projects(node)

    return node
