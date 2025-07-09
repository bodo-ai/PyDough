"""
Logic used to pull up projections in the relational plan so function calls
happen as late as possible, ideally after filters, filtering joins, and
aggregations.
"""

__all__ = ["pullup_projections"]


from pydough.relational import (
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    Filter,
    Join,
    JoinType,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    WindowCallExpression,
)
from pydough.relational.rel_util import apply_substitution, contains_window
from pydough.relational.relational_expressions.column_reference_finder import (
    ColumnReferenceFinder,
)

from .merge_projects import merge_adjacent_projects


def pull_non_columns(node: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    new_node_columns: dict[str, RelationalExpression] = {}
    new_project_columns: dict[str, RelationalExpression] = {}
    needs_pull: bool = False

    for name, expr in node.columns.items():
        new_node_columns[name] = expr
        match expr:
            case ColumnReference() | CorrelatedReference():
                new_project_columns[name] = ColumnReference(name, expr.data_type)
            case LiteralExpression() | CallExpression() | WindowCallExpression():
                new_project_columns[name] = expr
                needs_pull = True
            case _:
                raise NotImplementedError(
                    f"Unsupported expression type {expr.__class__.__name__} in `pull_non_columns` columns."
                )

    if not needs_pull:
        return node

    new_input: RelationalNode = node.copy(columns=new_node_columns)
    return Project(input=new_input, columns=new_project_columns)


def pull_project_into_join(node: Join, input_index: int) -> None:
    """
    TODO
    """
    if not isinstance(node.inputs[input_index], Project):
        return


def pull_project_into_filter(node: Filter) -> None:
    """
    TODO
    """
    if not isinstance(node.input, Project):
        return

    project: Project = node.input

    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    finder.reset()
    node.condition.accept(finder)
    condition_cols: set[ColumnReference] = finder.get_column_references()
    condition_names: set[str] = {col.name for col in condition_cols}

    ref_expr: ColumnReference
    new_ref: ColumnReference
    new_project_columns: dict[str, RelationalExpression] = {}
    used_cols: set[RelationalExpression] = set()
    transfer_substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for name, expr in project.columns.items():
        new_project_columns[name] = expr
        used_cols.add(expr)
    for name, expr in project.input.columns.items():
        ref_expr = ColumnReference(name, expr.data_type)
        if name in condition_names:
            continue
        if ref_expr not in used_cols:
            new_name: str = name
            idx: int = 0
            while new_name in new_project_columns:
                idx += 1
                new_name = f"{name}_{idx}"
            new_ref = ColumnReference(name, expr.data_type)
            new_project_columns[new_name] = new_ref
            transfer_substitutions[ref_expr] = new_ref

    node._input = project.copy(columns=new_project_columns)

    cond_contains_window: bool = contains_window(node.condition)
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    existing_outputs: set[RelationalExpression] = set(node.columns.values())
    new_filter_columns: dict[str, RelationalExpression] = {}
    for name, expr in project.columns.items():
        ref_expr = ColumnReference(name, expr.data_type)
        new_filter_columns[name] = expr
        new_expr: RelationalExpression = apply_substitution(
            expr, transfer_substitutions, {}
        )
        expr_contains_window: bool = contains_window(new_expr)
        if not (cond_contains_window and expr_contains_window):
            if name in condition_names:
                if ref_expr not in existing_outputs:
                    substitutions[ref_expr] = new_expr
            elif not expr_contains_window:
                new_filter_columns[name] = new_expr
    node._condition = apply_substitution(node.condition, substitutions, {})
    # node._columns = new_filter_columns


def pullup_projections(node: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    # Recursively invoke the procedure on all inputs to the node.
    node = node.copy(inputs=[pullup_projections(input) for input in node.inputs])
    match node:
        case RelationalRoot() | Project():
            return merge_adjacent_projects(node)
        case Join():
            pull_project_into_join(node, 0)
            if node.join_type == JoinType.INNER:
                pull_project_into_join(node, 1)
            return pull_non_columns(node)
        case Filter():
            pull_project_into_filter(node)
            return pull_non_columns(node)
        case _:
            return node
