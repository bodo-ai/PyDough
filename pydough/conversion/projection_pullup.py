"""
Logic used to pull up projections in the relational plan so function calls
happen as late as possible, ideally after filters, filtering joins, and
aggregations.
"""

__all__ = ["pullup_projections"]


from pydough.relational import (
    ColumnReference,
    Filter,
    Join,
    JoinType,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
)
from pydough.relational.rel_util import apply_substitution, contains_window
from pydough.relational.relational_expressions.column_reference_finder import (
    ColumnReferenceFinder,
)

from .merge_projects import merge_adjacent_projects


def widen_columns(
    node: RelationalNode,
) -> dict[RelationalExpression, RelationalExpression]:
    """
    TODO
    """
    existing_vals: set[RelationalExpression] = set(node.columns.values())
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for input_idx in range(len(node.inputs)):
        input_node: RelationalNode = node.inputs[input_idx]
        for name, expr in input_node.columns.items():
            ref_expr: ColumnReference = ColumnReference(
                name, expr.data_type, input_name=node.default_input_aliases[input_idx]
            )
            if expr not in existing_vals:
                new_name: str = name
                idx: int = 0
                while new_name in node.columns:
                    idx += 1
                    new_name = f"{name}_{idx}"
                new_ref: ColumnReference = ColumnReference(new_name, expr.data_type)
                node.columns[new_name] = ref_expr
                substitutions[ref_expr] = new_ref
    return substitutions


def pull_non_columns(node: RelationalNode) -> RelationalNode:
    """
    TODO
    """
    new_project_columns: dict[str, RelationalExpression] = {}
    needs_pull: bool = False

    for name, expr in node.columns.items():
        if isinstance(expr, ColumnReference):
            new_project_columns[name] = ColumnReference(name, expr.data_type)
        else:
            new_project_columns[name] = expr
            needs_pull = True

    if not needs_pull:
        return node

    substitutions: dict[RelationalExpression, RelationalExpression] = widen_columns(
        node
    )
    for name, expr in new_project_columns.items():
        new_project_columns[name] = apply_substitution(expr, substitutions, {})

    return Project(input=node, columns=new_project_columns)


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
    finder.reset()
    for expr in node.columns.values():
        expr.accept(finder)
    output_cols: set[ColumnReference] = finder.get_column_references()
    output_names: set[str] = {col.name for col in output_cols}

    transfer_substitutions: dict[RelationalExpression, RelationalExpression] = (
        widen_columns(project)
    )
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for name, expr in project.columns.items():
        new_expr: RelationalExpression = apply_substitution(
            expr, transfer_substitutions, {}
        )
        if (not contains_window(new_expr)) and (
            (name in condition_names) != (name in output_names)
        ):
            ref_expr: ColumnReference = ColumnReference(name, expr.data_type)
            substitutions[ref_expr] = apply_substitution(
                expr, transfer_substitutions, {}
            )
    node._condition = apply_substitution(node.condition, substitutions, {})
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }


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
