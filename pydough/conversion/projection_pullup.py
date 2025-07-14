"""
Logic used to pull up projections in the relational plan so function calls
happen as late as possible, ideally after filters, filtering joins, and
aggregations.
"""

__all__ = ["pullup_projections"]


import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
)
from pydough.relational.rel_util import (
    add_input_name,
    apply_substitution,
    contains_window,
    transpose_expression,
)
from pydough.relational.relational_expressions.column_reference_finder import (
    ColumnReferenceFinder,
)
from pydough.types import NumericType

from .merge_projects import merge_adjacent_projects


def widen_columns(
    node: RelationalNode,
) -> dict[RelationalExpression, RelationalExpression]:
    """
    TODO
    """
    existing_vals: dict[RelationalExpression, RelationalExpression] = {
        expr: ColumnReference(name, expr.data_type)
        for name, expr in node.columns.items()
    }
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for input_idx in range(len(node.inputs)):
        input_alias: str | None = node.default_input_aliases[input_idx]
        input_node: RelationalNode = node.inputs[input_idx]
        for name, expr in input_node.columns.items():
            if isinstance(node, Join):
                expr = add_input_name(expr, input_alias)
            ref_expr: ColumnReference = ColumnReference(
                name, expr.data_type, input_name=input_alias
            )
            if expr not in existing_vals:
                new_name: str = name
                idx: int = 0
                while new_name in node.columns:
                    idx += 1
                    new_name = f"{name}_{idx}"
                new_ref: ColumnReference = ColumnReference(new_name, expr.data_type)
                node.columns[new_name] = ref_expr
                existing_vals[expr] = ref_expr
                substitutions[ref_expr] = new_ref
            else:
                substitutions[ref_expr] = existing_vals[expr]
    return {k: v for k, v in substitutions.items() if k != v}


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
    substitutions = {k: add_input_name(v, None) for k, v in substitutions.items()}
    for name, expr in new_project_columns.items():
        new_project_columns[name] = apply_substitution(expr, substitutions, {})

    return Project(input=node, columns=new_project_columns)


def pull_project_into_join(node: Join, input_index: int) -> None:
    """
    TODO
    """
    if not isinstance(node.inputs[input_index], Project):
        return

    project = node.inputs[input_index]
    assert isinstance(project, Project)

    input_name: str | None = node.default_input_aliases[input_index]

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
        new_expr: RelationalExpression = add_input_name(
            apply_substitution(expr, transfer_substitutions, {}), input_name
        )
        if (not contains_window(new_expr)) and (
            (name in condition_names) != (name in output_names)
        ):
            ref_expr: ColumnReference = ColumnReference(
                name, expr.data_type, input_name=input_name
            )
            substitutions[ref_expr] = new_expr

    node._condition = apply_substitution(node.condition, substitutions, {})
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }


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
            substitutions[ref_expr] = new_expr
    node._condition = apply_substitution(node.condition, substitutions, {})
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }


def pull_project_into_limit(node: Limit) -> None:
    """
    TODO
    """
    if not isinstance(node.input, Project):
        return

    project: Project = node.input

    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    finder.reset()
    for expr in node.columns.values():
        expr.accept(finder)
    output_cols: set[ColumnReference] = finder.get_column_references()
    output_names: set[str] = {col.name for col in output_cols}

    finder.reset()
    for order_expr in node.orderings:
        order_expr.expr.accept(finder)
    order_cols: set[ColumnReference] = finder.get_column_references()
    order_names: set[str] = {col.name for col in order_cols}

    transfer_substitutions: dict[RelationalExpression, RelationalExpression] = (
        widen_columns(project)
    )
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    for name, expr in project.columns.items():
        new_expr: RelationalExpression = apply_substitution(
            expr, transfer_substitutions, {}
        )
        if (not contains_window(new_expr)) and (
            (name in output_names) != (name in order_names)
        ):
            ref_expr: ColumnReference = ColumnReference(name, expr.data_type)
            substitutions[ref_expr] = new_expr
    node._columns = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.columns.items()
    }
    node._orderings = [
        ExpressionSortInfo(
            apply_substitution(order_expr.expr, substitutions, {}),
            order_expr.ascending,
            order_expr.nulls_first,
        )
        for order_expr in node.orderings
    ]


def pull_project_into_aggregate(node: Aggregate) -> RelationalNode:
    """
    TODO
    """
    if not isinstance(node.input, Project):
        return node

    project: Project = node.input

    finder: ColumnReferenceFinder = ColumnReferenceFinder()
    finder.reset()
    for key_expr in node.aggregations.values():
        key_expr.accept(finder)
    agg_cols: set[ColumnReference] = finder.get_column_references()
    agg_names: set[str] = {col.name for col in agg_cols}
    finder.reset()
    for agg_expr in node.keys.values():
        agg_expr.accept(finder)
    key_cols: set[ColumnReference] = finder.get_column_references()
    key_names: set[str] = {col.name for col in key_cols}

    transfer_substitutions: dict[RelationalExpression, RelationalExpression] = (
        widen_columns(project)
    )
    substitutions: dict[RelationalExpression, RelationalExpression] = {}
    new_expr: RelationalExpression
    for name, expr in project.columns.items():
        new_expr = apply_substitution(expr, transfer_substitutions, {})
        if (not contains_window(new_expr)) and (
            (name in agg_names) != (name in key_names)
        ):
            ref_expr: ColumnReference = ColumnReference(name, expr.data_type)
            substitutions[ref_expr] = new_expr
    new_keys: dict[str, RelationalExpression] = {
        name: apply_substitution(expr, substitutions, {})
        for name, expr in node.keys.items()
    }
    new_aggs: dict[str, CallExpression] = {}
    for name, expr in node.aggregations.items():
        new_expr = apply_substitution(expr, substitutions, {})
        assert isinstance(new_expr, CallExpression)
        new_aggs[name] = simplify_agg(new_expr)
    return Aggregate(
        input=node.input,
        keys=new_keys,
        aggregations=new_aggs,
    )


def simplify_agg(agg: CallExpression) -> CallExpression:
    """
    TODO
    """
    arg: RelationalExpression
    if agg.op == pydop.SUM:
        arg = agg.inputs[0]
        if (
            isinstance(arg, LiteralExpression)
            and isinstance(arg.data_type, NumericType)
            and arg.value == 1
        ):
            return CallExpression(
                op=pydop.COUNT,
                return_type=agg.data_type,
                inputs=[],
            )
    # In all other cases, we just return the aggregation as is.
    return agg


def merge_adjacent_aggregations(node: Aggregate) -> Aggregate:
    """
    TODO
    """
    if not isinstance(node.input, Aggregate):
        return node

    input_agg: Aggregate = node.input

    top_keys: set[RelationalExpression] = {
        transpose_expression(expr, input_agg.columns) for expr in node.keys.values()
    }
    bottom_keys: set[RelationalExpression] = set(input_agg.keys.values())

    # print()
    # print("Top keys:")
    # for key in top_keys:
    #     print(f"  {key.to_string(True)}")
    # print("Bottom keys:")
    # for key in bottom_keys:
    #     print(f"  {key.to_string(True)}")

    if len(top_keys - bottom_keys) > 0:
        return node

    bottom_only_keys: set[RelationalExpression] = bottom_keys - top_keys

    new_keys: dict[str, RelationalExpression] = {
        name: transpose_expression(expr, input_agg.columns)
        for name, expr in node.keys.items()
    }
    new_aggs: dict[str, CallExpression] = {}
    input_expr: RelationalExpression
    for agg_name, agg_expr in node.aggregations.items():
        match agg_expr.op:
            case pydop.COUNT if len(agg_expr.inputs) == 0:
                if len(bottom_only_keys) == 0:
                    new_aggs[agg_name] = CallExpression(
                        op=pydop.ANYTHING,
                        return_type=agg_expr.data_type,
                        inputs=[LiteralExpression(1, agg_expr.data_type)],
                    )
                elif len(bottom_only_keys) == 1:
                    new_aggs[agg_name] = CallExpression(
                        op=pydop.NDISTINCT,
                        return_type=agg_expr.data_type,
                        inputs=[next(iter(bottom_only_keys))],
                    )
                else:
                    return node
            case pydop.SUM:
                input_expr = transpose_expression(agg_expr.inputs[0], input_agg.columns)
                if isinstance(input_expr, CallExpression) and input_expr.op in (
                    pydop.SUM,
                    pydop.COUNT,
                ):
                    new_aggs[agg_name] = input_expr
                else:
                    return node
            case _:
                return node

    return Aggregate(
        input=input_agg.input,
        keys=new_keys,
        aggregations=new_aggs,
    )


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
        case Limit():
            pull_project_into_limit(node)
            return pull_non_columns(node)
        case Aggregate():
            node = merge_adjacent_aggregations(node)
            return pull_project_into_aggregate(node)
        case _:
            return node
