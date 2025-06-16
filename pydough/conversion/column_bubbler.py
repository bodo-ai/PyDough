"""
Logic used to pull up names from leaf nodes further up in the tree, thus
reducing the number of aliases and simplifying the names in the final SQL,
while also removing duplicate calculations.
"""

__all__ = ["bubble_column_names"]


from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    ExpressionSortInfo,
    Filter,
    Join,
    Limit,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
)
from pydough.relational.rel_util import apply_substitution


def name_sort_key(name: str) -> tuple[bool, bool, str]:
    """
    TODO
    """
    return (
        name.startswith("expr") or name.startswith("agg"),
        any(char.isdigit() for char in name),
        name,
    )


def run_column_bubbling(
    node: RelationalNode,
) -> tuple[RelationalNode, dict[RelationalExpression, RelationalExpression]]:
    """
    TODO
    """
    remapping: dict[RelationalExpression, RelationalExpression] = {}
    output_columns: dict[str, RelationalExpression] = {}
    aliases: dict[RelationalExpression, RelationalExpression] = {}
    new_input: RelationalNode
    input_mapping: dict[RelationalExpression, RelationalExpression]
    old_expr: RelationalExpression
    new_expr: RelationalExpression
    new_ref: RelationalExpression
    result: RelationalNode
    match node:
        case Project() | Filter() | Limit():
            new_input, input_mapping = run_column_bubbling(node.input)
            for name in sorted(node.columns, key=name_sort_key):
                old_expr = node.columns[name]
                new_expr = apply_substitution(old_expr, input_mapping)
                new_ref = ColumnReference(name, old_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    if (
                        isinstance(new_expr, ColumnReference)
                        and name_sort_key(new_expr.name)[:2] <= name_sort_key(name)[:2]
                        and new_expr.name not in output_columns
                    ):
                        remapping[new_ref] = ColumnReference(
                            new_expr.name, new_expr.data_type
                        )
                        new_ref = remapping[new_ref]
                        name = new_expr.name
                    aliases[new_expr] = new_ref
                    output_columns[name] = new_expr
            if isinstance(node, Limit):
                new_orderings: list[ExpressionSortInfo] = []
                for ordering in node.orderings:
                    new_expr = apply_substitution(ordering.expr, input_mapping)
                    if new_expr in aliases:
                        new_expr = aliases[new_expr]
                    new_orderings.append(
                        ExpressionSortInfo(
                            new_expr, ordering.ascending, ordering.nulls_first
                        )
                    )
                node._orderings = new_orderings
            result = node.copy(output_columns, [new_input])
            if isinstance(result, Filter):
                result._condition = apply_substitution(result.condition, input_mapping)
            return result, remapping
        case Aggregate():
            new_input, input_mapping = run_column_bubbling(node.input)
            new_keys: dict[str, ColumnReference] = {}
            new_aggs: dict[str, CallExpression] = {}
            for name, key_expr in node.keys.items():
                new_expr = apply_substitution(key_expr, input_mapping)
                assert isinstance(new_expr, ColumnReference)
                new_ref = ColumnReference(name, key_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    new_keys[name] = new_expr
                    aliases[new_expr] = new_ref
            for name, call_expr in node.aggregations.items():
                new_expr = apply_substitution(call_expr, input_mapping)
                assert isinstance(new_expr, CallExpression)
                new_ref = ColumnReference(name, call_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    aliases[new_expr] = new_ref
                    new_aggs[name] = new_expr
            return Aggregate(new_input, new_keys, new_aggs), remapping
        case Scan():
            for name in sorted(node.columns, key=name_sort_key):
                new_expr = node.columns[name]
                new_ref = ColumnReference(name, new_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    if isinstance(new_expr, ColumnReference):
                        name = new_expr.name
                        remapping[new_ref] = new_ref = ColumnReference(
                            new_expr.name, new_expr.data_type
                        )
                    aliases[new_expr] = new_ref
                    output_columns[name] = new_expr
            return node.copy(output_columns), remapping
        case Join():
            new_left, left_mapping = run_column_bubbling(node.inputs[0])
            new_right, right_mapping = run_column_bubbling(node.inputs[1])
            input_mapping = {}
            for key, value in left_mapping.items():
                assert isinstance(key, ColumnReference)
                assert isinstance(value, ColumnReference)
                input_mapping[key.with_input(node.default_input_aliases[0])] = (
                    value.with_input(node.default_input_aliases[0])
                )
            for key, value in right_mapping.items():
                assert isinstance(key, ColumnReference)
                assert isinstance(value, ColumnReference)
                input_mapping[key.with_input(node.default_input_aliases[1])] = (
                    value.with_input(node.default_input_aliases[1])
                )
            for name in sorted(node.columns, key=name_sort_key):
                old_expr = node.columns[name]
                new_expr = apply_substitution(old_expr, input_mapping)
                new_ref = ColumnReference(name, old_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    if (
                        isinstance(new_expr, ColumnReference)
                        and name_sort_key(new_expr.name)[:2] <= name_sort_key(name)[:2]
                        and new_expr.name not in output_columns
                    ):
                        remapping[new_ref] = ColumnReference(
                            new_expr.name, new_expr.data_type
                        )
                        new_ref = remapping[new_ref]
                        name = new_expr.name
                    aliases[new_expr] = new_ref
                    output_columns[name] = new_expr
            result = node.copy(output_columns, [new_left, new_right])
            assert isinstance(result, Join)
            result.condition = apply_substitution(node.condition, input_mapping)
            return result, remapping
        case _:
            return node, remapping


def bubble_column_names(root: RelationalRoot) -> RelationalRoot:
    """
    TODO
    """
    new_input, column_remapping = run_column_bubbling(root.input)
    new_ordered_columns: list[tuple[str, RelationalExpression]] = []
    new_orderings: list[ExpressionSortInfo] | None = None
    for name, expr in root.ordered_columns:
        new_ordered_columns.append((name, apply_substitution(expr, column_remapping)))
    if root.orderings is not None:
        new_orderings = []
        for ordering in root.orderings:
            new_orderings.append(
                ExpressionSortInfo(
                    apply_substitution(ordering.expr, column_remapping),
                    ordering.ascending,
                    ordering.nulls_first,
                )
            )
    return RelationalRoot(new_input, new_ordered_columns, new_orderings)
