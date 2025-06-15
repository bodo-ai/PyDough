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
    Limit,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
)
from pydough.relational.rel_util import apply_substitution


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
    new_expr: RelationalExpression
    new_ref: RelationalExpression
    match node:
        case Project() | Filter() | Limit():
            new_input, input_mapping = run_column_bubbling(node.input)
            for name, expr in node.columns.items():
                new_expr = apply_substitution(expr, input_mapping)
                new_ref = ColumnReference(name, expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    output_columns[name] = new_expr
                    aliases[new_expr] = new_ref
            return node.copy(output_columns, [new_input]), remapping
        case Aggregate():
            new_input, input_mapping = run_column_bubbling(node.input)
            new_keys: dict[str, ColumnReference] = {}
            new_aggs: dict[str, CallExpression] = {}
            for name, expr in node.keys.items():
                new_expr = apply_substitution(expr, input_mapping)
                assert isinstance(new_expr, ColumnReference)
                new_ref = ColumnReference(name, expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    new_keys[name] = new_expr
                    aliases[new_expr] = new_ref
            for name, expr in node.aggregations.items():
                new_expr = apply_substitution(expr, input_mapping)
                assert isinstance(new_expr, CallExpression)
                new_ref = ColumnReference(name, expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    new_aggs[name] = new_expr
                    aliases[new_expr] = new_ref
            return Aggregate(new_input, new_keys, new_aggs), remapping
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
