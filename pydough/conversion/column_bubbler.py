"""
Logic used to pull up names from leaf nodes further up in the tree, thus
reducing the number of aliases and simplifying the names in the final SQL,
while also removing duplicate calculations.
"""

__all__ = ["bubble_column_names"]


import re

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
    Function that acts as a comparison key for column names when used in
    column bubbling, to determine which name is preferred to be kept.

    Args:
        `name`: The name whose sort key is being generated.

    Returns:
        A tuple that contains:
        - A boolean indicating if the name starts with "expr" or "agg".
        - A boolean indicating if the name contains any digits.
        - The name itself (used as a final tiebreaker for lexicographic
        comparison).
    """
    return (
        name.startswith("expr") or name.startswith("agg"),
        any(char.isdigit() for char in name),
        name,
    )


def generate_cleaner_names(expr: RelationalExpression, current_name: str) -> list[str]:
    """
    Generates more readable names for an expression based on its, if applicable.
    The patterns of name generation are:

    - If a function has a single input that is a column reference, the
      name is generated as `<function_name>_<column_name>`. For example,
      `SUM(sales)` would become `sum_sales`, and `AVG(num_cars_owned)`
      would become `avg_num_cars_owned`.
    - If an aggregation is a `COUNT` with no inputs, the name is simply
      `n_rows`, indicating the number of rows counted.
    - If the current name is in the form `name_idx`, try suggesting just `name`.

    If none of these conditions are met, the function returns an empty list.

    Args:
        `expr`: The function call expression for which to generate
        alternative names.
        `current_name`: The current name of the expression.

    Returns:
        A list of strings string representing the candidate generated names.
    """
    result: list[str] = []
    if isinstance(expr, CallExpression):
        if len(expr.inputs) == 1:
            input_expr = expr.inputs[0]
            if isinstance(input_expr, ColumnReference):
                result.append(f"{expr.op.function_name.lower()}_{input_expr.name}")
        if len(expr.inputs) == 0 and expr.op.function_name.lower() == "count":
            result.append("n_rows")

    if not (current_name.startswith("agg") or current_name.startswith("expr")):
        if re.match(r"^(.*)_[0-9]+$", current_name):
            result.append(re.findall(r"^(.*)_[0-9]+$", current_name)[0])
    return result


def run_column_bubbling(
    node: RelationalNode,
    corr_remap: dict[str, dict[RelationalExpression, RelationalExpression]],
) -> tuple[RelationalNode, dict[RelationalExpression, RelationalExpression]]:
    """
    Main recursive procedure for the column bubbling logic. This function
    traverses the relational tree, first running the procedure on the inputs to
    a node, then transforming the current node and forwarding the transformed
    version up to its parent.

    Args:
        `node`: The current node in the relational expression tree to process.
        `corr_remap`: A mapping of correlation names to column remappings from
        the left side of the corresponding join, used when transforming the
        right side of the same join to ensure that any renamed columns from the
        left side now use the new name in the right side.

    Returns:
        A tuple containing:
        - The transformed `RelationalNode` with bubbled column names.
        - A mapping of the original column references to their new
          references after the bubbling process was run on `node`.
    """

    # Mapping of the column references originally outputted by the node to the
    # new column reference that should be used to refer to that column after
    # it was renamed.
    remapping: dict[RelationalExpression, RelationalExpression] = {}

    # The new output columns to the function.
    output_columns: dict[str, RelationalExpression] = {}

    # A mapping of any expressions computed within the node to the column that
    # derives them.
    aliases: dict[RelationalExpression, RelationalExpression] = {}

    # A set of all names already used by the node (used to prevent collisions
    # when generating/replacing names)
    used_names: set[str] = set(node.columns)

    new_input: RelationalNode
    input_mapping: dict[RelationalExpression, RelationalExpression]
    old_expr: RelationalExpression
    new_expr: RelationalExpression
    new_ref: RelationalExpression
    result: RelationalNode
    alt_ref: ColumnReference
    match node:
        case Project() | Filter() | Limit():
            # For projection/filter/limit, recursively transform the single
            # input, then transform the output columns of the node accordingly.
            # Transform the output columns in order by the sort key so if there
            # are multiple names for the same calculation, the one with the
            # best name is used.
            new_input, input_mapping = run_column_bubbling(node.input, corr_remap)
            for name in sorted(node.columns, key=name_sort_key):
                old_expr = node.columns[name]
                # The substitution is applied to transform any renamed columns
                # from the input when used in the current node.
                new_expr = apply_substitution(old_expr, input_mapping, corr_remap)
                new_ref = ColumnReference(name, old_expr.data_type)
                if new_expr in aliases:
                    # If the column expression was already computed earlier in
                    # the loop, re-use the existing alias instead of keeping
                    # the new expression in the output.
                    remapping[new_ref] = aliases[new_expr]
                else:
                    # Otherwise, place the new expression in the output, but
                    # if it is a column reference then use the sort key to
                    # determine if the input name is better, and if so change
                    # the output name to the input name.
                    if (
                        isinstance(new_expr, ColumnReference)
                        and name_sort_key(new_expr.name)[:2] <= name_sort_key(name)[:2]
                        and new_expr.name not in used_names
                    ):
                        remapping[new_ref] = ColumnReference(
                            new_expr.name, new_expr.data_type
                        )
                        new_ref = remapping[new_ref]
                        name = new_expr.name
                        used_names.add(name)
                    # Try the same thing with generated alternative names
                    else:
                        for alt_name in generate_cleaner_names(new_expr, name):
                            if alt_name not in used_names:
                                remapping[new_ref] = ColumnReference(
                                    alt_name, new_expr.data_type
                                )
                                new_ref = remapping[new_ref]
                                name = alt_name
                                used_names.add(name)
                                break
                    aliases[new_expr] = new_ref
                    output_columns[name] = new_expr
            # For limit, also transform the orderings if they exist.
            if isinstance(node, Limit):
                new_orderings: list[ExpressionSortInfo] = []
                for ordering in node.orderings:
                    new_expr = apply_substitution(
                        ordering.expr, input_mapping, corr_remap
                    )
                    if new_expr in aliases:
                        new_expr = aliases[new_expr]
                    new_orderings.append(
                        ExpressionSortInfo(
                            new_expr, ordering.ascending, ordering.nulls_first
                        )
                    )
                node._orderings = new_orderings
            result = node.copy(output_columns, [new_input])
            # For Filter, also transform the condition.
            if isinstance(result, Filter):
                result._condition = apply_substitution(
                    result.condition, input_mapping, corr_remap
                )
            return result, remapping
        case Aggregate():
            # For aggregate, do the same as projection but run separately for
            # keys and aggregations.
            new_input, input_mapping = run_column_bubbling(node.input, corr_remap)
            new_keys: dict[str, RelationalExpression] = {}
            new_aggs: dict[str, CallExpression] = {}
            for name, key_expr in node.keys.items():
                new_expr = apply_substitution(key_expr, input_mapping, corr_remap)
                new_ref = ColumnReference(name, key_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    if (
                        isinstance(new_expr, ColumnReference)
                        and new_expr.name != name
                        and new_expr.name not in used_names
                    ):
                        used_names.add(new_expr.name)
                        alt_ref = ColumnReference(new_expr.name, new_expr.data_type)
                        remapping[new_ref] = alt_ref
                        new_ref = alt_ref
                        name = new_expr.name
                    new_keys[name] = new_expr
                    aliases[new_expr] = new_ref
            for name, call_expr in node.aggregations.items():
                new_expr = apply_substitution(call_expr, input_mapping, corr_remap)
                assert isinstance(new_expr, CallExpression)
                new_ref = ColumnReference(name, call_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    # Special case for aggregations: if the existing name is
                    # bad, try to replace it with a better name based on the
                    # function name and input column, if applicable.
                    for alt_name in generate_cleaner_names(new_expr, name):
                        if alt_name not in used_names:
                            used_names.add(alt_name)
                            alt_ref = ColumnReference(alt_name, call_expr.data_type)
                            remapping[new_ref] = alt_ref
                            new_ref = alt_ref
                            name = alt_name
                            break
                    aliases[new_expr] = new_ref
                    new_aggs[name] = new_expr
            return Aggregate(new_input, new_keys, new_aggs), remapping
        case Scan():
            # For scan, do the same as projection except there is no input to
            # transform first.
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
            # For join, first recursively transform the two inputs, then
            # combine the mappings from each into a single mapping with the
            # input aliases included. After the left side is transformed, add
            # a new entry to the correlation remap before handling the right
            # side, since the right side must know if any columns from the left
            # side were renamed.
            new_left, left_mapping = run_column_bubbling(node.inputs[0], corr_remap)
            if node.correl_name is not None:
                corr_remap[node.correl_name] = left_mapping
            new_right, right_mapping = run_column_bubbling(node.inputs[1], corr_remap)
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
            # Run the same logic as for projection, but with the combined
            # input mapping.
            for name in sorted(node.columns, key=name_sort_key):
                old_expr = node.columns[name]
                new_expr = apply_substitution(old_expr, input_mapping, corr_remap)
                new_ref = ColumnReference(name, old_expr.data_type)
                if new_expr in aliases:
                    remapping[new_ref] = aliases[new_expr]
                else:
                    if (
                        isinstance(new_expr, ColumnReference)
                        and name_sort_key(new_expr.name)[:2] <= name_sort_key(name)[:2]
                        and new_expr.name not in used_names
                    ):
                        remapping[new_ref] = ColumnReference(
                            new_expr.name, new_expr.data_type
                        )
                        new_ref = remapping[new_ref]
                        name = new_expr.name
                        used_names.add(name)
                    aliases[new_expr] = new_ref
                    output_columns[name] = new_expr
            result = node.copy(output_columns, [new_left, new_right])
            # Also transform the condition of the join using the input mapping.
            assert isinstance(result, Join)
            result.condition = apply_substitution(
                node.condition, input_mapping, corr_remap
            )
            return result, remapping
        case _:
            return node, remapping


def bubble_column_names(root: RelationalRoot) -> RelationalRoot:
    """
    Wrapper logic that invokes the column bubbling logic on the root node
    and remaps the input, ordered columns, and orderings accordingly.
    This function is used to simplify the names of columns in the final SQL
    and reduce the number of aliases by pulling up names from leaf nodes
    further up in the tree, removing duplicate calculations, and sometimes
    replacing names with more readable versions.

    Args:
        `root`: The root node of the relational expression tree.

    Returns:
        A new `RelationalRoot` with the bubbled column names and remapped
        input, ordered columns, and orderings.
    """
    new_input, column_remapping = run_column_bubbling(root.input, {})
    new_ordered_columns: list[tuple[str, RelationalExpression]] = []
    new_orderings: list[ExpressionSortInfo] | None = None
    for name, expr in root.ordered_columns:
        new_ordered_columns.append(
            (name, apply_substitution(expr, column_remapping, {}))
        )
    if root.orderings is not None:
        new_orderings = []
        for ordering in root.orderings:
            new_orderings.append(
                ExpressionSortInfo(
                    apply_substitution(ordering.expr, column_remapping, {}),
                    ordering.ascending,
                    ordering.nulls_first,
                )
            )
    return RelationalRoot(new_input, new_ordered_columns, new_orderings, root.limit)
