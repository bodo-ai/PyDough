"""
A mixture of utility functions for relational nodes and expressions.
"""

__all__ = [
    "add_expr_uses",
    "bubble_uniqueness",
    "build_filter",
    "contains_window",
    "extract_equijoin_keys",
    "false_when_null_columns",
    "fetch_or_insert",
    "get_conjunctions",
    "only_references_columns",
    "partition_expressions",
    "passthrough_column_mapping",
    "transpose_expression",
]

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping

import pydough.pydough_operators as pydop
from pydough.types import BooleanType

from .relational_expressions import (
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    ExpressionSortInfo,
    LiteralExpression,
    RelationalExpression,
    WindowCallExpression,
)
from .relational_nodes import (
    Filter,
    Join,
    RelationalNode,
)

null_propagating_operators = {
    pydop.EQU,
    pydop.LET,
    pydop.LEQ,
    pydop.GRT,
    pydop.GEQ,
    pydop.LET,
    pydop.NEQ,
    pydop.STARTSWITH,
    pydop.ENDSWITH,
    pydop.CONTAINS,
    pydop.LIKE,
    pydop.LOWER,
    pydop.UPPER,
    pydop.LENGTH,
    pydop.YEAR,
    pydop.QUARTER,
    pydop.MONTH,
    pydop.DAY,
    pydop.HOUR,
    pydop.MINUTE,
    pydop.SECOND,
    pydop.DATETIME,
    pydop.DATEDIFF,
    pydop.JOIN_STRINGS,
    pydop.ADD,
    pydop.SUB,
    pydop.MUL,
    pydop.DIV,
}
"""
A set of operators with the property that the output is null if any of the
inputs are null.
"""


def get_conjunctions(expr: RelationalExpression) -> set[RelationalExpression]:
    """
    Extract conjunctions from the given expression.

    Args:
        `expr`: The expression to extract conjunctions from.

    Returns:
        The set of filter conditions whose conjunction forms `expr`.
    """
    if isinstance(expr, LiteralExpression) and expr.value:
        # If the expression is a true literal, there are no predicates as the
        # conjunction is always True.
        return set()
    elif isinstance(expr, CallExpression) and expr.op == pydop.BAN:
        # If the expression is an AND call, flatten to obtain the conjunction
        # by gathering the conjunction of all of the inputs.
        result = set()
        for arg in expr.inputs:
            result.update(get_conjunctions(arg))
        return result
    else:
        # Otherwise, the expression itself is the conjunction.
        return {expr}


def partition_expressions(
    expressions: Iterable[RelationalExpression],
    predicate: Callable[[RelationalExpression], bool],
) -> tuple[set[RelationalExpression], set[RelationalExpression]]:
    """
    Partition the given relational expressions into two sets based on the given
    predicate.

    Args:
        `expressions`: The expressions to partition.
        `predicate`: The predicate to use for partitioning.

    Returns:
        A tuple of two sets of expressions, the first of expressions that cause
        the predicate to return True and the second of the remainder.
    """
    true_expressions: set[RelationalExpression] = set()
    false_expressions: set[RelationalExpression] = set()
    for expr in expressions:
        if predicate(expr):
            true_expressions.add(expr)
        else:
            false_expressions.add(expr)
    return true_expressions, false_expressions


def only_references_columns(
    expr: RelationalExpression, allowed_columns: set[str]
) -> bool:
    """
    Checks if a relational expression contains only column references from the set of allowed columns.

    Args:
        `expr`: The expression to check.
        `allowed_columns`: The set of allowed columns.

    Returns:
        Whether `expr` meets the criteria.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return True
        case ColumnReference():
            return expr.name in allowed_columns
        case CallExpression():
            return all(
                only_references_columns(arg, allowed_columns) for arg in expr.inputs
            )
        case WindowCallExpression():
            return (
                all(
                    only_references_columns(arg, allowed_columns) for arg in expr.inputs
                )
                and all(
                    only_references_columns(arg, allowed_columns)
                    for arg in expr.partition_inputs
                )
                and all(
                    only_references_columns(order_arg.expr, allowed_columns)
                    for order_arg in expr.order_inputs
                )
            )
        case _:
            raise NotImplementedError(
                f"only_references_columns not implemented for {expr.__class__.__name__}"
            )


def false_when_null_columns(expr: RelationalExpression, null_columns: set[str]) -> bool:
    """
    Returns whether an expression is guaranteed to be False, as far as a filter
    is concerned, if certain columns are null.

    Args:
        `expr`: The expression to check.
        `null_columns`: The set of columns that are null.

    Returns:
        Whether `expr` meets the criteria.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return False
        case ColumnReference():
            return expr.name in null_columns
        case CallExpression():
            if expr.op in null_propagating_operators:
                return any(
                    false_when_null_columns(arg, null_columns) for arg in expr.inputs
                )
            return False
        case WindowCallExpression():
            return False
        case _:
            raise NotImplementedError(
                f"false_when_null_columns not implemented for {expr.__class__.__name__}"
            )


def contains_window(expr: RelationalExpression) -> bool:
    """
    Returns whether a relational expression contains a window function.

    Args:
        `expr`: The expression to check.

    Returns:
        Whether `expr` contains a window function.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference() | ColumnReference():
            return False
        case CallExpression():
            return any(contains_window(arg) for arg in expr.inputs)
        case WindowCallExpression():
            return True
        case _:
            raise NotImplementedError(
                f"contains_window not implemented for {expr.__class__.__name__}"
            )


def passthrough_column_mapping(node: RelationalNode) -> dict[str, RelationalExpression]:
    """
    Builds a mapping of column names to their corresponding column references
    for the given relational node.

    Args:
        `node`: The relational node to build the mapping from.

    Returns:
        A dictionary mapping column names to their corresponding column
        references from `node`.
    """
    result: dict[str, RelationalExpression] = {}
    for name, expr in node.columns.items():
        result[name] = ColumnReference(name, expr.data_type)
    return result


def build_filter(
    node: RelationalNode, filters: set[RelationalExpression]
) -> RelationalNode:
    """
    Build a filter node with the given filters on top of an input node.

    Args:
        `node`: The input node to build the filter on top of.
        `filters`: The set of filters to apply.

    Returns:
        A filter node with the given filters applied on top of `node`. If
        the set of filters is empty, just returns `node`. Ignores any filter
        condition that is always True.
    """
    filters.discard(LiteralExpression(True, BooleanType()))
    condition: RelationalExpression
    if len(filters) == 0:
        return node
    elif len(filters) == 1:
        condition = filters.pop()
    else:
        condition = CallExpression(pydop.BAN, BooleanType(), sorted(filters, key=repr))

    return Filter(node, condition, passthrough_column_mapping(node))


def transpose_expression(
    expr: RelationalExpression,
    columns: Mapping[str, RelationalExpression],
    keep_input_names: bool = False,
) -> RelationalExpression:
    """
    Rewrites an expression by replacing its column references based on a given
    column mapping, allowing the expression to be pushed beneath the node that
    introduced the mapping. For example, if a node renamed columns, this
    function translates the expression from the new column names back to the
    original names.

    Args:
        `expr`: The expression to transposed.
        `columns`: The mapping of column names to their corresponding
        expressions.
        `keep_input_names`: If True, keeps the input names in the column
        references.

    Returns:
        The transposed expression with updated column references.
    """
    match expr:
        case LiteralExpression() | CorrelatedReference():
            return expr
        case ColumnReference():
            new_column = columns.get(expr.name)
            assert new_column is not None
            if (
                isinstance(new_column, ColumnReference)
                and new_column.input_name is not None
                and not keep_input_names
            ):
                new_column = new_column.with_input(None)
            return new_column
        case CallExpression():
            return CallExpression(
                expr.op,
                expr.data_type,
                [
                    transpose_expression(arg, columns, keep_input_names)
                    for arg in expr.inputs
                ],
            )
        case WindowCallExpression():
            return WindowCallExpression(
                expr.op,
                expr.data_type,
                [
                    transpose_expression(arg, columns, keep_input_names)
                    for arg in expr.inputs
                ],
                [
                    transpose_expression(arg, columns, keep_input_names)
                    for arg in expr.partition_inputs
                ],
                [
                    ExpressionSortInfo(
                        transpose_expression(order_arg.expr, columns),
                        order_arg.ascending,
                        order_arg.nulls_first,
                    )
                    for order_arg in expr.order_inputs
                ],
                expr.kwargs,
            )
        case _:
            raise NotImplementedError(
                f"transpose_expression not implemented for {expr.__class__.__name__}"
            )


def add_expr_uses(
    expr: RelationalExpression,
    n_uses: defaultdict[RelationalExpression, int],
    top_level: bool,
) -> None:
    """
    Count the number of times nontrivial expressions are used in an expression
    and add them to a mapping of such counts. In this case, an expression is
    deemed nontrivial if it is a function call or a window function call.

    Args:
        `expr`: The expression to count the nontrivial expressions of.
        `n_uses`: A dictionary mapping column names to their reference counts.
        This is modified in-place by the function call.
        `bool`: If True, does not count the expression itself (only its
        subexpressions) because it is a top-level reference rather than a
        subexpression.
    """
    if isinstance(expr, CallExpression):
        if not top_level:
            n_uses[expr] += 1
        for arg in expr.inputs:
            add_expr_uses(arg, n_uses, False)
    if isinstance(expr, WindowCallExpression):
        if not top_level:
            n_uses[expr] += 1
        for arg in expr.inputs:
            add_expr_uses(arg, n_uses, False)
        for partition_arg in expr.partition_inputs:
            add_expr_uses(partition_arg, n_uses, False)
        for order_arg in expr.order_inputs:
            add_expr_uses(order_arg.expr, n_uses, False)


def extract_equijoin_keys(
    join: Join,
) -> tuple[list[ColumnReference], list[ColumnReference]]:
    """
    Extracts the equi-join keys from a join condition with two inputs.

    Args:
        `join`: the Join node whose condition is being parsed.

    Returns:
        A tuple where the first element are the equi-join keys from the LHS,
        and the second is a list of the the corresponding RHS keys.
    """
    assert len(join.inputs) == 2
    lhs_keys: list[ColumnReference] = []
    rhs_keys: list[ColumnReference] = []
    stack: list[RelationalExpression] = [*join.conditions]
    lhs_name: str | None = join.default_input_aliases[0]
    rhs_name: str | None = join.default_input_aliases[1]
    while stack:
        condition: RelationalExpression = stack.pop()
        if isinstance(condition, CallExpression):
            if condition.op == pydop.BAN:
                stack.extend(condition.inputs)
            elif condition.op == pydop.EQU and len(condition.inputs) == 2:
                lhs_input: RelationalExpression = condition.inputs[0]
                rhs_input: RelationalExpression = condition.inputs[1]
                if isinstance(lhs_input, ColumnReference) and isinstance(
                    rhs_input, ColumnReference
                ):
                    if (
                        lhs_input.input_name == lhs_name
                        and rhs_input.input_name == rhs_name
                    ):
                        lhs_keys.append(lhs_input)
                        rhs_keys.append(rhs_input)
                    elif (
                        lhs_input.input_name == rhs_name
                        and rhs_input.input_name == lhs_name
                    ):
                        lhs_keys.append(rhs_input)
                        rhs_keys.append(lhs_input)

    return lhs_keys, rhs_keys


def fetch_or_insert(
    dictionary: dict[str, RelationalExpression], value: RelationalExpression
) -> str:
    """
    Inserts a value into a dictionary with a new name, returning that new name,
    unless the value is already in the dictionary, in which case it returns the
    existing name.

    Args:
        `dictionary`: The dictionary to insert the value into / lookup from.
        `value`: The value to insert / lookup the name for.

    Returns:
        The name of the key that will map to `value` in the dictionary.
    """
    for name, col in dictionary.items():
        if col == value:
            return name
    idx: int = 0
    new_name: str = f"expr_{idx}"
    while new_name in dictionary:
        idx += 1
        new_name = f"expr_{idx}"
    dictionary[new_name] = value
    return new_name


def include_isomorphisms(
    unique_sets: set[frozenset[str]], isomorphisms: dict[str, set[str]]
) -> None:
    """
    Expands a set of uniqueness sets by transforming each uniqueness set to
    include any isomorphisms between column names. For example, if the
    uniqueness sets are as follows:

    `{
    {A, B},
    {C},
    {A, D},
    }`

    And the following isomorphisms are in place:

    `{A: {E}, B: {F}, C: {G, H}}`

    Then the uniqueness sets become the following:

    `{
    {A, B},
    {C},
    {A, D},
    {E, B},
    {E, D},
    {G},
    {H},
    {A, F},
    {E, F},
    }`

    The transformation is done in-place.

    Args:
        `unique_sets`: the input uniqueness sets to be transformed.
        `isomorphisms`: the mapping of column names to different column names
        whose values are identical to the column in question.
    """
    # Skip if there are no isomorphisms or uniqueness sets
    if len(isomorphisms) == 0 or len(unique_sets) == 0:
        return

    # Find all of the column names used by any of the uniqueness sets
    names_used_for_uniqueness: set[str] = set()
    for unique_set in unique_sets:
        names_used_for_uniqueness.update(unique_set)

    # For each column that has isomorphic columns, find all uniqueness sets
    # that contain the column, and add a copy with that column replaced with
    # its isomorphic alias into the uniqueness sets.
    for name, aliases in isomorphisms.items():
        if name in names_used_for_uniqueness:
            new_unique_sets: set[frozenset[str]] = set()
            for unique_set in unique_sets:
                if name in unique_set:
                    for alias in aliases:
                        new_unique_sets.add(
                            unique_set.difference({name}).union({alias})
                        )
            unique_sets.update(new_unique_sets)


def bubble_uniqueness(
    uniqueness: set[frozenset[str]],
    columns: dict[str, RelationalExpression],
    input_name: str | None,
) -> set[frozenset[str]]:
    """
    Helper function that bubbles up the uniqueness information from the input
    node to the output node.

    Args:
        `uniqueness`: the uniqueness information from the input node.
        `columns`: the columns of the output node.
        `input_name`: the name of the input node to bubble from.

    Returns:
        The bubbled up uniqueness information.
    """
    output_uniqueness: set[frozenset[str]] = set()
    # Build a mapping of every input column name to the corresponding output
    # column name, if the input is preserved in the output.
    reverse_mapping: dict[str, str] = {}
    for name, col in columns.items():
        if isinstance(col, ColumnReference) and col.input_name == input_name:
            reverse_mapping[col.name] = name
    # For each uniqueness set, transform all of its elements from input column
    # names to output column names. If any input column in the set is not part
    # of the output, then that set is discarded.
    for unique_set in uniqueness:
        can_add: bool = True
        new_uniqueness_set: set[str] = set()
        for col_name in unique_set:
            if col_name in reverse_mapping:
                new_uniqueness_set.add(reverse_mapping[col_name])
            else:
                can_add = False
                break
        if can_add:
            output_uniqueness.add(frozenset(new_uniqueness_set))
    # Build a mapping of each output column name to the set of all other
    # output column names that have identical values, then use this to build
    # any isomorphic uniqueness sets.
    isomorphisms: dict[str, set[str]] = {}
    for name1, col1 in columns.items():
        for name2, col2 in columns.items():
            if name1 != name2 and col1 == col2:
                isomorphisms[name1] = isomorphisms.get(name1, set()).union({name2})
                isomorphisms[name2] = isomorphisms.get(name2, set()).union({name1})
    include_isomorphisms(output_uniqueness, isomorphisms)
    return output_uniqueness
