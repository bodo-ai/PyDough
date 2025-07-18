"""
Logic used to simplify relational expressions in a relational node.
"""

__all__ = ["simplify_expressions"]


from enum import Enum

from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    EmptySingleton,
    ExpressionSortInfo,
    Filter,
    Join,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalNode,
    RelationalRoot,
    Scan,
    WindowCallExpression,
)
from pydough.relational.rel_util import (
    add_input_name,
)


class LogicalPredicate(Enum):
    """
    Enum representing logical predicates that can be inferred about relational
    expressions.
    """

    NOT_NULL = "NOT_NULL"
    NOT_NEGATIVE = "NOT_NEGATIVE"
    POSITIVE = "POSITIVE"


def run_simplification(
    expr: RelationalExpression,
    input_predicates: dict[RelationalExpression, set[LogicalPredicate]],
) -> tuple[RelationalExpression, set[LogicalPredicate]]:
    """
    Runs the simplification on a single expression, applying any predicates
    inferred from the input nodes to aid the process and inferring any new
    predicates that apply to the resulting expression.

    Args:
        `expr`: The expression to simplify.
        `input_predicates`: A dictionary mapping input columns to the set of
        predicates that are true for the column.

    Returns:
        The simplified expression and a set of predicates that apply to the
        resulting expression.
    """
    new_args: list[RelationalExpression]
    new_partitions: list[RelationalExpression]
    new_orders: list[ExpressionSortInfo]
    arg_predicates: list[set[LogicalPredicate]]
    output_predicates: set[LogicalPredicate] = set()

    if isinstance(expr, LiteralExpression):
        if expr.value is not None:
            output_predicates.add(LogicalPredicate.NOT_NULL)
            if isinstance(expr.value, (int, float)):
                if expr.value >= 0:
                    output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
                    if expr.value > 0:
                        output_predicates.add(LogicalPredicate.POSITIVE)

    if isinstance(expr, ColumnReference):
        output_predicates.update(input_predicates.get(expr, set()))

    if isinstance(expr, CallExpression):
        new_args = []
        arg_predicates = []
        for arg in expr.inputs:
            new_arg, new_preds = run_simplification(arg, input_predicates)
            new_args.append(new_arg)
            arg_predicates.append(new_preds)
        expr = CallExpression(expr.op, expr.data_type, new_args)

    if isinstance(expr, WindowCallExpression):
        new_args = []
        new_partitions = []
        new_orders = []
        arg_predicates = []
        for arg in expr.inputs:
            new_arg, new_preds = run_simplification(arg, input_predicates)
            new_args.append(new_arg)
            arg_predicates.append(new_preds)
        for partition in expr.partition_inputs:
            new_partition, _ = run_simplification(partition, input_predicates)
            new_partitions.append(new_partition)
        for order in expr.order_inputs:
            new_order, _ = run_simplification(order.expr, input_predicates)
            new_orders.append(
                ExpressionSortInfo(new_order, order.ascending, order.nulls_first)
            )
        expr = WindowCallExpression(
            expr.op,
            expr.data_type,
            new_args,
            new_partitions,
            new_orders,
            expr.kwargs,
        )
    return expr, output_predicates


def simplify_expressions(
    node: RelationalNode,
) -> dict[RelationalExpression, set[LogicalPredicate]]:
    """
    The main recursive procedure done to perform expression simplification on
    a relational node and its descendants. The transformation is done in-place

    Args:
        `node`: The relational node to perform simplification on.

    Returns:
        The predicates inferred from the output columns of the node.
    """
    # Recursively invoke the procedure on all inputs to the node.
    input_predicates: dict[RelationalExpression, set[LogicalPredicate]] = {}
    for idx, input_node in enumerate(node.inputs):
        input_alias: str | None = node.default_input_aliases[idx]
        predicates = simplify_expressions(input_node)
        for expr, preds in predicates.items():
            input_predicates[add_input_name(expr, input_alias)] = preds

    # Transform the expressions of the current node in-place.
    ref_expr: RelationalExpression
    output_predicates: dict[RelationalExpression, set[LogicalPredicate]] = {}
    match node:
        case (
            Project()
            | Filter()
            | Join()
            | Limit()
            | RelationalRoot()
            | Scan()
            | EmptySingleton()
        ):
            for name, expr in node.columns.items():
                ref_expr = ColumnReference(name, expr.data_type)
                node.columns[name], output_predicates[ref_expr] = run_simplification(
                    expr, input_predicates
                )
            if isinstance(node, (Filter, Join)):
                node._condition = run_simplification(node.condition, input_predicates)[
                    0
                ]
            if isinstance(node, (RelationalRoot, Limit)):
                node._orderings = [
                    ExpressionSortInfo(
                        run_simplification(order_expr.expr, input_predicates)[0],
                        order_expr.ascending,
                        order_expr.nulls_first,
                    )
                    for order_expr in node.orderings
                ]
            if isinstance(node, RelationalRoot):
                node._ordered_columns = [
                    (name, node.columns[name]) for name, _ in node.ordered_columns
                ]
        case Aggregate():
            for name, expr in node.keys.items():
                ref_expr = ColumnReference(name, expr.data_type)
                node.keys[name], output_predicates[ref_expr] = run_simplification(
                    expr, input_predicates
                )
                node.columns[name] = node.keys[name]
            for name, expr in node.aggregations.items():
                ref_expr = ColumnReference(name, expr.data_type)
                new_agg, output_predicates[ref_expr] = run_simplification(
                    expr, input_predicates
                )
                assert isinstance(new_agg, CallExpression)
                node.aggregations[name] = new_agg
                node.columns[name] = node.aggregations[name]

        # For all other nodes, do not perform any simplification.
        case _:
            pass

    return output_predicates
