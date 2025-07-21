"""
Logic used to simplify relational expressions in a relational node.
"""

__all__ = ["simplify_expressions"]


from enum import Enum

import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    EmptySingleton,
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


NULL_PROPAGATING_OPS: set[pydop.PyDoughOperator] = {
    pydop.ADD,
    pydop.SUB,
    pydop.MUL,
    pydop.BAN,
    pydop.BOR,
    pydop.NOT,
    pydop.LOWER,
    pydop.UPPER,
    pydop.LENGTH,
    pydop.STRIP,
    pydop.REPLACE,
    pydop.FIND,
    pydop.ABS,
    pydop.CEIL,
    pydop.FLOOR,
    pydop.ROUND,
    pydop.EQU,
    pydop.NEQ,
    pydop.GEQ,
    pydop.GRT,
    pydop.LET,
    pydop.LEQ,
    pydop.BXR,
    pydop.STARTSWITH,
    pydop.ENDSWITH,
    pydop.CONTAINS,
    pydop.LIKE,
    pydop.SIGN,
    pydop.SMALLEST,
    pydop.LARGEST,
    pydop.IFF,
    pydop.YEAR,
    pydop.MONTH,
    pydop.DAY,
    pydop.HOUR,
    pydop.MINUTE,
    pydop.SECOND,
    pydop.DATEDIFF,
    pydop.DAYNAME,
    pydop.DAYOFWEEK,
    pydop.SLICE,
    pydop.LPAD,
    pydop.RPAD,
    pydop.MONOTONIC,
    pydop.JOIN_STRINGS,
}


def simplify_function_call(
    expr: CallExpression,
    arg_predicates: list[set[LogicalPredicate]],
    no_group_aggregate: bool,
) -> tuple[RelationalExpression, set[LogicalPredicate]]:
    """
    TODO
    """
    output_expr: RelationalExpression = expr
    output_predicates: set[LogicalPredicate] = set()
    if expr.op in NULL_PROPAGATING_OPS:
        if all(LogicalPredicate.NOT_NULL in preds for preds in arg_predicates):
            output_predicates.add(LogicalPredicate.NOT_NULL)
    match expr.op:
        case pydop.COUNT | pydop.NDISTINCT:
            output_predicates.add(LogicalPredicate.NOT_NULL)
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
            if not no_group_aggregate:
                if (
                    len(expr.inputs) == 0
                    or LogicalPredicate.NOT_NULL in arg_predicates[0]
                ):
                    output_predicates.add(LogicalPredicate.POSITIVE)
            elif (
                expr.op == pydop.COUNT
                and len(expr.inputs) == 1
                and LogicalPredicate.NOT_NULL in arg_predicates[0]
            ):
                output_predicates.add(LogicalPredicate.POSITIVE)
                output_expr = CallExpression(pydop.COUNT, expr.data_type, [])
        case (
            pydop.SUM
            | pydop.AVG
            | pydop.MIN
            | pydop.MAX
            | pydop.ANYTHING
            | pydop.MEDIAN
            | pydop.QUANTILE
        ):
            for predicate in [
                LogicalPredicate.NOT_NEGATIVE,
                LogicalPredicate.POSITIVE,
            ]:
                if predicate in arg_predicates[0]:
                    output_predicates.add(predicate)
            if (
                LogicalPredicate.NOT_NULL in arg_predicates[0]
                and not no_group_aggregate
            ):
                output_predicates.add(LogicalPredicate.NOT_NULL)
        case pydop.ADD | pydop.MUL | pydop.DIV:
            for predicate in [LogicalPredicate.NOT_NEGATIVE, LogicalPredicate.POSITIVE]:
                if all(predicate in preds for preds in arg_predicates):
                    output_predicates.add(predicate)
            if expr.op == pydop.DIV:
                if (
                    LogicalPredicate.NOT_NULL in arg_predicates[0]
                    and LogicalPredicate.NOT_NULL in arg_predicates[1]
                    and LogicalPredicate.POSITIVE in arg_predicates[1]
                ):
                    output_predicates.add(LogicalPredicate.NOT_NULL)
        case pydop.DEFAULT_TO:
            if LogicalPredicate.NOT_NULL in arg_predicates[0]:
                output_expr = expr.inputs[0]
                output_predicates = arg_predicates[0]
            else:
                if any(LogicalPredicate.NOT_NULL in preds for preds in arg_predicates):
                    output_predicates.add(LogicalPredicate.NOT_NULL)
                for pred in arg_predicates[0]:
                    if all(pred in preds for preds in arg_predicates):
                        output_predicates.add(pred)
        case pydop.ABS:
            if (
                LogicalPredicate.POSITIVE in arg_predicates[0]
                or LogicalPredicate.NOT_NEGATIVE in arg_predicates[0]
            ):
                output_expr = expr.inputs[0]
                output_predicates = arg_predicates[0]
            else:
                output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
        case (
            pydop.LENGTH
            | pydop.BAN
            | pydop.BOR
            | pydop.BXR
            | pydop.STARTSWITH
            | pydop.ENDSWITH
            | pydop.CONTAINS
            | pydop.LIKE
            | pydop.SQRT
            | pydop.MONOTONIC
        ):
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
        case pydop.EQU | pydop.NEQ | pydop.GEQ | pydop.GRT | pydop.LET | pydop.LEQ:
            match (expr.op, expr.inputs[1]):
                case (pydop.GRT, LiteralExpression()) if (
                    expr.inputs[1].value == 0
                    and LogicalPredicate.POSITIVE in arg_predicates[0]
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                    output_predicates.add(LogicalPredicate.NOT_NULL)
                    output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
                    output_predicates.add(LogicalPredicate.POSITIVE)
                case (pydop.GEQ, LiteralExpression()) if (
                    expr.inputs[1].value == 0
                    and LogicalPredicate.NOT_NEGATIVE in arg_predicates[0]
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                    output_predicates.add(LogicalPredicate.NOT_NULL)
                    output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
                    output_predicates.add(LogicalPredicate.POSITIVE)
                case _:
                    pass
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)

        case pydop.PRESENT:
            if LogicalPredicate.NOT_NULL in arg_predicates[0]:
                output_expr = LiteralExpression(True, expr.data_type)
            output_predicates.add(LogicalPredicate.NOT_NULL)
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
        case pydop.ABSENT:
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
        case pydop.IFF:
            if isinstance(expr.inputs[0], LiteralExpression):
                if bool(expr.inputs[0].value):
                    output_expr = expr.inputs[1]
                    output_predicates = arg_predicates[1]
                else:
                    output_expr = expr.inputs[2]
                    output_predicates = arg_predicates[2]
            elif (
                LogicalPredicate.POSITIVE in arg_predicates[0]
                and LogicalPredicate.NOT_NULL in arg_predicates[0]
            ):
                output_expr = expr.inputs[1]
                output_predicates = arg_predicates[1]
            else:
                output_predicates = arg_predicates[1] & arg_predicates[2]
        case pydop.KEEP_IF:
            if isinstance(expr.inputs[1], LiteralExpression):
                if bool(expr.inputs[1].value):
                    output_expr = expr.inputs[0]
                    output_predicates = arg_predicates[0]
                else:
                    output_expr = LiteralExpression(None, expr.data_type)
                    output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
            elif (
                LogicalPredicate.POSITIVE in arg_predicates[1]
                and LogicalPredicate.NOT_NULL in arg_predicates[1]
            ):
                output_expr = expr.inputs[0]
                output_predicates = arg_predicates[0]
            elif LogicalPredicate.NOT_NEGATIVE in arg_predicates[0]:
                output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
    return output_expr, output_predicates


def simplify_window_call(
    expr: WindowCallExpression,
    arg_predicates: list[set[LogicalPredicate]],
) -> tuple[RelationalExpression, set[LogicalPredicate]]:
    """
    TODO
    """
    output_predicates: set[LogicalPredicate] = set()
    no_frame: bool = not (
        expr.kwargs.get("cumulative", False) or "frame" in expr.kwargs
    )
    match expr.op:
        case pydop.RANKING | pydop.PERCENTILE:
            output_predicates.add(LogicalPredicate.NOT_NULL)
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
            output_predicates.add(LogicalPredicate.POSITIVE)
        case pydop.RELSUM | pydop.RELAVG:
            if LogicalPredicate.NOT_NULL in arg_predicates[0] and no_frame:
                output_predicates.add(LogicalPredicate.NOT_NULL)
            if LogicalPredicate.NOT_NEGATIVE in arg_predicates[0]:
                output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
            if LogicalPredicate.POSITIVE in arg_predicates[0] and no_frame:
                output_predicates.add(LogicalPredicate.POSITIVE)
        case pydop.RELSIZE:
            if no_frame:
                output_predicates.add(LogicalPredicate.NOT_NULL)
                output_predicates.add(LogicalPredicate.POSITIVE)
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
        case pydop.RELCOUNT:
            if no_frame:
                output_predicates.add(LogicalPredicate.NOT_NULL)
                if LogicalPredicate.NOT_NULL in arg_predicates[0]:
                    output_predicates.add(LogicalPredicate.POSITIVE)
            output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
    return expr, output_predicates


def infer_literal_predicates(expr: LiteralExpression) -> set[LogicalPredicate]:
    """
    Infers logical predicates from a literal expression.

    Args:
        `expr`: The literal expression to infer predicates from.

    Returns:
        A set of logical predicates inferred from the literal.
    """
    output_predicates: set[LogicalPredicate] = set()
    if expr.value is not None:
        output_predicates.add(LogicalPredicate.NOT_NULL)
        if isinstance(expr.value, (int, float)):
            if expr.value >= 0:
                output_predicates.add(LogicalPredicate.NOT_NEGATIVE)
                if expr.value > 0:
                    output_predicates.add(LogicalPredicate.POSITIVE)
    return output_predicates


def run_simplification(
    expr: RelationalExpression,
    input_predicates: dict[RelationalExpression, set[LogicalPredicate]],
    no_group_aggregate: bool,
) -> tuple[RelationalExpression, set[LogicalPredicate]]:
    """
    Runs the simplification on a single expression, applying any predicates
    inferred from the input nodes to aid the process and inferring any new
    predicates that apply to the resulting expression.

    Args:
        `expr`: The expression to simplify.
        `input_predicates`: A dictionary mapping input columns to the set of
        predicates that are true for the column.
        `no_group_aggregate`: A boolean indicating whether the expression is
        part of an aggregate operation w/o keys, which affects how predicates
        are inferred.

    Returns:
        The simplified expression and a set of predicates that apply to the
        resulting expression.
    """
    new_args: list[RelationalExpression]
    new_partitions: list[RelationalExpression]
    new_orders: list[ExpressionSortInfo]
    arg_predicates: list[set[LogicalPredicate]]
    output_predicates: set[LogicalPredicate] = set()
    requires_rewrite: bool = False

    if isinstance(expr, LiteralExpression):
        output_predicates = infer_literal_predicates(expr)

    if isinstance(expr, ColumnReference):
        output_predicates = input_predicates.get(expr, set())

    if isinstance(expr, CallExpression):
        new_args = []
        arg_predicates = []
        for arg in expr.inputs:
            new_arg, new_preds = run_simplification(
                arg, input_predicates, no_group_aggregate
            )
            requires_rewrite |= new_arg is not arg
            new_args.append(new_arg)
            arg_predicates.append(new_preds)
        if requires_rewrite:
            expr = CallExpression(expr.op, expr.data_type, new_args)
        expr, output_predicates = simplify_function_call(
            expr, arg_predicates, no_group_aggregate
        )

    if isinstance(expr, WindowCallExpression):
        new_args = []
        new_partitions = []
        new_orders = []
        arg_predicates = []
        for arg in expr.inputs:
            new_arg, new_preds = run_simplification(
                arg, input_predicates, no_group_aggregate
            )
            requires_rewrite |= new_arg is not arg
            new_args.append(new_arg)
            arg_predicates.append(new_preds)
        for partition in expr.partition_inputs:
            new_partition, _ = run_simplification(
                partition, input_predicates, no_group_aggregate
            )
            requires_rewrite |= new_partition is not partition
            new_partitions.append(new_partition)
        for order in expr.order_inputs:
            new_order, _ = run_simplification(
                order.expr, input_predicates, no_group_aggregate
            )
            requires_rewrite |= new_order is not order.expr
            new_orders.append(
                ExpressionSortInfo(new_order, order.ascending, order.nulls_first)
            )
        if requires_rewrite:
            expr = WindowCallExpression(
                expr.op,
                expr.data_type,
                new_args,
                new_partitions,
                new_orders,
                expr.kwargs,
            )
        expr, output_predicates = simplify_window_call(expr, arg_predicates)

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
                    expr, input_predicates, False
                )
            if isinstance(node, (Filter, Join)):
                node._condition = run_simplification(
                    node.condition, input_predicates, False
                )[0]
            if isinstance(node, (RelationalRoot, Limit)):
                node._orderings = [
                    ExpressionSortInfo(
                        run_simplification(order_expr.expr, input_predicates, False)[0],
                        order_expr.ascending,
                        order_expr.nulls_first,
                    )
                    for order_expr in node.orderings
                ]
            if isinstance(node, RelationalRoot):
                node._ordered_columns = [
                    (name, node.columns[name]) for name, _ in node.ordered_columns
                ]
            if isinstance(node, Join) and node.join_type != JoinType.INNER:
                for expr, preds in output_predicates.items():
                    if (
                        isinstance(expr, ColumnReference)
                        and expr.input_name != node.default_input_aliases[0]
                    ):
                        preds.discard(LogicalPredicate.NOT_NULL)
                        preds.discard(LogicalPredicate.POSITIVE)
        case Aggregate():
            for name, expr in node.keys.items():
                ref_expr = ColumnReference(name, expr.data_type)
                node.keys[name], output_predicates[ref_expr] = run_simplification(
                    expr, input_predicates, False
                )
                node.columns[name] = node.keys[name]
            for name, expr in node.aggregations.items():
                ref_expr = ColumnReference(name, expr.data_type)
                new_agg, output_predicates[ref_expr] = run_simplification(
                    expr, input_predicates, len(node.keys) == 0
                )
                assert isinstance(new_agg, CallExpression)
                node.aggregations[name] = new_agg
                node.columns[name] = node.aggregations[name]

        # For all other nodes, do not perform any simplification.
        case _:
            pass

    return output_predicates
