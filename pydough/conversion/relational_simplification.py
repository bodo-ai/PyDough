"""
Logic used to simplify relational expressions in a relational node.
"""

__all__ = ["simplify_expressions"]


from dataclasses import dataclass

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


@dataclass
class PredicateSet:
    """
    A set of logical predicates that can be inferred about relational
    expressions and used to simplify other expressions.
    """

    not_null: bool = False
    """
    Whether the expression is guaranteed to not be null.
    """

    not_negative: bool = False
    """
    Whether the expression is guaranteed to not be negative.
    """

    positive: bool = False
    """
    Whether the expression is guaranteed to be positive.
    """

    def __or__(self, other: "PredicateSet") -> "PredicateSet":
        """
        Combines two predicate sets using a logical OR operation.
        """
        return PredicateSet(
            not_null=self.not_null or other.not_null,
            not_negative=self.not_negative or other.not_negative,
            positive=self.positive or other.positive,
        )

    def __and__(self, other: "PredicateSet") -> "PredicateSet":
        """
        Combines two predicate sets using a logical AND operation.
        """
        return PredicateSet(
            not_null=self.not_null and other.not_null,
            not_negative=self.not_negative and other.not_negative,
            positive=self.positive and other.positive,
        )

    def __sub__(self, other: "PredicateSet") -> "PredicateSet":
        """
        Subtracts one predicate set from another.
        """
        return PredicateSet(
            not_null=self.not_null and not other.not_null,
            not_negative=self.not_negative and not other.not_negative,
            positive=self.positive and not other.positive,
        )

    @staticmethod
    def union(predicates: list["PredicateSet"]) -> "PredicateSet":
        """
        Computes the union of a list of predicate sets.
        """
        result: PredicateSet = PredicateSet()
        for pred in predicates[1:]:
            result = result | pred
        return result

    @staticmethod
    def intersect(predicates: list["PredicateSet"]) -> "PredicateSet":
        """
        Computes the intersection of a list of predicate sets.
        """
        result: PredicateSet = PredicateSet()
        if len(predicates) == 0:
            return result
        else:
            result |= predicates[0]
        for pred in predicates[1:]:
            result = result & pred
        return result


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
    arg_predicates: list[PredicateSet],
    no_group_aggregate: bool,
) -> tuple[RelationalExpression, PredicateSet]:
    """
    TODO
    """
    output_expr: RelationalExpression = expr
    output_predicates: PredicateSet = PredicateSet()
    union_set: PredicateSet = PredicateSet.union(arg_predicates)
    intersect_set: PredicateSet = PredicateSet.intersect(arg_predicates)

    # If the call has null propagating rules, all of hte arguments are non-null,
    # the output is guaranteed to be non-null.
    if expr.op in NULL_PROPAGATING_OPS:
        if intersect_set.not_null:
            output_predicates.not_null = True

    match expr.op:
        case pydop.COUNT | pydop.NDISTINCT:
            # COUNT(n), COUNT(*), and NDISTINCT(n) are guaranteed to be non-null
            # and non-negative.
            output_predicates.not_null = True
            output_predicates.not_negative = True

            # The output if COUNT(*) is positive if unless doing a no-groupby
            # aggregation. Same goes for calling COUNT or NDISTINCT ona non-null
            # column.
            if not no_group_aggregate:
                if len(expr.inputs) == 0 or arg_predicates[0].not_null:
                    output_predicates.positive = True

            # COUNT(x) where x is non-null can be rewritten as COUNT(*), which
            # has the same positive rule as before.
            elif (
                expr.op == pydop.COUNT
                and len(expr.inputs) == 1
                and arg_predicates[0].not_null
            ):
                if not no_group_aggregate:
                    output_predicates.positive = True
                output_expr = CallExpression(pydop.COUNT, expr.data_type, [])

        # All of these operators are non-null aor non-negative if their first
        # argument is.
        case (
            pydop.SUM
            | pydop.AVG
            | pydop.MIN
            | pydop.MAX
            | pydop.ANYTHING
            | pydop.MEDIAN
            | pydop.QUANTILE
        ):
            output_predicates |= arg_predicates[0] & PredicateSet(
                not_null=True, not_negative=True
            )

        # The result of addition is non-negative or positive if all the
        # operands are. It is also positive if all the operands are non-negative
        # and at least one of them is positive.
        case pydop.ADD:
            output_predicates |= intersect_set & PredicateSet(
                not_negative=True, positive=True
            )
            if intersect_set.not_negative and union_set.positive:
                output_predicates.positive = True

        # The result of multiplication is non-negative or positive if all the
        # operands are.
        case pydop.MUL:
            output_predicates |= intersect_set & PredicateSet(
                not_negative=True, positive=True
            )

        # The result of division is non-negative or positive if all the
        # operands are, and is also non-null if both operands are non-null and
        # the second operand is positive.
        case pydop.DIV:
            output_predicates |= intersect_set & PredicateSet(
                not_negative=True, positive=True
            )
            if (
                arg_predicates[0].not_null
                and arg_predicates[1].not_null
                and arg_predicates[1].positive
            ):
                output_predicates.not_null = True

        case pydop.DEFAULT_TO:
            # DEFAULT_TO(None, x) -> x
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and expr.inputs[0].value is None
            ):
                if len(expr.inputs) == 2:
                    output_expr = expr.inputs[1]
                    output_predicates = arg_predicates[1]
                else:
                    output_expr = CallExpression(
                        pydop.DEFAULT_TO, expr.data_type, expr.inputs[1:]
                    )
                    output_predicates |= PredicateSet.intersect(arg_predicates[1:])

            # DEFAULT_TO(x, y) -> x if x is non-null.
            elif arg_predicates[0].not_null:
                output_expr = expr.inputs[0]
                output_predicates |= arg_predicates[0]

            # Otherwise, it is non-null if any of the arguments are non-null,
            # and gains any predicates that all the arguments have in common.
            else:
                if union_set.not_null:
                    output_predicates.not_null = True
                output_predicates |= intersect_set

        # ABS(x) -> x if x is positive or non-negative. At hte very least, we
        # know it is always non-negative.
        case pydop.ABS:
            if arg_predicates[0].not_negative or arg_predicates[0].positive:
                output_expr = expr.inputs[0]
                output_predicates |= arg_predicates[0]
            else:
                output_predicates.not_negative = True

        # LENGTH(x) can be constant folded if x is a string literal. Otherwise,
        # we know it is non-negative.
        case pydop.LENGTH:
            if isinstance(expr.inputs[0], LiteralExpression) and isinstance(
                expr.inputs[0].value, str
            ):
                str_len: int = len(expr.inputs[0].value)
                output_expr = LiteralExpression(str_len, expr.data_type)
                if str_len > 0:
                    output_predicates.positive = True
            output_predicates.not_negative = True

        # LOWER, UPPER, STARTSWITH, ENDSWITH, and CONTAINS can be constant
        # folded if the inputs are string literals. The boolean-returning
        # operators are always non-negative.
        case pydop.LOWER:
            if isinstance(expr.inputs[0], LiteralExpression) and isinstance(
                expr.inputs[0].value, str
            ):
                output_expr = LiteralExpression(
                    expr.inputs[0].value.lower(), expr.data_type
                )
        case pydop.UPPER:
            if isinstance(expr.inputs[0], LiteralExpression) and isinstance(
                expr.inputs[0].value, str
            ):
                output_expr = LiteralExpression(
                    expr.inputs[0].value.upper(), expr.data_type
                )
        case pydop.STARTSWITH:
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and isinstance(expr.inputs[0].value, str)
                and isinstance(expr.inputs[1], LiteralExpression)
                and isinstance(expr.inputs[1].value, str)
            ):
                output_expr = LiteralExpression(
                    expr.inputs[0].value.startswith(expr.inputs[1].value),
                    expr.data_type,
                )
                output_predicates.positive |= expr.inputs[0].value.startswith(
                    expr.inputs[1].value
                )
            output_predicates.not_negative = True
        case pydop.ENDSWITH:
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and isinstance(expr.inputs[0].value, str)
                and isinstance(expr.inputs[1], LiteralExpression)
                and isinstance(expr.inputs[1].value, str)
            ):
                output_expr = LiteralExpression(
                    expr.inputs[0].value.endswith(expr.inputs[1].value), expr.data_type
                )
                output_predicates.positive |= expr.inputs[0].value.endswith(
                    expr.inputs[1].value
                )
            output_predicates.not_negative = True
        case pydop.CONTAINS:
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and isinstance(expr.inputs[0].value, str)
                and isinstance(expr.inputs[1], LiteralExpression)
                and isinstance(expr.inputs[1].value, str)
            ):
                output_expr = LiteralExpression(
                    expr.inputs[1].value in expr.inputs[0].value, expr.data_type
                )
                output_predicates.positive |= (
                    expr.inputs[1].value in expr.inputs[0].value
                )
            output_predicates.not_negative = True

        # SQRT(x) can be constant folded if x is a literal and non-negative.
        # Otherwise, it is non-negative, and positive if x is positive.
        case pydop.SQRT:
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and isinstance(expr.inputs[0].value, (int, float))
                and expr.inputs[0].value >= 0
            ):
                sqrt_value: float = expr.inputs[0].value ** 0.5
                output_expr = LiteralExpression(sqrt_value, expr.data_type)
            if arg_predicates[0].positive:
                output_predicates.positive = True
            output_predicates.not_negative = True

        case pydop.MONOTONIC:
            v0: int | float | None = None
            v1: int | float | None = None
            v2: int | float | None = None
            monotonic_result: bool
            if isinstance(expr.inputs[0], LiteralExpression) and isinstance(
                expr.inputs[0].value, (int, float)
            ):
                v0 = expr.inputs[0].value
            if isinstance(expr.inputs[1], LiteralExpression) and isinstance(
                expr.inputs[1].value, (int, float)
            ):
                v1 = expr.inputs[1].value
            if isinstance(expr.inputs[2], LiteralExpression) and isinstance(
                expr.inputs[2].value, (int, float)
            ):
                v2 = expr.inputs[2].value

            # MONOTONIC(x, y, z), where x/y/z are all literals
            # -> True if x <= y <= z, False otherwise
            if v0 is not None and v1 is not None and v2 is not None:
                monotonic_result = (v0 <= v1) and (v1 <= v2)
                output_expr = LiteralExpression(monotonic_result, expr.data_type)
                if monotonic_result:
                    output_predicates.positive = True

            # MONOTONIC(x, y, z), where x/y are literals
            # -> if x <= y, then y <= z, otherwise False
            elif v0 is not None and v1 is not None:
                if v0 <= v1:
                    output_expr = CallExpression(
                        pydop.LEQ, expr.data_type, expr.inputs[1:]
                    )
                else:
                    output_expr = LiteralExpression(False, expr.data_type)

            # MONOTONIC(x, y, z), where y/z are literals
            # -> if y <= z, then x <= y, otherwise False
            elif v1 is not None and v2 is not None:
                if v1 <= v2:
                    output_expr = CallExpression(
                        pydop.LEQ, expr.data_type, expr.inputs[:2]
                    )
                else:
                    output_expr = LiteralExpression(False, expr.data_type)
            output_predicates.not_negative = True

        # XOR and LIKE are always non-negative
        case pydop.BXR | pydop.LIKE:
            output_predicates.not_negative = True

        # X & Y is False if any of the arguments are False-y literals, and True
        # if all of the arguments are Truth-y literals.
        case pydop.BAN:
            if any(
                isinstance(arg, LiteralExpression) and arg.value in [0, False, None]
                for arg in expr.inputs
            ):
                output_expr = LiteralExpression(False, expr.data_type)
            if all(
                isinstance(arg, LiteralExpression) and arg.value not in [0, False, None]
                for arg in expr.inputs
            ):
                output_expr = LiteralExpression(True, expr.data_type)
            output_predicates.not_negative = True

        # X | Y is True if any of the arguments are Truth-y literals, and False
        # if all of the arguments are False-y literals.
        case pydop.BOR:
            if any(
                isinstance(arg, LiteralExpression) and arg.value not in [0, False, None]
                for arg in expr.inputs
            ):
                output_expr = LiteralExpression(True, expr.data_type)
            if all(
                isinstance(arg, LiteralExpression) and arg.value in [0, False, None]
                for arg in expr.inputs
            ):
                output_expr = LiteralExpression(False, expr.data_type)
            output_predicates.not_negative = True

        case pydop.EQU | pydop.NEQ | pydop.GEQ | pydop.GRT | pydop.LET | pydop.LEQ:
            match (expr.inputs[0], expr.op, expr.inputs[1]):
                # x > y is True if x is positive and y is a literal that is
                # zero or negative. The same goes for x >= y.
                case (_, pydop.GRT, LiteralExpression()) | (
                    _,
                    pydop.GEQ,
                    LiteralExpression(),
                ) if (
                    isinstance(expr.inputs[1].value, (int, float, bool))
                    and expr.inputs[1].value <= 0
                    and arg_predicates[0].not_null
                    and arg_predicates[0].positive
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                    output_predicates |= PredicateSet(
                        not_null=True, not_negative=True, positive=True
                    )

                # x >= y is True if x is non-negative and y is a literal that is
                # zero or negative.
                case (_, pydop.GEQ, LiteralExpression()) if (
                    isinstance(expr.inputs[1].value, (int, float, bool))
                    and expr.inputs[1].value <= 0
                    and arg_predicates[0].not_null
                    and arg_predicates[0].not_negative
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                    output_predicates |= PredicateSet(
                        not_null=True, not_negative=True, positive=True
                    )

                # The rest of the case of x CMP y can be constant folded if both
                # x and y are literals.
                case (LiteralExpression(), _, LiteralExpression()):
                    match (
                        expr.inputs[0].value,
                        expr.inputs[1].value,
                        expr.op,
                    ):
                        case (None, _, _) | (_, None, _):
                            output_expr = LiteralExpression(None, expr.data_type)
                        case (x, y, pydop.EQU):
                            output_expr = LiteralExpression(x == y, expr.data_type)
                        case (x, y, pydop.NEQ):
                            output_expr = LiteralExpression(x != y, expr.data_type)
                        case (x, y, pydop.LET) if isinstance(
                            x, (int, float, str, bool)
                        ) and isinstance(y, (int, float, str, bool)):
                            output_expr = LiteralExpression(x < y, expr.data_type)  # type: ignore
                        case (x, y, pydop.LEQ) if isinstance(
                            x, (int, float, str, bool)
                        ) and isinstance(y, (int, float, str, bool)):
                            output_expr = LiteralExpression(x <= y, expr.data_type)  # type: ignore
                        case (x, y, pydop.GRT) if isinstance(
                            x, (int, float, str, bool)
                        ) and isinstance(y, (int, float, str, bool)):
                            output_expr = LiteralExpression(x > y, expr.data_type)  # type: ignore
                        case (x, y, pydop.GEQ) if isinstance(
                            x, (int, float, str, bool)
                        ) and isinstance(y, (int, float, str, bool)):
                            output_expr = LiteralExpression(x >= y, expr.data_type)  # type: ignore

                case _:
                    pass

            output_predicates.not_negative = True

        # PRESENT(x) is True if x is non-null.
        case pydop.PRESENT:
            if arg_predicates[0].not_null:
                output_expr = LiteralExpression(True, expr.data_type)
                output_predicates.positive = True
            output_predicates.not_null = True
            output_predicates.not_negative = True

        # ABSENT(x) is True if x is null.
        case pydop.ABSENT:
            if (
                isinstance(expr.inputs[0], LiteralExpression)
                and expr.inputs[0].value is None
            ):
                output_expr = LiteralExpression(True, expr.data_type)
                output_predicates.positive = True
            output_predicates.not_null = True
            output_predicates.not_negative = True

        # IFF(True, y, z) -> y (same if the first argument is guaranteed to be
        # positive & non-null).
        # IFF(False, y, z) -> z
        # Otherwise, it inherits the intersection of the predicates of y and z.
        case pydop.IFF:
            if isinstance(expr.inputs[0], LiteralExpression):
                if bool(expr.inputs[0].value):
                    output_expr = expr.inputs[1]
                    output_predicates |= arg_predicates[1]
                else:
                    output_expr = expr.inputs[2]
                    output_predicates |= arg_predicates[2]
            elif arg_predicates[0].not_null and arg_predicates[0].positive:
                output_expr = expr.inputs[1]
                output_predicates |= arg_predicates[1]
            else:
                output_predicates |= arg_predicates[1] & arg_predicates[2]

        # KEEP_IF(x, True) -> x
        # KEEP_IF(x, False) -> None
        case pydop.KEEP_IF:
            if isinstance(expr.inputs[1], LiteralExpression):
                if bool(expr.inputs[1].value):
                    output_expr = expr.inputs[0]
                    output_predicates |= arg_predicates[0]
                else:
                    output_expr = LiteralExpression(None, expr.data_type)
                    output_predicates.not_negative = True
            elif arg_predicates[1].not_null and arg_predicates[1].positive:
                output_expr = expr.inputs[0]
                output_predicates = arg_predicates[0]
            else:
                output_predicates |= arg_predicates[0] & PredicateSet(
                    not_null=True, not_negative=True
                )
    return output_expr, output_predicates


def simplify_window_call(
    expr: WindowCallExpression,
    arg_predicates: list[PredicateSet],
) -> tuple[RelationalExpression, PredicateSet]:
    """
    TODO
    """
    output_predicates: PredicateSet = PredicateSet()
    no_frame: bool = not (
        expr.kwargs.get("cumulative", False) or "frame" in expr.kwargs
    )
    match expr.op:
        # RANKING & PERCENTILE are always non-null, non-negative, and positive.
        case pydop.RANKING | pydop.PERCENTILE:
            output_predicates |= PredicateSet(
                not_null=True, not_negative=True, positive=True
            )

        # RELSUM and RELAVG retain the properties of their argument, but become
        # nullable if there is a frame.
        case pydop.RELSUM | pydop.RELAVG:
            if arg_predicates[0].not_null and no_frame:
                output_predicates.not_null = True
            if arg_predicates[0].not_negative:
                output_predicates.not_negative = True
            if arg_predicates[0].positive:
                output_predicates.positive = True

        # RELSIZE is always non-negative, but is only non-null & positive if
        # there is no frame.
        case pydop.RELSIZE:
            if no_frame:
                output_predicates.not_null = True
                output_predicates.positive = True
            output_predicates.not_negative = True

        # RELCOUNT is always non-negative, but it is only non-null if there is
        # no frame, and positive if there is no frame and the first argument
        # is non-null.
        case pydop.RELCOUNT:
            if no_frame:
                output_predicates.not_null = True
                if arg_predicates[0].not_null:
                    output_predicates.positive = True
            output_predicates.not_negative = True
    return expr, output_predicates


def infer_literal_predicates(expr: LiteralExpression) -> PredicateSet:
    """
    Infers logical predicates from a literal expression.

    Args:
        `expr`: The literal expression to infer predicates from.

    Returns:
        A set of logical predicates inferred from the literal.
    """
    output_predicates: PredicateSet = PredicateSet()
    if expr.value is not None:
        output_predicates.not_null = True
        if isinstance(expr.value, (int, float)):
            if expr.value >= 0:
                output_predicates.not_negative = True
                if expr.value > 0:
                    output_predicates.positive = True
    return output_predicates


def run_simplification(
    expr: RelationalExpression,
    input_predicates: dict[RelationalExpression, PredicateSet],
    no_group_aggregate: bool,
) -> tuple[RelationalExpression, PredicateSet]:
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
    arg_predicates: list[PredicateSet]
    output_predicates: PredicateSet = PredicateSet()
    requires_rewrite: bool = False

    if isinstance(expr, LiteralExpression):
        output_predicates = infer_literal_predicates(expr)

    if isinstance(expr, ColumnReference):
        output_predicates = input_predicates.get(expr, PredicateSet())

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
) -> dict[RelationalExpression, PredicateSet]:
    """
    The main recursive procedure done to perform expression simplification on
    a relational node and its descendants. The transformation is done in-place

    Args:
        `node`: The relational node to perform simplification on.

    Returns:
        The predicates inferred from the output columns of the node.
    """
    # Recursively invoke the procedure on all inputs to the node.
    input_predicates: dict[RelationalExpression, PredicateSet] = {}
    for idx, input_node in enumerate(node.inputs):
        input_alias: str | None = node.default_input_aliases[idx]
        predicates = simplify_expressions(input_node)
        for expr, preds in predicates.items():
            input_predicates[add_input_name(expr, input_alias)] = preds

    # Transform the expressions of the current node in-place.
    ref_expr: RelationalExpression
    output_predicates: dict[RelationalExpression, PredicateSet] = {}
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
                        preds.not_null = False
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
