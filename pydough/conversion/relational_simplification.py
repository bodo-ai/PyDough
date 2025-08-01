"""
Logic used to simplify relational expressions in a relational node. A visitor
is used on the relational nodes to first simplify the child subtrees, then a
relational shuttle is run on the expressions of the current node to simplify
them, using the input predicates from the child nodes, and also infer the
predicates of the simplified expressions.
"""

__all__ = ["simplify_expressions"]


from dataclasses import dataclass

import pydough.pydough_operators as pydop
from pydough.relational import (
    Aggregate,
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    EmptySingleton,
    Filter,
    Join,
    JoinType,
    Limit,
    LiteralExpression,
    Project,
    RelationalExpression,
    RelationalExpressionShuttle,
    RelationalNode,
    RelationalRoot,
    RelationalVisitor,
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
        for pred in predicates:
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
    pydop.ABS,
    pydop.ADD,
    pydop.BAN,
    pydop.BOR,
    pydop.BXR,
    pydop.CEIL,
    pydop.CONTAINS,
    pydop.DATEDIFF,
    pydop.DAY,
    pydop.DAYNAME,
    pydop.DAYOFWEEK,
    pydop.ENDSWITH,
    pydop.EQU,
    pydop.FIND,
    pydop.FLOOR,
    pydop.GEQ,
    pydop.GRT,
    pydop.HOUR,
    pydop.JOIN_STRINGS,
    pydop.LARGEST,
    pydop.LENGTH,
    pydop.LEQ,
    pydop.LET,
    pydop.LIKE,
    pydop.LOWER,
    pydop.LPAD,
    pydop.MINUTE,
    pydop.MONOTONIC,
    pydop.MONTH,
    pydop.MUL,
    pydop.NEQ,
    pydop.NOT,
    pydop.REPLACE,
    pydop.ROUND,
    pydop.RPAD,
    pydop.SECOND,
    pydop.SIGN,
    pydop.SLICE,
    pydop.SMALLEST,
    pydop.STARTSWITH,
    pydop.STRIP,
    pydop.SUB,
    pydop.UPPER,
    pydop.YEAR,
}
"""
A set of operators that only output null if one of the inputs is null. This set
is significant because it means that if all of the inputs to a function are
guaranteed to be non-null, the output is guaranteed to be non-null as well.
"""


class SimplificationShuttle(RelationalExpressionShuttle):
    """
    Shuttle implementation for simplifying relational expressions. Has three
    sources of state used to determine how to simplify expressions:

    - `input_predicates`: A dictionary mapping column references to
      the corresponding predicate sets for all of the columns that are used as
      inputs to all of the expressions in the current relational node (e.g. from
      the inputs to the node). This needs to be set before the shuttle is
      used, and the default is an empty dictionary.
    - `no_group_aggregate`: A boolean indicating whether the current
      transformation is being done within the context of an aggregation without
      grouping keys. This is important because some aggregation functions will
      have different behaviors with/without grouping keys. For example, COUNT(*)
      is always positive if there are grouping keys, but if there are no
      grouping keys, the answer could be 0. This needs to be set before the
      shuttle is used, and the default is False.
    - `stack`: A stack of predicate sets corresponding to all inputs to the
      current expression. Used for simplifying function calls by first
      simplifying their inputs and placing their predicate sets on the stack.
    """

    def __init__(self):
        self.stack: list[PredicateSet] = []
        self._input_predicates: dict[RelationalExpression, PredicateSet] = {}
        self._no_group_aggregate: bool = False

    @property
    def input_predicates(self) -> dict[RelationalExpression, PredicateSet]:
        """
        Returns the input predicates that were passed to the shuttle.
        """
        return self._input_predicates

    @input_predicates.setter
    def input_predicates(self, value: dict[RelationalExpression, PredicateSet]) -> None:
        """
        Sets the input predicates for the shuttle.
        """
        self._input_predicates = value

    @property
    def no_group_aggregate(self) -> bool:
        """
        Returns whether the shuttle is currently handling a no-group-aggregate.
        """
        return self._no_group_aggregate

    @no_group_aggregate.setter
    def no_group_aggregate(self, value: bool) -> None:
        """
        Sets whether the shuttle is handling a no-group-aggregate.
        """
        self._no_group_aggregate = value

    def reset(self) -> None:
        self.stack = []

    def visit_literal_expression(
        self, literal_expression: LiteralExpression
    ) -> RelationalExpression:
        output_predicates: PredicateSet = PredicateSet()
        if literal_expression.value is not None:
            output_predicates.not_null = True
            if isinstance(literal_expression.value, (int, float, bool)):
                if literal_expression.value >= 0:
                    output_predicates.not_negative = True
                    if literal_expression.value > 0:
                        output_predicates.positive = True
        self.stack.append(output_predicates)
        return literal_expression

    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        self.stack.append(self.input_predicates.get(column_reference, PredicateSet()))
        return column_reference

    def visit_correlated_reference(
        self, correlated_reference: CorrelatedReference
    ) -> RelationalExpression:
        self.stack.append(PredicateSet())
        return correlated_reference

    def visit_call_expression(
        self, call_expression: CallExpression
    ) -> RelationalExpression:
        new_call = super().visit_call_expression(call_expression)
        assert isinstance(new_call, CallExpression)
        arg_predicates: list[PredicateSet] = [
            self.stack.pop() for _ in range(len(new_call.inputs))
        ]
        arg_predicates.reverse()
        return self.simplify_function_call(
            new_call, arg_predicates, self.no_group_aggregate
        )

    def visit_window_expression(
        self, window_expression: WindowCallExpression
    ) -> RelationalExpression:
        new_window = super().visit_window_expression(window_expression)
        assert isinstance(new_window, WindowCallExpression)
        for _ in range(len(new_window.order_inputs)):
            self.stack.pop()
        for _ in range(len(new_window.partition_inputs)):
            self.stack.pop()
        arg_predicates: list[PredicateSet] = [
            self.stack.pop() for _ in range(len(new_window.inputs))
        ]
        arg_predicates.reverse()
        return self.simplify_window_call(new_window, arg_predicates)

    def simplify_function_call(
        self,
        expr: CallExpression,
        arg_predicates: list[PredicateSet],
        no_group_aggregate: bool,
    ) -> RelationalExpression:
        """
        Procedure to simplify a function call expression based on the operator
        and the predicates of its arguments. This assumes that the arguments
        have already been simplified.

        Args:
            `expr`: The CallExpression to simplify, whose arguments have already
            been simplified.
            `arg_predicates`: A list of PredicateSet objects corresponding to
            the predicates of the arguments of the expression.
            `no_group_aggregate`: Whether the expression is part of a no-group
            aggregate.

        Returns:
            The simplified expression with the predicates updated based on the
            simplification rules. The predicates for the output are placed on
            the stack.
        """
        output_expr: RelationalExpression = expr
        output_predicates: PredicateSet = PredicateSet()
        union_set: PredicateSet = PredicateSet.union(arg_predicates)
        intersect_set: PredicateSet = PredicateSet.intersect(arg_predicates)

        # If the call has null propagating rules, all of the arguments are
        # non-null, the output is guaranteed to be non-null.
        if expr.op in NULL_PROPAGATING_OPS:
            if intersect_set.not_null:
                output_predicates.not_null = True

        match expr.op:
            case pydop.COUNT | pydop.NDISTINCT:
                # COUNT(n), COUNT(*), and NDISTINCT(n) are guaranteed to be
                # non-null and non-negative.
                output_predicates.not_null = True
                output_predicates.not_negative = True

                # The output of COUNT(*) is positive unless doing a
                # no-groupby aggregation. Same goes for calling COUNT or
                # NDISTINCT on a non-null column.
                if not no_group_aggregate:
                    if len(expr.inputs) == 0 or arg_predicates[0].not_null:
                        output_predicates.positive = True

                # COUNT(x) where x is non-null can be rewritten as COUNT(*),
                # which has the same positive rule as before.
                elif (
                    expr.op == pydop.COUNT
                    and len(expr.inputs) == 1
                    and arg_predicates[0].not_null
                ):
                    if not no_group_aggregate:
                        output_predicates.positive = True
                    output_expr = CallExpression(pydop.COUNT, expr.data_type, [])

            # All of these operators are non-null or non-negative if their
            # first argument is.
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
            # operands are. It is also positive if all the operands are
            # non-negative and at least one of them is positive.
            case pydop.ADD:
                output_predicates |= intersect_set & PredicateSet(
                    not_negative=True, positive=True
                )
                if intersect_set.not_negative and union_set.positive:
                    output_predicates.positive = True

            # The result of multiplication is non-negative or positive if all
            # the operands are.
            case pydop.MUL:
                output_predicates |= intersect_set & PredicateSet(
                    not_negative=True, positive=True
                )

            # The result of division is non-negative or positive if all the
            # operands are, and is also non-null if both operands are non-null
            # and the second operand is positive.
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
                # Modify the list of arguments by removing any that are None,
                # and stopping once we find the first argument that has is
                # non-null.
                new_args: list[RelationalExpression] = []
                new_predicates: list[PredicateSet] = []
                for i, arg in enumerate(expr.inputs):
                    if isinstance(arg, LiteralExpression) and arg.value is None:
                        continue
                    new_args.append(arg)
                    new_predicates.append(arg_predicates[i])
                    if arg_predicates[i].not_null:
                        break
                if len(new_args) == 0:
                    # If all inputs are None, the output is None.
                    output_expr = LiteralExpression(None, expr.data_type)
                elif len(new_args) == 1:
                    # If there is only one input, the output is that input.
                    output_expr = new_args[0]
                    output_predicates |= new_predicates[0]
                else:
                    # If there are multiple inputs, the output is a new
                    # DEFAULT_TO expression with the non-None inputs.
                    output_expr = CallExpression(
                        pydop.DEFAULT_TO, expr.data_type, new_args
                    )
                    output_predicates = PredicateSet.intersect(new_predicates)
                    if PredicateSet.union(new_predicates).not_null:
                        output_predicates.not_null = True

            # ABS(x) -> x if x is positive or non-negative. At the very least, we
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
            # operators are always non-negative. Most of cases do not set
            # predicates because there are no predicates to infer, beyond those
            # already accounted for with NULL_PROPAGATING_OPS.
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
                        expr.inputs[0].value.endswith(expr.inputs[1].value),
                        expr.data_type,
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

            # LIKE is always non-negative
            case pydop.LIKE:
                output_predicates.not_negative = True

            # X & Y is False if any of the arguments are False-y literals, and True
            # if all of the arguments are Truth-y literals.
            case pydop.BAN:
                if any(
                    isinstance(arg, LiteralExpression) and arg.value in [0, False, None]
                    for arg in expr.inputs
                ):
                    output_expr = LiteralExpression(False, expr.data_type)
                elif all(
                    isinstance(arg, LiteralExpression)
                    and arg.value not in [0, False, None]
                    for arg in expr.inputs
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                output_predicates.not_negative = True

            # X | Y is True if any of the arguments are Truth-y literals, and False
            # if all of the arguments are False-y literals.
            case pydop.BOR:
                if any(
                    isinstance(arg, LiteralExpression)
                    and arg.value not in [0, False, None]
                    for arg in expr.inputs
                ):
                    output_expr = LiteralExpression(True, expr.data_type)
                elif all(
                    isinstance(arg, LiteralExpression) and arg.value in [0, False, None]
                    for arg in expr.inputs
                ):
                    output_expr = LiteralExpression(False, expr.data_type)
                output_predicates.not_negative = True

            # NOT(x) is True if x is a False-y literal, and False if x is a
            # Truth-y literal.
            case pydop.NOT:
                if (
                    isinstance(expr.inputs[0], LiteralExpression)
                    and expr.inputs[0].value is not None
                ):
                    output_expr = LiteralExpression(
                        not bool(expr.inputs[0].value), expr.data_type
                    )
                    output_predicates.positive = not bool(expr.inputs[0].value)
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

                    # x >= y is True if x is non-negative and y is a literal
                    # that is zero or negative.
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

                    # The rest of the case of x CMP y can be constant folded if
                    # both x and y are literals.
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
                        # All other cases remain non-simplified.
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

            # IFF(True, y, z) -> y (same if the first argument is guaranteed to
            # be positive & non-null).
            # IFF(False, y, z) -> z
            # Otherwise, uses the intersection of the predicates of y and z.
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
            case _:
                # All other operators remain non-simplified.
                pass

        self.stack.append(output_predicates)
        return output_expr

    def simplify_window_call(
        self,
        expr: WindowCallExpression,
        arg_predicates: list[PredicateSet],
    ) -> RelationalExpression:
        """
        Procedure to simplify a window call expression based on the operator
        and the predicates of its arguments. This assumes that the arguments
        have already been simplified.

        Args:
            `expr`: The WindowCallExpression to simplify, whose arguments have
            already been simplified.
            `arg_predicates`: A list of PredicateSet objects corresponding to
            the predicates of the arguments of the expression.

        Returns:
            The simplified expression with the predicates updated based on
            the simplification rules. The predicates for the output are placed
            on the stack.
        """
        output_predicates: PredicateSet = PredicateSet()
        output_expr: RelationalExpression = expr
        no_frame: bool = not (
            expr.kwargs.get("cumulative", False) or "frame" in expr.kwargs
        )
        match expr.op:
            # RANKING & PERCENTILE are always non-null, non-negative, and
            # positive.
            case pydop.RANKING | pydop.PERCENTILE:
                output_predicates |= PredicateSet(
                    not_null=True, not_negative=True, positive=True
                )

            # RELSUM and RELAVG retain the properties of their argument, but
            # become nullable if there is a frame.
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

            # RELCOUNT is always non-negative, but it is only non-null if there
            # is no frame, and positive if there is no frame and the first
            # argument is non-null.
            case pydop.RELCOUNT:
                if no_frame:
                    output_predicates.not_null = True
                    if arg_predicates[0].not_null:
                        output_predicates.positive = True
                output_predicates.not_negative = True

            case _:
                # All other operators remain non-simplified.
                pass

        self.stack.append(output_predicates)
        return output_expr


class SimplificationVisitor(RelationalVisitor):
    """
    Relational visitor implementation that simplifies relational expressions
    within the relational tree and its subtrees in-place. The visitor first
    transforms all the subtrees and collects predicate set information for the
    output columns of each node, then uses those predicates to simplify the
    expressions of the current node. The predicates for the output predicates of
    the current node are placed on the stack.
    """

    def __init__(self, additional_shuttles: list[RelationalExpressionShuttle]):
        self.stack: list[dict[RelationalExpression, PredicateSet]] = []
        self.shuttle: SimplificationShuttle = SimplificationShuttle()
        self.additional_shuttles: list[RelationalExpressionShuttle] = (
            additional_shuttles
        )

    def reset(self):
        self.stack.clear()
        self.shuttle.reset()
        for shuttle in self.additional_shuttles:
            shuttle.reset()

    def get_input_predicates(
        self, node: RelationalNode
    ) -> dict[RelationalExpression, PredicateSet]:
        """
        Recursively simplifies the inputs to the current node and collects
        the predicates for each column from all of the inputs to the current
        node.

        Args:
            `node`: The current relational node whose inputs are being
            simplified.

        Returns:
            A dictionary mapping each input column reference from a column from
            an input to the current node to the set of its inferred predicates.
        """
        self.visit_inputs(node)
        # For each input, pop the predicates from the stack and add them
        # to the input predicates dictionary, using the appropriate input alias.
        input_predicates: dict[RelationalExpression, PredicateSet] = {}
        for i in reversed(range(len(node.inputs))):
            input_alias: str | None = node.default_input_aliases[i]
            predicates: dict[RelationalExpression, PredicateSet] = self.stack.pop()
            for expr, preds in predicates.items():
                input_predicates[add_input_name(expr, input_alias)] = preds

        return input_predicates

    def generic_visit(
        self, node: RelationalNode
    ) -> dict[RelationalExpression, PredicateSet]:
        """
        The generic pattern for relational simplification used by most of the
        relational nodes as a base. It simplifies all descendants of the current
        node, and uses the predicates from the inputs to transform all of the
        expressions of the current node in-place. The predicates for the output
        columns of the current node are returned as a dictionary mapping each
        output column reference to its set of predicates.

        Args:
            `node`: The current relational node to simplify.

        Returns:
            A dictionary mapping each output column reference from the current
            node to the set of its inferred predicates.
        """
        # Simplify the inputs to the current node and collect the predicates
        # for each column from the inputs.
        input_predicates: dict[RelationalExpression, PredicateSet] = (
            self.get_input_predicates(node)
        )
        # Set the input predicates and no-group-aggregate state for the shuttle.
        self.shuttle.input_predicates = input_predicates
        self.shuttle.no_group_aggregate = (
            isinstance(node, Aggregate) and len(node.keys) == 0
        )
        # Transform the expressions of the current node in-place.
        ref_expr: RelationalExpression
        output_predicates: dict[RelationalExpression, PredicateSet] = {}
        for name, expr in node.columns.items():
            ref_expr = ColumnReference(name, expr.data_type)
            expr = expr.accept_shuttle(self.shuttle)
            output_predicates[ref_expr] = self.shuttle.stack.pop()
            for shuttle in self.additional_shuttles:
                expr = expr.accept_shuttle(shuttle)
            node.columns[name] = expr
        return output_predicates

    def visit_scan(self, node: Scan) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        self.stack.append(output_predicates)

    def visit_empty_singleton(self, node: EmptySingleton) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        self.stack.append(output_predicates)

    def visit_project(self, node: Project) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        self.stack.append(output_predicates)

    def visit_filter(self, node: Filter) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        # Transform the filter condition in-place.
        node._condition = node.condition.accept_shuttle(self.shuttle)
        self.shuttle.stack.pop()
        for shuttle in self.additional_shuttles:
            node._condition = node.condition.accept_shuttle(shuttle)
        self.stack.append(output_predicates)

    def visit_join(self, node: Join) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        # Transform the join condition in-place.
        node._condition = node.condition.accept_shuttle(self.shuttle)
        self.shuttle.stack.pop()
        for shuttle in self.additional_shuttles:
            node._condition = node.condition.accept_shuttle(shuttle)
        # If the join is not an inner join, remove any not-null predicates
        # from the RHS of the join.
        if node.join_type != JoinType.INNER:
            for expr, preds in output_predicates.items():
                if (
                    isinstance(expr, ColumnReference)
                    and expr.input_name != node.default_input_aliases[0]
                ):
                    preds.not_null = False
        self.stack.append(output_predicates)

    def visit_limit(self, node: Limit) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        # Transform the order keys in-place.
        for ordering_expr in node.orderings:
            ordering_expr.expr = ordering_expr.expr.accept_shuttle(self.shuttle)
            self.shuttle.stack.pop()
            for shuttle in self.additional_shuttles:
                ordering_expr.expr = ordering_expr.expr.accept_shuttle(shuttle)
        self.stack.append(output_predicates)

    def visit_root(self, node: RelationalRoot) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        node._ordered_columns = [
            (name, node.columns[name]) for name, _ in node.ordered_columns
        ]
        # Transform the order keys in-place.
        for ordering_expr in node.orderings:
            ordering_expr.expr = ordering_expr.expr.accept_shuttle(self.shuttle)
            self.shuttle.stack.pop()
            for shuttle in self.additional_shuttles:
                ordering_expr.expr = ordering_expr.expr.accept_shuttle(shuttle)
        self.stack.append(output_predicates)

    def visit_aggregate(self, node: Aggregate) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        # Transform the keys & aggregations to match the columns.
        for name in node.keys:
            node.keys[name] = node.columns[name]
        for name in node.aggregations:
            expr = node.columns[name]
            assert isinstance(expr, CallExpression)
            node.aggregations[name] = expr
        self.stack.append(output_predicates)


def simplify_expressions(
    node: RelationalNode,
    additional_shuttles: list[RelationalExpressionShuttle],
) -> None:
    """
    Transforms the current node and all of its descendants in-place to simplify
    any relational expressions.

    Args:
        `node`: The relational node to perform simplification on.
        `additional_shuttles`: A list of additional shuttles to apply to the
        expressions of the node and its descendants. These shuttles are applied
        after the simplification shuttle, and can be used to perform additional
        transformations on the expressions.
    """
    simplifier: SimplificationVisitor = SimplificationVisitor(additional_shuttles)
    node.accept(simplifier)
