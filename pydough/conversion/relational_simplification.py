"""
Logic used to simplify relational expressions in a relational node. A visitor
is used on the relational nodes to first simplify the child subtrees, then a
relational shuttle is run on the expressions of the current node to simplify
them, using the input predicates from the child nodes, and also infer the
predicates of the simplified expressions.
"""

__all__ = ["simplify_expressions"]


import datetime
import re
from dataclasses import dataclass

import pandas as pd

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
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
from pydough.sqlglot.transform_bindings.sqlglot_transform_utils import (
    DateTimeUnit,
    offset_pattern,
    trunc_pattern,
)
from pydough.types import ArrayType, BooleanType, NumericType, StringType


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


NULLABLE_IF_INPUT_NULLABLE_OPS: set[pydop.PyDoughOperator] = {
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
A set of operators that can only output null if one of the inputs is null. This
set is significant because it means that if all of the inputs to a function are
guaranteed to be non-null, the output is guaranteed to be non-null as well.
"""


NULL_IF_INPUT_NULL_OPS: set[pydop.PyDoughOperator] = (
    NULLABLE_IF_INPUT_NULLABLE_OPS | {pydop.GETPART, pydop.DATETIME}
) - {pydop.BOR, pydop.SLICE}
"""
A set of operators that will always output null if any of their inputs are null.
This includes all operators from `NULLABLE_IF_INPUT_NULLABLE_OPS` unless it is
possible for them to output a non-null value even if some inputs are null (e.g.
OR, SLICE), and also include some operators that can return NULL even if none of
the inputs are null (e.g. GETPART or DATEDIFF).
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

    def __init__(self, configs: PyDoughConfigs):
        self.stack: list[PredicateSet] = []
        self._input_predicates: dict[RelationalExpression, PredicateSet] = {}
        self._no_group_aggregate: bool = False
        self._configs: PyDoughConfigs = configs

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

    @property
    def configs(self) -> PyDoughConfigs:
        """
        Returns the PyDough configuration settings.
        """
        return self._configs

    @configs.setter
    def configs(self, value: PyDoughConfigs) -> None:
        """
        Sets the PyDough configuration settings.
        """
        self._configs = value

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

    def quarter_month_array(self, quarter: int) -> RelationalExpression:
        """
        Returns the months corresponding to the given quarter as a
        LiteralExpression of an integer array.

        Args:
            `quarter`: The quarter (1-4) to get the corresponding months for.

        Returns:
            A LiteralExpression containing an array of the months in the
            given quarter.
        """
        assert 1 <= quarter <= 4
        start: int = 3 * (quarter - 1) + 1
        month_arr: list[int] = list(range(start, start + 3))
        return LiteralExpression(month_arr, ArrayType(NumericType()))

    def switch_operator(
        self, expr: CallExpression, op: pydop.PyDoughExpressionOperator
    ) -> RelationalExpression:
        """
        Returns a new CallExpression switching the operator of the given
        CallExpression to the given operator, keeping the same inputs and data
        type.

        Args:
            `expr`: The CallExpression whose operator is to be switched.
            `op`: The operator to switch to.

        Returns:
            A new CallExpression with the given operator.
        """
        return CallExpression(op, expr.data_type, expr.inputs)

    def keep_if_not_null(
        self, source: RelationalExpression, expr: RelationalExpression
    ) -> RelationalExpression:
        """
        Returns a CallExpression that keeps the given expression only if the
        source expression is not null.

        Args:
            `source`: The source expression to check for nullness.
            `expr`: The expression to keep if the source is not null.

        Returns:
            A CallExpression representing KEEP_IF(expr, PRESENT(source)).
        """
        source_not_null: RelationalExpression = CallExpression(
            pydop.PRESENT, BooleanType(), [source]
        )
        return CallExpression(pydop.KEEP_IF, expr.data_type, [expr, source_not_null])

    def simplify_function_literal_comparison(
        self,
        expr: RelationalExpression,
        op: pydop.PyDoughOperator,
        func_expr: CallExpression,
        lit_expr: LiteralExpression,
    ) -> RelationalExpression:
        """
        Simplifies a comparison between a function call expression and a
        literal expression, e.g. `QUARTER(x) == 2` can be simplified to
        `ISIN(MONTH(x), [4, 5, 6])`.

        Args:
            `expr`: The original expression representing the comparison. This
            should be returned if there is no simplification possible.
            `op`: The comparison operator (e.g. EQU, NEQ, LET, etc).
            `func_expr`: The left argument of the comparison, which is a
            function call expression.
            `lit_expr`: The right argument of the comparison, which is a
            literal expression.
        """
        assert op in (pydop.EQU, pydop.NEQ, pydop.GRT, pydop.GEQ, pydop.LET, pydop.LEQ)
        result: RelationalExpression = expr
        conditional_true: RelationalExpression = self.keep_if_not_null(
            func_expr.inputs[0], LiteralExpression(True, expr.data_type)
        )
        conditional_false: RelationalExpression = self.keep_if_not_null(
            func_expr.inputs[0], LiteralExpression(False, expr.data_type)
        )
        dt_unit_boundaries: dict[pydop.PyDoughExpressionOperator, tuple[int, int]] = {
            pydop.QUARTER: (1, 4),
            pydop.MONTH: (1, 12),
            pydop.DAY: (1, 31),
            pydop.MONTH: (1, 12),
            pydop.HOUR: (0, 23),
            pydop.MINUTE: (0, 59),
            pydop.SECOND: (0, 59),
        }

        match (op, func_expr.op, lit_expr.data_type):
            # e.g. QUARTER(x) == 0 <=> KEEP_IF(False, PRESENT(x))
            # or QUARTER(x) <= -1 <=> KEEP_IF(False, PRESENT(x))
            # same for other units below their lower bound
            case (pydop.EQU | pydop.LEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value < dt_unit_boundaries[func_expr.op][0]
            ):
                result = conditional_false

            # e.g. QUARTER(x) < 1 <=> KEEP_IF(False, PRESENT(x))
            # same for other units at or below their lower bound
            case (pydop.LET, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value <= dt_unit_boundaries[func_expr.op][0]
            ):
                result = conditional_false

            # e.g. QUARTER(x) == 5 <=> KEEP_IF(False, PRESENT(x))
            # or QUARTER(x) >= 5 <=> KEEP_IF(False, PRESENT(x))
            # same for other units above their lower bound
            case (pydop.EQU | pydop.GEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value > dt_unit_boundaries[func_expr.op][1]
            ):
                result = conditional_false

            # e.g. QUARTER(x) > 4 <=> KEEP_IF(False, PRESENT(x))
            # same for other units at or above their lower bound
            case (pydop.GRT, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value >= dt_unit_boundaries[func_expr.op][1]
            ):
                result = conditional_false

            # e.g. QUARTER(x) != 5 <=> KEEP_IF(True, PRESENT(x))
            # or QUARTER(x) < 5 <=> KEEP_IF(True, PRESENT(x))
            # Same for other units above their upper bound
            case (pydop.LET | pydop.NEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value > dt_unit_boundaries[func_expr.op][1]
            ):
                result = conditional_true

            # e.g. QUARTER(x) <= 4 <=> KEEP_IF(True, PRESENT(x))
            # Same for other units at or above their upper bound
            case (pydop.LEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value >= dt_unit_boundaries[func_expr.op][1]
            ):
                result = conditional_true

            # e.g. QUARTER(x) != 0 <=> KEEP_IF(True, PRESENT(x))
            # or QUARTER(x) > 0 <=> KEEP_IF(True, PRESENT(x))
            # Same for other units below their lower bound
            case (pydop.GRT | pydop.NEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value < dt_unit_boundaries[func_expr.op][0]
            ):
                result = conditional_true

            # e.g. QUARTER(x) >= 1 <=> KEEP_IF(True, PRESENT(x))
            # Same for other units at or below their lower bound
            case (pydop.GEQ, _, NumericType()) if (
                isinstance(lit_expr.value, int)
                and func_expr.op in dt_unit_boundaries
                and lit_expr.value <= dt_unit_boundaries[func_expr.op][0]
            ):
                result = conditional_true

            # e.g. QUARTER(x) == 1 <=> ISIN(MONTH(x), [1, 2, 3])
            case (pydop.EQU, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (1, 2, 3, 4):
                result = CallExpression(
                    pydop.ISIN,
                    expr.data_type,
                    [
                        self.switch_operator(func_expr, pydop.MONTH),
                        self.quarter_month_array(lit_expr.value),
                    ],
                )

            # e.g. QUARTER(x) != 4 <=> NOT(ISIN(MONTH(x), [10, 11, 12]))
            case (pydop.NEQ, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (1, 2, 3, 4):
                result = CallExpression(
                    pydop.NOT,
                    expr.data_type,
                    [
                        CallExpression(
                            pydop.ISIN,
                            expr.data_type,
                            [
                                self.switch_operator(func_expr, pydop.MONTH),
                                self.quarter_month_array(lit_expr.value),
                            ],
                        )
                    ],
                )

            # e.g. QUARTER(x) < 4 <=> MONTH(X) < 10
            case (pydop.LET, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (2, 3, 4):
                result = CallExpression(
                    pydop.LET,
                    expr.data_type,
                    [
                        self.switch_operator(func_expr, pydop.MONTH),
                        LiteralExpression((lit_expr.value * 3) - 2, NumericType()),
                    ],
                )

            # e.g. QUARTER(x) <= 2 <=> MONTH(X) <= 6
            case (pydop.LEQ, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (1, 2, 3):
                result = CallExpression(
                    pydop.LEQ,
                    expr.data_type,
                    [
                        self.switch_operator(func_expr, pydop.MONTH),
                        LiteralExpression(lit_expr.value * 3, NumericType()),
                    ],
                )

            # e.g. QUARTER(x) > 1 <=> MONTH(X) > 3
            case (pydop.GRT, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (1, 2, 3):
                result = CallExpression(
                    pydop.GRT,
                    expr.data_type,
                    [
                        self.switch_operator(func_expr, pydop.MONTH),
                        LiteralExpression(lit_expr.value * 3, NumericType()),
                    ],
                )

            # e.g. QUARTER(x) >= 3 <=> MONTH(X) >= 7
            case (pydop.GEQ, pydop.QUARTER, NumericType()) if isinstance(
                lit_expr.value, int
            ) and lit_expr.value in (2, 3, 4):
                result = CallExpression(
                    pydop.GEQ,
                    expr.data_type,
                    [
                        self.switch_operator(func_expr, pydop.MONTH),
                        LiteralExpression((lit_expr.value * 3) - 2, NumericType()),
                    ],
                )

            # Fall back to the original expression by default.
            case _:
                pass
        return result

    def get_timestamp_literal(self, expr: RelationalExpression) -> pd.Timestamp | None:
        """
        Attempts to extract a pandas Timestamp from a literal expression. Does
        not try to parse strings with alphabetic characters to avoid parsing
        things like 'now' that depend on the current date.

        Args:
            `expr`: The expression to extract the timestamp from.

        Returns:
            A pandas Timestamp if the expression is a literal that can be
            converted to a timestamp, otherwise None.
        """
        if not isinstance(expr, LiteralExpression):
            return None
        if isinstance(expr.value, pd.Timestamp):
            return expr.value
        elif isinstance(expr.value, datetime.date):
            return pd.Timestamp(expr.value)
        elif isinstance(expr.value, str) and not any(c.isalpha() for c in expr.value):
            try:
                return pd.Timestamp(expr.value)
            except Exception:
                return None
        else:
            return None

    def simplify_datetime_literal_part(
        self,
        expr: RelationalExpression,
        op: pydop.PyDoughExpressionOperator,
        lit_expr: LiteralExpression,
    ) -> RelationalExpression:
        """
        Attempts to simplify a datetime part extraction function call with a
        literal argument, e.g. `YEAR('2020-05-01')` can be simplified to `2020`.

        Args:
            `expr`: The original expression representing the datetime part
            extraction. This should be returned if there is no simplification
            possible.
            `op`: The datetime part extraction operator (e.g. YEAR, MONTH, DAY,
            etc).
            `lit_expr`: The literal expression argument to the datetime part
            extraction function.

        Returns:
            The simplified expression if possible, otherwise the original
            expression.
        """
        # Extract a pandas Timestamp from the literal if possible. Allows cases
        # where the literal is a native Python datetime/date, a pandas
        # Timestamp, or a string without any alphabetic characters (to avoid
        # parsing things like 'now' that depend on the current date).
        timestamp_value: pd.Timestamp | None = self.get_timestamp_literal(lit_expr)

        # Fall back to the original expression by default.
        if timestamp_value is None:
            return expr

        # Otherwise, extract the relevant part from the timestamp and return it
        # as a literal.
        match op:
            case pydop.YEAR:
                return LiteralExpression(timestamp_value.year, NumericType())
            case pydop.QUARTER:
                quarter: int = ((timestamp_value.month - 1) // 3) + 1
                return LiteralExpression(quarter, NumericType())
            case pydop.MONTH:
                return LiteralExpression(timestamp_value.month, NumericType())
            case pydop.DAY:
                return LiteralExpression(timestamp_value.day, NumericType())
            case pydop.HOUR:
                return LiteralExpression(timestamp_value.hour, NumericType())
            case pydop.MINUTE:
                return LiteralExpression(timestamp_value.minute, NumericType())
            case pydop.SECOND:
                return LiteralExpression(timestamp_value.second, NumericType())
            case pydop.DAYNAME:
                return LiteralExpression(timestamp_value.day_name(), StringType())
            case pydop.DAYOFWEEK:
                # Derive the day of week as an integer, adjusting based on the
                # configured start of the week.
                dow: int = timestamp_value.weekday()
                dow -= self.configs.start_of_week.pandas_dow
                dow %= 7
                if not self.configs.start_week_as_zero:
                    dow += 1
                return LiteralExpression(dow, NumericType())
            case _:
                return expr

    def compress_datetime_literal_chain(
        self, expr: CallExpression
    ) -> RelationalExpression:
        """
        Attempts to compress a DATETIME(arg0, arg1, arg2, ...) function call
        where arg0 is a timestamp literal and all other arguments are string
        literals representing datetime modifiers (e.g. 'start of month',
        '+3 days', etc). If successful, returns a LiteralExpression with the
        resulting timestamp or date. If not successful, returns the original
        expression.

        Args:
            `expr`: The CallExpression representing the DATETIME function call.
            Assumes all the arguments are literals.

        Returns:
            A LiteralExpression with the resulting timestamp or date if
            successful, otherwise the original expression.
        """
        assert expr.op == pydop.DATETIME and len(expr.inputs) > 0

        # Extract a pandas Timestamp from the first argument if possible. If
        # not possible, return the original expression.
        timestamp_value: pd.Timestamp | None = self.get_timestamp_literal(
            expr.inputs[0]
        )
        if timestamp_value is None:
            return expr

        # Extract the raw string values from the remaining arguments. If any
        # of them are not string literals, return the original expression.
        raw_args: list[str] = []
        for arg in expr.inputs[1:]:
            if isinstance(arg, LiteralExpression) and isinstance(arg.value, str):
                raw_args.append(arg.value)
            else:
                return expr

        # Keep track of whether the final result should be returned as a date
        # (i.e. without a time component) or as a timestamp.
        return_as_date: bool = timestamp_value == timestamp_value.normalize()

        # Process each argument in order, applying truncations and offsets to
        # the timestamp value as needed. If any argument is not recognized,
        # return the original expression.
        for raw_arg in raw_args:
            amt: int
            unit: DateTimeUnit | None
            trunc_match: re.Match | None = trunc_pattern.fullmatch(raw_arg)
            offset_match: re.Match | None = offset_pattern.fullmatch(raw_arg)
            if trunc_match is not None:
                # If the string is in the form `start of <unit>`, apply
                # truncation.
                unit = DateTimeUnit.from_string(str(trunc_match.group(1)))
                if unit is None:
                    raise ValueError(
                        f"Unsupported DATETIME modifier string: {raw_arg!r}"
                    )
                match unit:
                    case DateTimeUnit.YEAR:
                        timestamp_value = timestamp_value.to_period("Y").to_timestamp()
                        return_as_date = True
                    case DateTimeUnit.QUARTER:
                        timestamp_value = timestamp_value.to_period("Q").to_timestamp()
                        return_as_date = True
                    case DateTimeUnit.MONTH:
                        timestamp_value = timestamp_value.to_period("M").to_timestamp()
                        return_as_date = True
                    case DateTimeUnit.WEEK:
                        # Compute the number of day since the start of the week
                        # (accounting for the session configs) and subtract that
                        # many days from the normalized timestamp.
                        dow: int = timestamp_value.weekday()
                        dow -= self.configs.start_of_week.pandas_dow
                        dow %= 7
                        timestamp_value = timestamp_value.normalize() - pd.Timedelta(
                            days=dow
                        )
                        return_as_date = True
                    case DateTimeUnit.DAY:
                        timestamp_value = timestamp_value.floor("d")
                        return_as_date = True
                    case DateTimeUnit.HOUR:
                        timestamp_value = timestamp_value.floor("h")
                    case DateTimeUnit.MINUTE:
                        timestamp_value = timestamp_value.floor("min")
                    case _:
                        # Doesn't support truncating to SECOND.
                        return expr
            elif offset_match is not None:
                # If the string is in the form `±<amt> <unit>`, apply an
                # offset.
                amt = int(offset_match.group(2))
                if str(offset_match.group(1)) == "-":
                    amt *= -1
                unit = DateTimeUnit.from_string(str(offset_match.group(3)))
                if unit is None:
                    raise ValueError(
                        f"Unsupported DATETIME modifier string: {raw_arg!r}"
                    )
                match unit:
                    case DateTimeUnit.YEAR:
                        timestamp_value = timestamp_value + pd.DateOffset(years=amt)
                    case DateTimeUnit.QUARTER:
                        timestamp_value = timestamp_value + pd.DateOffset(
                            months=amt * 3
                        )
                    case DateTimeUnit.MONTH:
                        timestamp_value = timestamp_value + pd.DateOffset(months=amt)
                    case DateTimeUnit.WEEK:
                        timestamp_value = timestamp_value + pd.DateOffset(days=amt * 7)
                    case DateTimeUnit.DAY:
                        timestamp_value = timestamp_value + pd.DateOffset(days=amt)
                    case DateTimeUnit.HOUR:
                        timestamp_value = timestamp_value + pd.Timedelta(hours=amt)
                        return_as_date = False
                    case DateTimeUnit.MINUTE:
                        timestamp_value = timestamp_value + pd.Timedelta(minutes=amt)
                        return_as_date = False
                    case DateTimeUnit.SECOND:
                        timestamp_value = timestamp_value + pd.Timedelta(seconds=amt)
                        return_as_date = False
            else:
                return expr

        # Return the final timestamp as a literal expression, converting to a
        # date if needed.
        if return_as_date:
            return LiteralExpression(timestamp_value.date(), expr.data_type)
        else:
            return LiteralExpression(timestamp_value, expr.data_type)

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

        # Return None if any of the inputs are None and the operator is
        # guaranteed to return NULL if any of its inptus are NULL.
        if expr.op in NULL_IF_INPUT_NULL_OPS:
            if any(
                isinstance(arg, LiteralExpression) and arg.value is None
                for arg in expr.inputs
            ):
                self.stack.append(output_predicates)
                return LiteralExpression(None, expr.data_type)

        # If the call has null propagating rules, all of the arguments are
        # non-null, the output is guaranteed to be non-null.
        if expr.op in NULLABLE_IF_INPUT_NULLABLE_OPS:
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
            # already accounted for with NULLABLE_IF_INPUT_NULLABLE_OPS.
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

                    # In cases where we do FUNC(x) cmp LIT, attempt additional
                    # simplifications.
                    case (CallExpression(), _, LiteralExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, expr.op, expr.inputs[0], expr.inputs[1]
                        )
                    case (LiteralExpression(), pydop.EQU | pydop.NEQ, CallExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, expr.op, expr.inputs[1], expr.inputs[0]
                        )
                    case (LiteralExpression(), pydop.GRT, CallExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, pydop.LET, expr.inputs[1], expr.inputs[0]
                        )
                    case (LiteralExpression(), pydop.GEQ, CallExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, pydop.LEQ, expr.inputs[1], expr.inputs[0]
                        )
                    case (LiteralExpression(), pydop.LET, CallExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, pydop.GRT, expr.inputs[1], expr.inputs[0]
                        )
                    case (LiteralExpression(), pydop.LEQ, CallExpression()):
                        output_expr = self.simplify_function_literal_comparison(
                            expr, pydop.GEQ, expr.inputs[1], expr.inputs[0]
                        )

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
            # KEEP_IF(None, y) -> None
            case pydop.KEEP_IF:
                if isinstance(expr.inputs[1], LiteralExpression):
                    if bool(expr.inputs[1].value):
                        output_expr = expr.inputs[0]
                        output_predicates |= arg_predicates[0]
                    else:
                        output_expr = LiteralExpression(None, expr.data_type)
                        output_predicates.not_negative = True
                elif (
                    isinstance(expr.inputs[0], LiteralExpression)
                    and expr.inputs[0].value is None
                ):
                    output_expr = LiteralExpression(None, expr.data_type)
                elif arg_predicates[1].not_null and arg_predicates[1].positive:
                    output_expr = expr.inputs[0]
                    output_predicates = arg_predicates[0]
                else:
                    # Otherwise the predicates are the same as the first
                    # argument, except it can be null.
                    output_predicates |= arg_predicates[0]
                    output_predicates.not_null = False

            # DATETIME(DATETIME(u, v, w), x, y, z) -> DATETIME(u, v, w, x, y, z)
            case pydop.DATETIME:
                if (
                    isinstance(expr.inputs[0], CallExpression)
                    and expr.inputs[0].op == pydop.DATETIME
                ):
                    output_expr = CallExpression(
                        pydop.DATETIME,
                        expr.data_type,
                        expr.inputs[0].inputs + expr.inputs[1:],
                    )
                assert isinstance(output_expr, CallExpression)
                if all(
                    isinstance(arg, LiteralExpression) for arg in output_expr.inputs
                ):
                    output_expr = self.compress_datetime_literal_chain(output_expr)

            # YEAR(literal_datetime) -> can infer the year as a literal
            # (same for QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, DAYOFWEEK,
            # and DAYNAME)
            case (
                pydop.YEAR
                | pydop.QUARTER
                | pydop.MONTH
                | pydop.DAY
                | pydop.HOUR
                | pydop.MINUTE
                | pydop.SECOND
                | pydop.DAYOFWEEK
                | pydop.DAYNAME
            ):
                if isinstance(expr.inputs[0], LiteralExpression):
                    output_expr = self.simplify_datetime_literal_part(
                        expr, expr.op, expr.inputs[0]
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

    def __init__(
        self,
        configs: PyDoughConfigs,
        additional_shuttles: list[RelationalExpressionShuttle],
    ):
        self.stack: list[dict[RelationalExpression, PredicateSet]] = []
        self.shuttle: SimplificationShuttle = SimplificationShuttle(configs)
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

    def infer_null_predicates_from_condition(
        self,
        output_predicates: dict[RelationalExpression, PredicateSet],
        condition: RelationalExpression,
        columns: dict[str, RelationalExpression],
    ) -> None:
        """
        Infers whether an output column can be marked as not-null based on the
        given condition expression. If the condition implies that a column is
        not null, the corresponding PredicateSet in output_predicates is updated
        in-place.

        Args:
            `output_predicates`: A dictionary mapping each output column
            reference from the current node to the set of its inferred
            predicates.
            `condition`: The condition expression from the current node (e.g. a
            filter or an inner/semi join) which, if false when a certain column
            is null, means that column can be marked as not-null in the output.
            `columns`: A dictionary mapping column names to their corresponding
            relational expressions in the current node.
        """
        from .filter_pushdown import NullReplacementShuttle

        self.shuttle.input_predicates = {}
        # Iterate across all of the output columns that are not already marked
        # as not-null and identify the ones that correspond to a column
        # reference passed through from the input node.
        for expr, preds in output_predicates.items():
            if preds.not_null:
                continue
            if isinstance(expr, ColumnReference) and expr.name in columns:
                expr = columns[expr.name]
                if isinstance(expr, ColumnReference):
                    # Transform the condition by creating a version where the
                    # input column is replaced with a NULL literal, and then run
                    # the simplifier on the new expression.
                    shuttle: NullReplacementShuttle = NullReplacementShuttle(
                        {expr.name}
                    )
                    new_cond: RelationalExpression = condition.accept_shuttle(shuttle)
                    new_cond = new_cond.accept_shuttle(self.shuttle)
                    # If the new condition simplifies to a False-y literal, then
                    # the column must be not-null since it means that if the
                    # column were, the row would be filtered out.
                    if isinstance(new_cond, LiteralExpression) and not bool(
                        new_cond.value
                    ):
                        preds.not_null = True

    def visit_filter(self, node: Filter) -> None:
        output_predicates: dict[RelationalExpression, PredicateSet] = (
            self.generic_visit(node)
        )
        # Transform the filter condition in-place.
        node._condition = node.condition.accept_shuttle(self.shuttle)
        self.shuttle.stack.pop()
        for shuttle in self.additional_shuttles:
            node._condition = node.condition.accept_shuttle(shuttle)
        self.infer_null_predicates_from_condition(
            output_predicates,
            node.condition,
            node.columns,
        )
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

        if node.join_type in (JoinType.INNER, JoinType.SEMI):
            self.infer_null_predicates_from_condition(
                output_predicates,
                node.condition,
                node.columns,
            )
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
    configs: PyDoughConfigs,
    additional_shuttles: list[RelationalExpressionShuttle],
) -> None:
    """
    Transforms the current node and all of its descendants in-place to simplify
    any relational expressions.

    Args:
        `node`: The relational node to perform simplification on.
        `configs`: The PyDough configuration settings.
        `additional_shuttles`: A list of additional shuttles to apply to the
        expressions of the node and its descendants. These shuttles are applied
        after the simplification shuttle, and can be used to perform additional
        transformations on the expressions.
    """
    simplifier: SimplificationVisitor = SimplificationVisitor(
        configs, additional_shuttles
    )
    node.accept(simplifier)
