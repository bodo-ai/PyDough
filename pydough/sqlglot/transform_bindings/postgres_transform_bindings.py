"""
Definition of SQLGlot transformation bindings for the Postgres dialect.
"""

__all__ = ["PostgresTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
    apply_parens,
)


class PostgresTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Postgres dialect.
    """

    PYDOP_TO_POSTGRES_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.CEIL: "CEIL",
        pydop.FLOOR: "FLOOR",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }

    """
    Mapping of PyDough operators to equivalent Postgres function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        match operator:
            case pydop.AVG:
                return self.convert_avg(args, types)

        if operator in self.PYDOP_TO_POSTGRES_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_POSTGRES_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_sum(
        self, arg: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Converts a SUM function call to its SQLGlot equivalent.
        This method checks the type of the argument to determine whether to use
        COUNT_IF (for BooleanType) or SUM (for other types).
        Arguments:
            arg (SQLGlotExpression): The argument to the SUM function.
            types (list[PyDoughType]): The types of the arguments.
        """
        match types[0]:
            # If the argument is of BooleanType, it uses COUNT_IF to count true values.
            case BooleanType():
                return sqlglot_expressions.CountIf(this=arg[0])
            case _:
                # For other types, use SUM directly
                return sqlglot_expressions.Sum(this=arg[0])

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # Check if position is a CAST to BIGINT. If it is change it to INT.

        assert len(args) == 3
        string_expr, delimiter_expr, index_expr = args

        result: SQLGlotExpression = sqlglot_expressions.SplitPart(
            this=string_expr,  # string expression
            delimiter=delimiter_expr,  # delimiter
            part_index=sqlglot_expressions.Cast(
                this=sqlglot_expressions.Case(  # position (1-based)
                    ifs=[
                        sqlglot_expressions.If(
                            this=sqlglot_expressions.EQ(
                                this=index_expr,
                                expression=sqlglot_expressions.Literal.number(0),
                            ),
                            true=sqlglot_expressions.Literal.number(1),
                        )
                    ],
                    default=index_expr,
                ),
                to=sqlglot_expressions.DataType.build("INTEGER"),
            ),
        )
        return result

    def dialect_day_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        extract_expr: SQLGlotExpression = sqlglot_expressions.Extract(
            this=sqlglot_expressions.var("DOW"),
            expression=base,
        )
        return extract_expr

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        if unit == DateTimeUnit.QUARTER:
            unit = DateTimeUnit.MONTH
            amt *= 3

        return super().apply_datetime_offset(base, amt, unit)

    def convert_extract_datetime(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
        unit: DateTimeUnit,
    ) -> SQLGlotExpression:
        assert len(args) == 1

        result = sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit.value.upper()),
            expression=self.make_datetime_arg(args[0]),
        )

        if unit == DateTimeUnit.SECOND:
            result = sqlglot_expressions.Cast(
                this=result, to=sqlglot_expressions.DataType.build("BIGINT")
            )

        return result

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        assert len(args) == 3
        # Check if unit is a string.
        if not (isinstance(args[0], sqlglot_expressions.Literal) and args[0].is_string):
            raise ValueError(
                f"Unsupported argument for DATEDIFF: {args[0]!r}. It should be a string literal."
            )
        date1: SQLGlotExpression = self.make_datetime_arg(args[1])
        date2: SQLGlotExpression = self.make_datetime_arg(args[2])
        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)

        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

        year_diff: SQLGlotExpression = sqlglot_expressions.Sub(
            this=self.convert_extract_datetime([date2], [types[2]], DateTimeUnit.YEAR),
            expression=self.convert_extract_datetime(
                [date1], [types[1]], DateTimeUnit.YEAR
            ),
        )

        result: SQLGlotExpression

        match unit:
            case DateTimeUnit.YEAR:
                # YEAR(date2) - YEAR(date1)
                return year_diff
            case DateTimeUnit.QUARTER:
                # (YEAR(date2) - YEAR(date1)) * 4 + (QUARTER(date2) - QUARTER(date1))
                literal_4: SQLGlotExpression = sqlglot_expressions.Literal.number(4)

                quarter_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=self.convert_extract_datetime(
                        [date2], [types[2]], DateTimeUnit.QUARTER
                    ),
                    expression=self.convert_extract_datetime(
                        [date1], [types[1]], DateTimeUnit.QUARTER
                    ),
                )
                result = sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff), expression=literal_4
                    ),
                    expression=apply_parens(quarter_diff),
                )
                return result
            case DateTimeUnit.MONTH:
                # (YEAR(date2) - YEAR(date1)) * 12 + (MONTH(date2) - MONTH(date1))
                literal_12: SQLGlotExpression = sqlglot_expressions.Literal.number(12)

                month_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=self.convert_extract_datetime(
                        [date2], [types[2]], DateTimeUnit.MONTH
                    ),
                    expression=self.convert_extract_datetime(
                        [date1], [types[1]], DateTimeUnit.MONTH
                    ),
                )
                result = sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff), expression=literal_12
                    ),
                    expression=apply_parens(month_diff),
                )
                return result

            case DateTimeUnit.WEEK:
                # raw_delta = number of days between date1 and date2
                # dow1 = DAYOFWEEK(date1)
                # dow2 = DAYOFWEEK(date2)
                # result = INTEGER((raw_delta + dow1 - dow2) / 7)
                raw_delta = self.convert_datediff(
                    [sqlglot_expressions.convert("DAY"), date1, date2], types
                )

                dow1 = self.convert_dayofweek([date1], [types[1]])
                dow2 = self.convert_dayofweek([date2], [types[2]])
                divion = sqlglot_expressions.Div(
                    this=apply_parens(
                        sqlglot_expressions.Add(
                            this=raw_delta,
                            expression=sqlglot_expressions.Sub(
                                this=dow1, expression=dow2
                            ),
                        )
                    ),
                    expression=sqlglot_expressions.Literal.number(7),
                )

                return sqlglot_expressions.Cast(
                    this=divion, to=sqlglot_expressions.DataType.build("BIGINT")
                )

            case DateTimeUnit.DAY:
                # EXTRACT(DAY FROM (date2 - date1))
                result = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Cast(
                        this=date2, to=sqlglot_expressions.DataType.build("DATE")
                    ),
                    expression=sqlglot_expressions.Cast(
                        this=date1, to=sqlglot_expressions.DataType.build("DATE")
                    ),
                )

                return result

            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE:
                # CAST(EXTRACT(EPOCH FROM (
                #     DATE_TRUNC('{hour/minute}', o_orderdate)
                #     - DATE_TRUNC('hour/minute}', TIMESTAMP '1993-05-25 12:45:36')
                # )) / {3600/60} AS BIGINT)

                division_literal: int = 3600 if unit == DateTimeUnit.HOUR else 60

                date1_truc: SQLGlotExpression = sqlglot_expressions.TimestampTrunc(
                    this=date1, unit=unit
                )

                date2_truc: SQLGlotExpression = sqlglot_expressions.TimestampTrunc(
                    this=date2, unit=unit
                )

                sub_dates: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=date2_truc, expression=date1_truc
                )

                extract_epoch: SQLGlotExpression = sqlglot_expressions.Extract(
                    this=sqlglot_expressions.Var(this="EPOCH"),
                    expression=apply_parens(sub_dates),
                )

                division: SQLGlotExpression = sqlglot_expressions.Div(
                    this=extract_epoch,
                    expression=sqlglot_expressions.Literal.number(division_literal),
                )

                result = sqlglot_expressions.Cast(
                    this=division, to=sqlglot_expressions.DataType.build("BIGINT")
                )
                return result

            case DateTimeUnit.SECOND:
                # CAST(EXTRACT(EPOCH FROM (date2 - date1)) AS BIGINT)
                substraction: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Cast(
                        this=date2, to=sqlglot_expressions.DataType.build("TIMESTAMP")
                    ),
                    expression=sqlglot_expressions.Cast(
                        this=date1, to=sqlglot_expressions.DataType.build("TIMESTAMP")
                    ),
                )

                result = sqlglot_expressions.Extract(
                    this=sqlglot_expressions.Var(this="EPOCH"),
                    expression=apply_parens(substraction),
                )
                return result
            case _:
                raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        if unit == DateTimeUnit.WEEK:
            dow = self.days_from_start_of_week(base)
            # The week is truncated to the start of the week
            # base - CAST(dow || ' days' as INTERVAL)
            minus_dow: SQLGlotExpression = sqlglot_expressions.Sub(
                this=base,
                expression=sqlglot_expressions.Cast(
                    this=sqlglot_expressions.DPipe(
                        this=dow,
                        expression=sqlglot_expressions.Literal.string(" days"),
                        safe=True,
                    ),
                    to=sqlglot_expressions.DataType.build("INTERVAL"),
                ),
            )

            return self.apply_datetime_truncation(minus_dow, DateTimeUnit.DAY)

        else:
            return super().apply_datetime_truncation(base, unit)

    def convert_slice(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Convert a PyDough slice operation to a SQLGlot expression.
        Postgres uses the SUBSTRING function for slicing.

        Outline of the logic:
        - If the start index is None, it defaults to 1 (1-based indexing).
        - If the stop index is None, it defaults to the length of the string.
        - a = start index
        - b = stop index
        match (a, b):
            case (None, None):
                return SUBSTRING(x FROM 1)
            case (+a, None):
                return SUBSTRING(x FROM a + 1)
            case (-a, None):
                return SUBSTRING(x FROM LENGTH(x) - a + 1)
            case (None, +b):
                return SUBSTRING(x FROM 1 FOR b)
            case (None, -b):
                return SUBSTRING(x FROM 1 FOR GREATEST(LENGTH(x) - b, 0))
            case (+a, +b):
                return SUBSTRING(x FROM a+1 FOR GREATEST(b - a, 0))
            case (-a, -b):
                return SUBSTRING(x
                    FROM LENGTH(x) - a + 1
                    FOR GREATEST((LENGTH(x) + b) - GREATEST(LENGTH(x) + a, 0), 0)
                )
            case (+a, -b):
                return SUBSTRING(x
                    FROM a + 1
                    FOR GREATEST(LENGTH(x) + b - a, 0)
                )
            case (-a, +b):
                SUBSTRING(x
                    FROM GREATEST(LENGTH(x) + a + 1, 1)
                    FOR b - GREATEST(LENGTH(x) + a, 0)
                )
        """

        assert len(args) == 4
        string_expr, start, stop, step = args

        start_idx: int | None = None
        if not isinstance(start, sqlglot_expressions.Null):
            if isinstance(start, sqlglot_expressions.Literal):
                try:
                    start_idx = int(start.this)
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the start index being integer literal or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the start index being integer literal or absent."
                )

        stop_idx: int | None = None
        if not isinstance(stop, sqlglot_expressions.Null):
            if isinstance(stop, sqlglot_expressions.Literal):
                try:
                    stop_idx = int(stop.this)
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the stop index being integer literal or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the stop index being integer literal or absent."
                )

        step_idx: int | None = None
        if not isinstance(step, sqlglot_expressions.Null):
            if isinstance(step, sqlglot_expressions.Literal):
                try:
                    step_idx = int(step.this)
                    if step_idx != 1:
                        raise ValueError(
                            "SLICE function currently only supports the step being integer literal 1 or absent."
                        )
                except ValueError:
                    raise ValueError(
                        "SLICE function currently only supports the step being integer literal 1 or absent."
                    )
            else:
                raise ValueError(
                    "SLICE function currently only supports the step being integer literal 1 or absent."
                )

        # SQLGlot expressions for 0 and 1 and empty string
        sql_one: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
        sql_zero: SQLGlotExpression = sqlglot_expressions.Literal.number(0)
        expr_length: SQLGlotExpression = sqlglot_expressions.Length(this=string_expr)
        one_index_start: SQLGlotExpression = sqlglot_expressions.Add(
            this=start, expression=sql_one
        )
        # length adjustment
        length: SQLGlotExpression = None

        match (start_idx, stop_idx):
            case (None, end_idx) if end_idx is not None and end_idx >= 0:
                length = stop

            case (None, end_idx) if end_idx is not None and end_idx < 0:
                # GREATEST(LENGTH(x) - b, 0)
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Add(this=expr_length, expression=stop),
                    expressions=[sql_zero],
                )

            case (begin_idx, end_idx) if (
                begin_idx is not None
                and end_idx is not None
                and begin_idx >= 0
                and end_idx >= 0
            ):
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(this=stop, expression=start),
                    expressions=[sql_zero],
                )

            case (begin_idx, end_idx) if (
                begin_idx is not None
                and end_idx is not None
                and begin_idx < 0
                and end_idx < 0
            ):
                # GREATEST((LENGTH(x) + b) - GREATEST(LENGTH(x) + a, 0), 0)
                sub_len_b: SQLGlotExpression = sqlglot_expressions.Add(
                    this=expr_length, expression=stop
                )

                sub_len_a: SQLGlotExpression = sqlglot_expressions.Add(
                    this=expr_length, expression=start
                )
                greatest_len_a: SQLGlotExpression = sqlglot_expressions.Greatest(
                    this=sub_len_a, expressions=[sql_zero]
                )
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(
                        this=sub_len_b, expression=greatest_len_a
                    ),
                    expressions=[sql_zero],
                )

            case (begin_idx, end_idx) if (
                begin_idx is not None
                and end_idx is not None
                and begin_idx >= 0
                and end_idx < 0
            ):
                # GREATEST(LENGTH(x) + b - a, 0)
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(
                        this=sqlglot_expressions.Add(this=expr_length, expression=stop),
                        expression=start,
                    ),
                    expressions=[sql_zero],
                )

            case (begin_idx, end_idx) if (
                begin_idx is not None
                and end_idx is not None
                and begin_idx < 0
                and end_idx >= 0
            ):
                # b - GREATEST(LENGTH(x) + a, 0)
                length = sqlglot_expressions.Greatest(
                    this=sqlglot_expressions.Sub(
                        this=stop,
                        expression=sqlglot_expressions.Greatest(
                            this=sqlglot_expressions.Add(
                                this=expr_length,
                                expression=start,
                            ),
                            expressions=[sql_zero],
                        ),
                    ),
                    expressions=[sql_zero],
                )

        # start adjustment
        if start_idx is not None and start_idx >= 0:
            start = one_index_start
        elif start_idx is not None and start_idx < 0:
            # GREATEST(LENGTH(x) + a + 1, 1)
            start = sqlglot_expressions.Greatest(
                this=sqlglot_expressions.Add(
                    this=sqlglot_expressions.Add(this=expr_length, expression=start),
                    expression=sql_one,
                ),
                expressions=[sql_one],
            )

        elif start_idx is None:
            start = sql_one

        result: SQLGlotExpression = sqlglot_expressions.Substring(
            this=string_expr, start=start, length=length
        )

        return result

    def convert_round(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        assert len(args) == 1 or len(args) == 2
        precision_glot: SQLGlotExpression
        if len(args) == 1:
            precision_glot = sqlglot_expressions.Literal.number(0)
        else:
            # Check if the second argument is a integer literal.
            if (
                not isinstance(args[1], sqlglot_expressions.Literal)
                or args[1].is_string
            ):
                raise ValueError(
                    f"Unsupported argument {args[1]} for ROUND."
                    "The precision argument should be an integer literal."
                )
            try:
                int(args[1].this)
            except ValueError:
                raise ValueError(
                    f"Unsupported argument {args[1]} for ROUND."
                    "The precision argument should be an integer literal."
                )
            precision_glot = args[1]
        return sqlglot_expressions.Round(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType.build("NUMERIC")
            ),
            decimals=precision_glot,
        )

    def convert_avg(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        assert len(args) == 1

        return sqlglot_expressions.Avg(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType.build("NUMERIC")
            )
        )

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        return sqlglot_expressions.Cast(
            this=args[0], to=sqlglot_expressions.DataType.build("INTEGER")
        )

    def convert_lpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        result: SQLGlotExpression = sqlglot_expressions.Pad(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType.build("TEXT")
            ),
            expression=args[1],
            fill_pattern=args[2],
            is_left=True,
        )
        return result

    def convert_rpad(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        result: SQLGlotExpression = sqlglot_expressions.Pad(
            this=sqlglot_expressions.Cast(
                this=args[0], to=sqlglot_expressions.DataType.build("TEXT")
            ),
            expression=args[1],
            fill_pattern=args[2],
            is_left=False,
        )
        return result
