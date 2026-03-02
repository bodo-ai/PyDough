"""
Definition of SQLGlot transformation bindings for the Oracle dialect.
"""

__all__ = ["OracleTransformBindings"]


import math
from typing import Any

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.errors.error_types import PyDoughSQLException
from pydough.types import PyDoughType
from pydough.types.boolean_type import BooleanType
from pydough.types.datetime_type import DatetimeType
from pydough.types.numeric_type import NumericType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
    apply_parens,
)


class OracleTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the Oracle dialect.
    """

    @property
    def dialect_dow_mapping(self) -> dict[str, int]:
        return {
            "Sunday": 1,
            "Monday": 2,
            "Tuesday": 3,
            "Wednesday": 4,
            "Thursday": 5,
            "Friday": 6,
            "Saturday": 7,
        }

    @property
    def values_alias_column(self) -> bool:
        return False

    @property
    def oracle_strftime_mapping(self) -> dict[str, str]:
        """
        This mapping is used by `oracle_format` when converting a
        format string supplied by the user (which typically uses Python
        `strftime`-style specifiers) into a form that Oracle will
        understand.  Only a small subset of directives is currently
        supported; additional tokens may be added as needed.
        """
        return {
            "%Y": "YYYY",
            "%m": "MM",
            "%d": "DD",
            "%H": "HH24",
            "%M": "MI",
            "%S": "SS",
        }

    PYDOP_TO_ORACLE_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.ABS: "ABS",
        pydop.LARGEST: "GREATEST",
        pydop.SMALLEST: "LEAST",
        pydop.STRIP: "TRIM",
        pydop.FIND: "INSTR",
        pydop.PERCENTILE: "PERCENTILE_CONT",
    }

    """
    Mapping of PyDough operators to equivalent Oracle function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_ORACLE_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_ORACLE_FUNC[operator], expressions=args
            )
        match operator:
            case pydop.DEFAULT_TO:
                # sqlglot convert COALESCE in NVL for Oracle, which is fine for
                # 2 args but with more sqlglot doesn't handle it correctly.
                return self.convert_default_to(args, types)

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_default_to(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Convert a DEFAULT_TO PyDough function in COALESCE handling correctly
        more than 2 arguments.

        Sqlglot converts COALESCE in NVL for Oracle. For 2+ args this becomes a
        nested NVL call.

        Args:
            `args`: The arguments for the COALESE expression.
            `types`: The PyDough types of the arguments.

        Returns:
            COALESCE expression with its arguments correctly handled
        """
        return sqlglot_expressions.Coalesce(this=args[0], expressions=args[1:])

    def convert_str_count(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        STRCOUNT(X, Y) =>
        CASE
            WHEN LENGTH(X) IS NULL OR LENGTH(Y) IS NULL THEN 0
            ELSE
            CAST((LENGTH(X) - NVL(LENGTH(REPLACE(X, Y, '')), 0)) / LENGTH(Y), AS INTEGER)
        END
        """
        assert len(args) == 2

        string: SQLGlotExpression = args[0]
        substring_count: SQLGlotExpression = args[1]

        # eliminate the substring of the string: REPLACE(X, Y, "")
        string_replaced: SQLGlotExpression = self.convert_replace(
            [string, substring_count], types
        )

        # The length of the first string given: LENGH(X)
        len_string: SQLGlotExpression = sqlglot_expressions.Length(this=string)

        # The length of the replaced string: NVL(LENGH(REPLACE(X, Y, "")), 0)
        # NVL(LENGTH(REPLACE('aaaa', 'aa', '')), 0)
        len_string_replaced: SQLGlotExpression = sqlglot_expressions.Coalesce(
            this=sqlglot_expressions.Length(this=string_replaced),
            expressions=[sqlglot_expressions.Literal.number(0)],
            is_nvl=True,
        )

        # The length of the Y string: LENGTH(Y)
        len_substring_count: SQLGlotExpression = sqlglot_expressions.Length(
            this=substring_count
        )

        # The length difference between string X and
        # replaced string: REPLACE(X, Y, "")
        difference: SQLGlotExpression = sqlglot_expressions.Sub(
            this=len_string, expression=len_string_replaced
        )

        # Take in count if LENGH(Y) > 1 dividing the difference by Y's length:
        # LENGTH(X) - LENGTH(REPLACE(X, Y, ''))) / LENGTH(Y)
        quotient: SQLGlotExpression = sqlglot_expressions.Div(
            this=apply_parens(difference), expression=len_substring_count
        )

        # Cast to Interger:
        # CAST((LENGTH(X) - LENGTH(REPLACE(X, Y, ''))) / LENGTH(Y), AS INTEGER)
        casted: SQLGlotExpression = sqlglot_expressions.Cast(
            this=quotient, to=sqlglot_expressions.DataType.build("BIGINT")
        )

        # CASE when LENGTH(X) IS NULL OR LENGTH(Y) IS NULL THEN 0 else casted
        answer: SQLGlotExpression = (
            sqlglot_expressions.Case()
            .when(
                sqlglot_expressions.Or(
                    this=sqlglot_expressions.Is(
                        this=len_string,
                        expression=sqlglot_expressions.Null(),
                    ),
                    expression=sqlglot_expressions.Is(
                        this=len_substring_count, expression=sqlglot_expressions.Null()
                    ),
                ),
                sqlglot_expressions.Literal.number(0),
            )
            .else_(casted)
        )
        return answer

    def convert_slice(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Convert a PyDough slice operation to a SQLGlot expression.
        Oracle uses the SUBSTR function for slicing.

        Outline of the logic:
        - If the start index is None, it defaults to 1 (1-based indexing).
        - If the stop index is None, it defaults to the length of the string.
        - a = start index
        - b = stop index
        match (a, b):
            case (None, None):
                return SUBSTR(x, 1)
            case (+a, None):
                return SUBSTR(x, a + 1)
            case (-a, None):
                return SUBSTR(x, a)
            case (None, +b):
                return SUBSTR(x, 1, b)
            case (None, -b):
                return SUBSTR(x, 1, LENGTH(x) + b)
            case (+a, +b):
                return SUBSTR(x, a + 1, GREATEST(b - a, 0))
            case (-a, -b):
                return SUBSTR(x, a, GREATEST(b - a, 0))
            case (+a, -b):
                return SUBSTR(x, a + 1, GREATEST(LENGTH(x) + b - a, 0))
            case (-a, +b):
                return SUBSTR(x, a, b - GREATEST(LENGTH(x) + a, 0))
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
                length = sqlglot_expressions.Add(this=expr_length, expression=stop)

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
                length = sqlglot_expressions.Case(
                    ifs=[
                        sqlglot_expressions.If(
                            this=sqlglot_expressions.LT(
                                this=expr_length,
                                expression=sqlglot_expressions.Abs(this=start),
                            ),
                            true=sqlglot_expressions.Add(
                                this=expr_length, expression=stop
                            ),
                        )
                    ],
                    default=sqlglot_expressions.Greatest(
                        this=sqlglot_expressions.Sub(this=stop, expression=start),
                        expressions=[sql_zero],
                    ),
                )

            case (begin_idx, end_idx) if (
                begin_idx is not None
                and end_idx is not None
                and begin_idx >= 0
                and end_idx < 0
            ):
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
                length = sqlglot_expressions.Sub(
                    this=stop,
                    expression=sqlglot_expressions.Greatest(
                        this=sqlglot_expressions.Add(
                            this=expr_length, expression=start
                        ),
                        expressions=[sql_zero],
                    ),
                )

        # start adjustment
        if start_idx is not None and start_idx >= 0:
            start = one_index_start
        elif start_idx is not None and start_idx < 0:
            start = sqlglot_expressions.Case(
                ifs=[
                    sqlglot_expressions.If(
                        this=sqlglot_expressions.GT(
                            this=expr_length,
                            expression=sqlglot_expressions.Abs(this=start),
                        ),
                        true=start,
                    )
                ],
                default=sql_one,
            )

        elif start_idx is None:
            start = sql_one

        result: SQLGlotExpression = sqlglot_expressions.Substring(
            this=string_expr, start=start, length=length
        )
        return result

    def convert_get_part(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        GETPART(str, delim, idx) ->
        CASE
            WHEN delim IS NULL OR delim = '' THEN
                CASE
                    WHEN idx = 1 THEN str
                    ELSE NULL
                END
            ELSE
                REGEXP_SUBSTR(
                    str,
                    '(.*?)(' || REGEXP_REPLACE(delim, '([][(){}.*+?^$|\#-])', '\\\1') || '|$)',
                    1,
                    CASE
                        WHEN
                        CASE
                            WHEN idx = 0 THEN 1
                            WHEN idx > 0 THEN idx
                            ELSE
                            (
                                ((LENGTH(str) - LENGTH(REPLACE(str, delim))) / LENGTH(delim)) + 1
                            ) + idx + 1
                        END
                        BETWEEN 1 AND
                        (
                            ((LENGTH(str) - LENGTH(REPLACE(str, delim))) / LENGTH(delim)) + 1
                        )
                        THEN
                        CASE
                            WHEN idx > 0
                            THEN idx
                            ELSE
                            (
                                ((LENGTH(str) - LENGTH(REPLACE(str, delim))) / LENGTH(delim)) + 1
                            ) + idx + 1
                        END
                        ELSE NULL
                    END,
                    NULL,
                    1
                )
        END
        """

        assert len(args) == 3

        string_expr, delimiter_expr, index_expr = args
        literal_0: SQLGlotExpression = sqlglot_expressions.Literal.number(0)
        literal_1: SQLGlotExpression = sqlglot_expressions.Literal.number(1)

        regexp_1: SQLGlotExpression = sqlglot_expressions.Literal.string("(.*?)(")
        replace_chars: SQLGlotExpression = sqlglot_expressions.Literal.string(
            "([][(){}.*+?^$|\\#-])"
        )
        regexp_2: SQLGlotExpression = sqlglot_expressions.Literal.string("|$)")

        delim_regexp: SQLGlotExpression = sqlglot_expressions.DPipe(
            this=regexp_1,
            expression=sqlglot_expressions.DPipe(
                this=sqlglot_expressions.RegexpReplace(
                    this=delimiter_expr,
                    expression=replace_chars,
                    replacement=sqlglot_expressions.Literal.string("\\\\\\1"),
                ),
                expression=regexp_2,
            ),
        )

        # ((LENGTH(str) - LENGTH(REPLACE(str, delim))) / LENGTH(delim)) + 1
        parts: SQLGlotExpression = sqlglot_expressions.Add(
            this=apply_parens(
                sqlglot_expressions.Div(
                    this=apply_parens(
                        sqlglot_expressions.Sub(
                            this=sqlglot_expressions.Length(this=string_expr),
                            expression=sqlglot_expressions.Length(
                                this=sqlglot_expressions.Anonymous(
                                    this="REPLACE",
                                    expressions=[string_expr, delimiter_expr],
                                )
                            ),
                        )
                    ),
                    expression=sqlglot_expressions.Length(this=delimiter_expr),
                )
            ),
            expression=literal_1,
        )

        # (parts) + idx + 1
        occurrences: SQLGlotExpression = sqlglot_expressions.Add(
            this=apply_parens(parts),
            expression=sqlglot_expressions.Add(
                this=apply_parens(index_expr), expression=literal_1
            ),
        )

        # CASE
        #     WHEN idx = 0 THEN 1
        #     WHEN idx > 0 THEN idx
        #     ELSE
        #     (
        #         ((LENGTH(str) - LENGTH(REPLACE(str, delim))) / LENGTH(delim)) + 1
        #     ) + idx + 1
        # END
        case_neg_idx: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(this=index_expr, expression=literal_0),
                    true=literal_1,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.GT(
                        this=apply_parens(index_expr), expression=literal_0
                    ),
                    true=index_expr,
                ),
            ],
            default=occurrences,
        )

        case_occurrence: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.Between(
                        this=case_neg_idx, low=literal_1, high=parts
                    ),
                    true=case_neg_idx,
                )
            ],
            default=sqlglot_expressions.Null(),
        )

        regexp_substr: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="REGEXP_SUBSTR",
            expressions=[
                string_expr,
                delim_regexp,
                literal_1,
                case_occurrence,
                sqlglot_expressions.Null(),
                literal_1,
            ],
        )
        # CASE
        #     WHEN delim IS NULL OR delim = '' THEN
        #         CASE
        #             WHEN idx = 1 THEN str
        #             ELSE NULL
        #         END
        #     ELSE
        #         REGEXP_SUBSTR()
        # END
        result: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.Or(
                        this=sqlglot_expressions.Is(
                            this=delimiter_expr, expression=sqlglot_expressions.Null()
                        ),
                        expression=sqlglot_expressions.EQ(
                            this=delimiter_expr,
                            expression=sqlglot_expressions.Literal.string(""),
                        ),
                    ),
                    true=sqlglot_expressions.Case(
                        ifs=[
                            sqlglot_expressions.If(
                                this=sqlglot_expressions.EQ(
                                    this=index_expr, expression=literal_1
                                ),
                                true=string_expr,
                            )
                        ],
                        default=sqlglot_expressions.Null(),
                    ),
                )
            ],
            default=regexp_substr,
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

        date1: SQLGlotExpression = sqlglot_expressions.Cast(
            this=self.make_datetime_arg(args[1]),
            to=sqlglot_expressions.DataType(this="DATE"),
        )
        date2: SQLGlotExpression = sqlglot_expressions.Cast(
            this=self.make_datetime_arg(args[2]),
            to=sqlglot_expressions.DataType(this="DATE"),
        )

        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

        year_diff: SQLGlotExpression = sqlglot_expressions.Sub(
            this=sqlglot_expressions.Extract(
                this=sqlglot_expressions.Var(this="YEAR"), expression=date2
            ),
            expression=sqlglot_expressions.Extract(
                this=sqlglot_expressions.Var(this="YEAR"), expression=date1
            ),
        )

        match unit:
            case DateTimeUnit.YEAR:
                # EXTRACT(YEAR FROM date2) - EXTRACT(YEAR FROM date1)
                return year_diff
            case DateTimeUnit.QUARTER:
                # (EXTRACT(YEAR FROM date2) - EXTRACT(YEAR FROM date1)) * 4 +
                # (EXTRACT(QUATER FROM date2) - EXTRACT(QUARTER FROM date1))
                quarter_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Extract(
                        this=sqlglot_expressions.Var(this="QUARTER"), expression=date2
                    ),
                    expression=sqlglot_expressions.Extract(
                        this=sqlglot_expressions.Var(this="QUARTER"), expression=date1
                    ),
                )

                return sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff),
                        expression=sqlglot_expressions.Literal.number(4),
                    ),
                    expression=apply_parens(quarter_diff),
                )

            case DateTimeUnit.MONTH:
                # EXTRACT(YEAR FROM date2) - EXTRACT(YEAR FROM date1)) * 12 +
                # (EXTRACT(MONTH FROM date2) - EXTRACT(MONTH FROM date1))
                month_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Extract(
                        this=sqlglot_expressions.Var(this="MONTH"), expression=date2
                    ),
                    expression=sqlglot_expressions.Extract(
                        this=sqlglot_expressions.Var(this="MONTH"), expression=date1
                    ),
                )
                return sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff),
                        expression=sqlglot_expressions.Literal.number(12),
                    ),
                    expression=apply_parens(month_diff),
                )

            case DateTimeUnit.WEEK:
                # raw_delta = number of days between date1 and date2
                # dow1 = DAYOFWEEK(date1)
                # dow2 = DAYOFWEEK(date2)
                # result = FLOOR((raw_delta + dow1 - dow2) / 7)
                raw_delta: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=date2, expression=date1
                )

                dow1: SQLGlotExpression = self.convert_dayofweek([date1], [types[1]])
                dow2: SQLGlotExpression = self.convert_dayofweek([date2], [types[2]])

                division: SQLGlotExpression = sqlglot_expressions.Div(
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

                return sqlglot_expressions.Anonymous(
                    this="FLOOR", expressions=[division]
                )

            case DateTimeUnit.DAY:
                # date2 - date1
                return sqlglot_expressions.Sub(this=date2, expression=date1)

            case DateTimeUnit.HOUR:
                # (TRUNC(date2, 'HH24') - TRUNC(date1, 'HH24')) * 24
                hours_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.DateTrunc(
                        this=date2, unit=sqlglot_expressions.Literal.string("HH24")
                    ),
                    expression=sqlglot_expressions.DateTrunc(
                        this=date1, unit=sqlglot_expressions.Literal.string("HH24")
                    ),
                )
                return sqlglot_expressions.Mul(
                    this=apply_parens(hours_diff),
                    expression=sqlglot_expressions.Literal.number(24),
                )

            case DateTimeUnit.MINUTE:
                # (TRUNC(date2, 'MI') - TRUNC(date1, 'MI')) * 24 * 60
                minutes_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.DateTrunc(
                        this=date2, unit=sqlglot_expressions.Literal.string("MI")
                    ),
                    expression=sqlglot_expressions.DateTrunc(
                        this=date1, unit=sqlglot_expressions.Literal.string("MI")
                    ),
                )
                return sqlglot_expressions.Mul(
                    this=apply_parens(minutes_diff),
                    expression=sqlglot_expressions.Literal.number(24 * 60),
                )

            case DateTimeUnit.SECOND:
                # (date2 - date1) * 24 * 60 * 60
                dates_sub: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=date2, expression=date1
                )
                second_diff: SQLGlotExpression = sqlglot_expressions.Mul(
                    this=apply_parens(dates_sub),
                    expression=sqlglot_expressions.Literal.number(24 * 60 * 60),
                )
                return second_diff

            case _:
                raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

    def dialect_day_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        return sqlglot_expressions.ToChar(
            this=base,
            format=sqlglot_expressions.Literal.string("D"),
        )

    def days_from_start_of_week(self, base: SQLGlotExpression) -> SQLGlotExpression:
        offset: int = (-self.start_of_week_offset) % 7 - 1
        dow_expr: SQLGlotExpression = self.dialect_day_of_week(base)

        return sqlglot_expressions.Mod(
            this=apply_parens(
                sqlglot_expressions.Add(
                    this=dow_expr,
                    expression=sqlglot_expressions.Literal.number(offset),
                )
            ),
            expression=sqlglot_expressions.Literal.number(7),
        )

    def convert_current_timestamp(self) -> SQLGlotExpression:
        """
        Create a SQLGlot expression to obtain the current timestamp removing the
        timezone for Oracle.
        SQL:
            CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS DATE)
        """
        return sqlglot_expressions.Anonymous(
            this="SYS_EXTRACT_UTC",
            expressions=[
                sqlglot_expressions.Column(
                    this=sqlglot_expressions.Identifier(this="SYSTIMESTAMP")
                )
            ],
        )

    def coerce_to_timestamp(self, base: SQLGlotExpression) -> SQLGlotExpression:
        return sqlglot_expressions.Cast(
            this=base, to=sqlglot_expressions.DataType.build("DATE")
        )

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        match unit:
            case DateTimeUnit.QUARTER:
                return sqlglot_expressions.Anonymous(
                    this="TRUNC",
                    expressions=[
                        self.make_datetime_arg(base),
                        sqlglot_expressions.Literal.string("Q"),
                    ],
                )
            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE | DateTimeUnit.SECOND:
                return sqlglot_expressions.TimestampTrunc(
                    this=self.make_datetime_arg(base),
                    unit=sqlglot_expressions.Var(this=unit.value.lower()),
                )
            case DateTimeUnit.WEEK:
                return sqlglot_expressions.DateTrunc(
                    this=self.make_datetime_arg(base),
                    unit=sqlglot_expressions.Var(this="IW"),
                )
            case _:
                return sqlglot_expressions.DateTrunc(
                    this=self.make_datetime_arg(base),
                    unit=sqlglot_expressions.Var(this=unit.value.lower()),
                )

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        new_expr: SQLGlotExpression | None = None

        original_amt: int = amt
        if amt < 0:
            amt *= -1

        interval: SQLGlotExpression
        match unit:
            case (
                DateTimeUnit.HOUR
                | DateTimeUnit.MINUTE
                | DateTimeUnit.SECOND
                | DateTimeUnit.DAY
            ):
                interval = sqlglot_expressions.Anonymous(
                    this="NUMTODSINTERVAL",
                    expressions=[
                        sqlglot_expressions.convert(amt),
                        sqlglot_expressions.Literal.string(unit.value),
                    ],
                )
            case DateTimeUnit.WEEK:
                # Oracle doesn't support week intervals, so we convert weeks to
                # days by multiplying the amount by 7 and using a day interval
                interval = sqlglot_expressions.Anonymous(
                    this="NUMTODSINTERVAL",
                    expressions=[
                        sqlglot_expressions.convert(amt * 7),
                        sqlglot_expressions.Literal.string("DAY"),
                    ],
                )
            case DateTimeUnit.QUARTER:
                # Oracle doesn't support QUARTER in NUMTOYMINTERVAL.
                # Convert quarters to months (1 quarter = 3 months).
                interval = sqlglot_expressions.Anonymous(
                    this="NUMTOYMINTERVAL",
                    expressions=[
                        sqlglot_expressions.convert(amt * 3),
                        sqlglot_expressions.Literal.string("MONTH"),
                    ],
                )
            case DateTimeUnit.MONTH | DateTimeUnit.YEAR:
                add_month = (
                    original_amt if unit == DateTimeUnit.MONTH else original_amt * 12
                )
                return sqlglot_expressions.AddMonths(
                    this=base, expression=sqlglot_expressions.convert(add_month)
                )
            case _:
                raise ValueError(f"Unsupported unit '{unit}' for datetime offset.")

        if original_amt > 0:
            new_expr = sqlglot_expressions.Add(this=base, expression=interval)
        elif original_amt < 0:
            new_expr = sqlglot_expressions.Sub(this=base, expression=interval)
        else:
            new_expr = base
        return new_expr

    def convert_join_strings(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        # args[0] is the delimiter, args[1:] are the strings to join
        delim: SQLGlotExpression = args[0]
        string_args: list[SQLGlotExpression] = args[1:]

        # Build a chain of NVL2(arg, delim || arg, NULL) expressions
        concatenated: SQLGlotExpression | None = None
        for arg in string_args:
            # Create: NVL2(arg, delim || arg, NULL)
            delim_and_arg = sqlglot_expressions.DPipe(
                this=delim,
                expression=arg,
                safe=True,
            )
            nvl2_expr = sqlglot_expressions.Anonymous(
                this="NVL2",
                expressions=[arg, delim_and_arg, sqlglot_expressions.Null()],
            )

            if concatenated is None:
                concatenated = nvl2_expr
            else:
                concatenated = sqlglot_expressions.DPipe(
                    this=concatenated, expression=nvl2_expr, safe=True
                )

        # Wrap in LTRIM to remove the very first separator
        result: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="LTRIM", expressions=[concatenated, delim]
        )
        return result

    def convert_extract_datetime(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
        unit: DateTimeUnit,
    ) -> SQLGlotExpression:
        assert len(args) == 1

        cast_type: SQLGlotExpression = (
            "DATE"
            if unit not in [DateTimeUnit.HOUR, DateTimeUnit.MINUTE, DateTimeUnit.SECOND]
            else "TIMESTAMP"
        )

        return sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit.value.upper()),
            expression=sqlglot_expressions.Cast(
                this=self.make_datetime_arg(args[0]),
                to=sqlglot_expressions.DataType(this=cast_type),
            ),
        )

    def oracle_format(self, fmt: str) -> str:
        """
        Translate a Python-style `strftime` format string to Oracle.

        One-to-one conversion replacement of each key in the
        mapping with its corresponding value.  Unknown directives are left
        untouched.

        Args:
            - `fmt`: A format string containing Python `strftime` directives (e.g.
            `"%Y-%m-%d %H:%M:%S"`).

        Returns
            The resulting Oracle-compatible format (e.g. ``"YYYY-MM-DD
            HH24:MI:SS"``).

        Example:
        "%Y-%m-%d" becomes 'YYYY-MM-DD'
        "%Y/%m/%d %H:%M" becomes 'YYYY/MM/DD HH24:MI'
        """
        for k, v in self.oracle_strftime_mapping.items():
            fmt = fmt.replace(k, v)
        return fmt

    def convert_string(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        if len(args) == 1:
            # Length defaults to 4000 which is the max length of a VARCHAR2 in
            # Oracle
            return sqlglot_expressions.Cast(
                this=args[0],
                to=sqlglot_expressions.DataType(
                    this=sqlglot_expressions.Var(this="VARCHAR"),
                    expressions=[
                        sqlglot_expressions.DataTypeParam(
                            this=sqlglot_expressions.Literal.number(4000)
                        )
                    ],
                    nested=False,
                ),
            )
        else:
            assert len(args) == 2
            if (
                not isinstance(args[1], sqlglot_expressions.Literal)
                or not args[1].is_string
            ):
                raise PyDoughSQLException(
                    f"STRING(X,Y) requires the second argument to be a string date format literal, but received {args[1]}"
                )
            return sqlglot_expressions.ToChar(
                this=args[0],
                format=sqlglot_expressions.Literal.string(
                    self.oracle_format(args[1].this)
                ),
            )

    def generate_dataframe_item_dialect_expression(
        self, item: Any, item_type: PyDoughType
    ) -> SQLGlotExpression:
        match item_type:
            case DatetimeType():
                return sqlglot_expressions.Anonymous(
                    this="TO_TIMESTAMP",
                    expressions=[
                        sqlglot_expressions.Literal.string(item),
                        sqlglot_expressions.Literal.string("YYYY-MM-DD HH24:MI:SS"),
                    ],
                )
            case NumericType():
                if math.isinf(item):
                    infinity_val: SQLGlotExpression = sqlglot_expressions.Identifier(
                        this="BINARY_DOUBLE_INFINITY"
                    )
                    if item >= 0:
                        return infinity_val
                    else:
                        return sqlglot_expressions.Neg(this=infinity_val)

                return sqlglot_expressions.Literal.number(item)

            case _:  # UnknownType
                return sqlglot_expressions.Literal.string(str(item))

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        if isinstance(types[0], BooleanType):
            # Oracle can't convert boolean to int, for this Case will be used
            return sqlglot_expressions.Case(
                ifs=[
                    sqlglot_expressions.If(
                        this=args[0],
                        true=sqlglot_expressions.Literal.number(1),
                    )
                ],
                default=sqlglot_expressions.Literal.number(0),
            )
        else:
            return super().convert_integer(args, types)
