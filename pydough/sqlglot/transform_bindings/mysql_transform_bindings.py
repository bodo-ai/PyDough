"""
Definition of SQLGlot transformation bindings for the MySQL dialect.
"""

__all__ = ["MySQLTransformBindings"]

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.types import PyDoughType
from pydough.types.string_type import StringType

from .base_transform_bindings import BaseTransformBindings
from .sqlglot_transform_utils import (
    DateTimeUnit,
    apply_parens,
    expand_std,
    expand_variance,
)


class MySQLTransformBindings(BaseTransformBindings):
    """
    Subclass of BaseTransformBindings for the MySQL dialect.
    """

    @property
    def dialect_dow_mapping(self) -> dict[str, int]:
        """
        A mapping of each day of week string to its corresponding integer value
        in the dialect when converted to a day of week.
        """
        return {
            "Sunday": 1,
            "Monday": 2,
            "Tuesday": 3,
            "Wednesday": 4,
            "Thursday": 5,
            "Friday": 6,
            "Saturday": 7,
        }

    PYDOP_TO_MYSQL_FUNC: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.LPAD: "LPAD",
        pydop.RPAD: "RPAD",
        pydop.HOUR: "HOUR",
        pydop.MINUTE: "MINUTE",
        pydop.SECOND: "SECOND",
        pydop.DAYNAME: "DAYNAME",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
    }

    """
    Mapping of PyDough operators to equivalent MySQL function names
    These are used to generate anonymous function calls in SQLGlot
    """

    def convert_call_to_sqlglot(
        self,
        operator: pydop.PyDoughExpressionOperator,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        if operator in self.PYDOP_TO_MYSQL_FUNC:
            return sqlglot_expressions.Anonymous(
                this=self.PYDOP_TO_MYSQL_FUNC[operator], expressions=args
            )

        return super().convert_call_to_sqlglot(operator, args, types)

    def convert_slice(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        """
        Convert a PyDough slice operation to a SQLGlot expression.
        MySQL uses the SUBSTRING function for slicing.

        Outline of the logic:
        - If the start index is None, it defaults to 1 (1-based indexing).
        - If the stop index is None, it defaults to the length of the string.
        - a = start index
        - b = stop index
        match (a, b):
            case (None, None):
                return SUBSTRING(x, 1)
            case (+a, None):
                return SUBSTRING(x, a + 1)
            case (-a, None):
                return SUBSTRING(x, a)
            case (None, +b):
                return SUBSTRING(x, 1, b)
            case (None, -b):
                return SUBSTRING(x, 1, LENGTH(x) + b)
            case (+a, +b):
                return SUBSTRING(x, a + 1, GREATEST(b - a, 0))
            case (-a, -b):
                return SUBSTRING(x, a, GREATEST(b - a, 0))
            case (+a, -b):
                return SUBSTRING(x, a + 1, GREATEST(LENGTH(x) + b - a, 0))
            case (-a, +b):
                return SUBSTRING(x, a, b - GREATEST(LENGTH(x) + a, 0))
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
                WHEN LENGTH(str) = 0 THEN NULL
                WHEN LENGTH(delim) = 0 THEN
                    CASE
                        WHEN ABS(idx) = 1 THEN str
                        ELSE NULL
                    END
                WHEN idx > 0 AND idx <= (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
                    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, -1)
                WHEN idx < 0 AND -idx <= (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
                    THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, 1)
                WHEN idx = 0 THEN SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, 1), delim, -1)
                ELSE NULL
            END
        """

        assert len(args) == 3

        string_expr, delimiter_expr, index_expr = args
        literal_1: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
        literal_neg_1: SQLGlotExpression = sqlglot_expressions.Literal.number(-1)
        literal_0: SQLGlotExpression = sqlglot_expressions.Literal.number(0)

        # (LENGTH(str) - LENGTH(REPLACE(str, delim, '')))/LENGTH(delim) + 1
        difference: SQLGlotExpression = sqlglot_expressions.Sub(
            this=sqlglot_expressions.Length(this=string_expr),
            expression=sqlglot_expressions.Length(
                this=self.convert_replace([string_expr, delimiter_expr], types[:2])
            ),
        )

        index_expr = apply_parens(index_expr)

        delimiter_count: SQLGlotExpression = sqlglot_expressions.Div(
            this=apply_parens(difference),
            expression=sqlglot_expressions.Length(this=delimiter_expr),
        )
        total_parts = sqlglot_expressions.Add(
            this=delimiter_count, expression=literal_1
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, -1)
        pos_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, index_expr],
                ),
                delimiter_expr,
                literal_neg_1,
            ],
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, idx), delim, 1)
        neg_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, index_expr],
                ),
                delimiter_expr,
                literal_1,
            ],
        )

        # SUBSTRING_INDEX(SUBSTRING_INDEX(str, delim, 1), delim, -1)
        zero_index_case: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="SUBSTRING_INDEX",
            expressions=[
                sqlglot_expressions.Anonymous(
                    this="SUBSTRING_INDEX",
                    expressions=[string_expr, delimiter_expr, literal_1],
                ),
                delimiter_expr,
                literal_neg_1,
            ],
        )

        result: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=sqlglot_expressions.Length(this=string_expr),
                        expression=literal_0,
                    ),
                    true=sqlglot_expressions.Null(),
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=sqlglot_expressions.Length(this=delimiter_expr),
                        expression=literal_0,
                    ),
                    true=sqlglot_expressions.Case(
                        ifs=[
                            sqlglot_expressions.If(
                                this=sqlglot_expressions.EQ(
                                    this=sqlglot_expressions.Abs(this=index_expr),
                                    expression=literal_1,
                                ),
                                true=string_expr,
                            )
                        ],
                        default=sqlglot_expressions.Null(),
                    ),
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.And(
                        this=sqlglot_expressions.GT(
                            this=index_expr, expression=literal_0
                        ),
                        expression=sqlglot_expressions.LTE(
                            this=index_expr, expression=total_parts
                        ),
                    ),
                    true=pos_index_case,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.And(
                        this=sqlglot_expressions.LT(
                            this=index_expr, expression=literal_0
                        ),
                        expression=sqlglot_expressions.LTE(
                            this=sqlglot_expressions.Abs(this=index_expr),
                            expression=total_parts,
                        ),
                    ),
                    true=neg_index_case,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(this=index_expr, expression=literal_0),
                    true=zero_index_case,
                ),
            ],
            default=sqlglot_expressions.Null(),
        )
        return result

    def convert_variance(
        self, args: list[SQLGlotExpression], types: list[PyDoughType], type: str
    ) -> SQLGlotExpression:
        return expand_variance(args=args, types=types, type=type)

    def convert_std(
        self, args: list[SQLGlotExpression], types: list[PyDoughType], type: str
    ) -> SQLGlotExpression:
        return expand_std(args=args, types=types, type=type)

    def convert_datediff(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        Creates a SQLGlot expression for `DATEDIFF(unit, X, Y)`.

        Args:
            `args`: The operands to `DATEDIFF`, after they were
            converted to SQLGlot expressions.
            `types`: The PyDough types of the arguments to `DATEDIFF`.

        Returns:
            The SQLGlot expression matching the functionality of `DATEDIFF`.
        """
        assert len(args) == 3
        # Check if unit is a string.
        if not (isinstance(args[0], sqlglot_expressions.Literal) and args[0].is_string):
            raise ValueError(
                f"Unsupported argument for DATEDIFF: {args[0]!r}. It should be a string literal."
            )
        date1 = self.make_datetime_arg(args[1])
        date2 = self.make_datetime_arg(args[2])

        unit: DateTimeUnit | None = DateTimeUnit.from_string(args[0].this)
        if unit is None:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

        year_diff: SQLGlotExpression = sqlglot_expressions.Sub(
            this=sqlglot_expressions.Year(this=date2),
            expression=sqlglot_expressions.Year(this=date1),
        )

        match unit:
            case DateTimeUnit.YEAR:
                # YEAR(date2) - YEAR(date1)
                return year_diff
            case DateTimeUnit.QUARTER:
                # (YEAR(date2) - YEAR(date1)) * 4 + (QUARTER(date2) - QUARTER(date1))
                literal_4: SQLGlotExpression = sqlglot_expressions.Literal.number(4)

                quarter_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Quarter(this=date2),
                    expression=sqlglot_expressions.Quarter(this=date1),
                )
                return sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff), expression=literal_4
                    ),
                    expression=apply_parens(quarter_diff),
                )
            case DateTimeUnit.MONTH:
                # (YEAR(date2) - YEAR(date1)) * 12 + (MONTH(date2) - MONTH(date1))
                literal_12: SQLGlotExpression = sqlglot_expressions.Literal.number(12)

                month_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                    this=sqlglot_expressions.Month(this=date2),
                    expression=sqlglot_expressions.Month(this=date1),
                )
                return sqlglot_expressions.Add(
                    this=sqlglot_expressions.Mul(
                        this=apply_parens(year_diff), expression=literal_12
                    ),
                    expression=apply_parens(month_diff),
                )
            case DateTimeUnit.WEEK:
                # raw_delta = number of days between date1 and date2
                # dow1 = DAYOFWEEK(date1)
                # dow2 = DAYOFWEEK(date2)
                # result = INTEGER((raw_delta + dow1 - dow2) / 7)
                raw_delta: SQLGlotExpression = sqlglot_expressions.DateDiff(
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

                return sqlglot_expressions.Cast(
                    this=division, to=sqlglot_expressions.DataType.build("BIGINT")
                )

            case DateTimeUnit.DAY:
                # DATEDIFF(DATE(date2), DATE(date1))
                return sqlglot_expressions.DateDiff(
                    this=sqlglot_expressions.DateTrunc(
                        this=date2, unit=sqlglot_expressions.Var(this="DAY")
                    ),
                    expression=sqlglot_expressions.DateTrunc(
                        this=date1, unit=sqlglot_expressions.Var(this="DAY")
                    ),
                )

            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE:
                # HOUR/MINUTE
                # TIMESTAMPDIFF(HOUR, DATE_FORMAT(date1, '%Y-%m-%d %H:{00/%i}:00'), DATE_FORMAT(date2, '%Y-%m-%d %H:{00/%i}:00'))
                # Truncate to the corresponding unit, then take the difference
                minute_str: str = "00" if unit == DateTimeUnit.HOUR else "%i"
                trunc_string: str = f"%Y-%m-%d %H:{minute_str}:00"
                fmt: SQLGlotExpression = sqlglot_expressions.Literal.string(
                    trunc_string
                )

                return sqlglot_expressions.TimestampDiff(
                    unit=sqlglot_expressions.Var(this=unit.value),
                    this=sqlglot_expressions.Anonymous(
                        this="DATE_FORMAT", expressions=[date2, fmt]
                    ),
                    expression=sqlglot_expressions.Anonymous(
                        this="DATE_FORMAT", expressions=[date1, fmt]
                    ),
                )

            case DateTimeUnit.SECOND:
                # TIMESTAMPDIFF(SECOND, date1, date2)
                return sqlglot_expressions.TimestampDiff(
                    unit=sqlglot_expressions.Var(this=unit.value),
                    this=date2,
                    expression=date1,
                )

            case _:
                raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")

    def apply_datetime_truncation(
        self, base: SQLGlotExpression, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        if unit == DateTimeUnit.WEEK:
            # DOW = DAYOFWEEK(X)
            # Y = subtract DOW days from X
            # RESULT = DATETIME(Y, "start of day")
            dow = self.days_from_start_of_week(base)
            minus_dow: SQLGlotExpression = sqlglot_expressions.DateSub(
                this=base,
                expression=dow,
                unit=sqlglot_expressions.Var(this=DateTimeUnit.DAY),
            )
            return self.apply_datetime_truncation(minus_dow, DateTimeUnit.DAY)
        elif unit == DateTimeUnit.DAY:
            return sqlglot_expressions.Cast(
                this=base, to=sqlglot_expressions.DataType.build("DATE")
            )

        else:
            return super().apply_datetime_truncation(base, unit)

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

    def convert_strip(
        self,
        args: list[SQLGlotExpression],
        types: list[PyDoughType],
    ) -> SQLGlotExpression:
        """
        CASE replacement_expr == args[0] THEN ""
        DEFAULT
            REGEXP_REPLACE(CAST(args[0] AS CHAR) COLLATE utf8mb4_bin, regex_replace, '')
        REGEXP_REPLACE(
            CAST(args[0] AS CHAR) COLLATE utf8mb4_bin,
            '^[' || regexp_replace(
                replacement_expr, '([][\\^\\-])', '\\\1') ||
                ']+|[' ||
                regexp_replace(replacement_expr, '([][\\^\\-])', '\\\1') ||
                ']+$',
            ''
        )
        """
        assert 1 <= len(args) <= 2

        replacement_expr: SQLGlotExpression = (
            sqlglot_expressions.Literal.string("\\s") if len(args) == 1 else args[1]
        )

        #  '^[' || REGEXP_REPLACE(replacement_expr, '([][\\^\\-])', '\\\1') || ']+|['
        dpipe2: SQLGlotExpression = sqlglot_expressions.DPipe(
            this=sqlglot_expressions.DPipe(
                this=sqlglot_expressions.Literal.string("^["),
                expression=sqlglot_expressions.RegexpReplace(
                    this=replacement_expr,
                    expression=sqlglot_expressions.Literal.string("([]\\[\\^\\-])"),
                    replacement=sqlglot_expressions.Literal.string("\\\\\\1"),
                ),
            ),
            expression=sqlglot_expressions.Literal.string("]+|["),
            safe=True,
        )

        # dpipe2 || REGEXP_REPLACE(replacement_expr, '([][\\^\\-])', '\\\1'
        dpipe1: SQLGlotExpression = sqlglot_expressions.DPipe(
            this=dpipe2,
            expression=sqlglot_expressions.RegexpReplace(
                this=replacement_expr,
                expression=sqlglot_expressions.Literal.string("([]\\[\\^\\-])"),
                replacement=sqlglot_expressions.Literal.string("\\\\\\1"),
            ),
            safe=True,
        )
        # dpipe1 || ']+$'
        regex_replace: SQLGlotExpression = sqlglot_expressions.DPipe(
            this=dpipe1,
            expression=sqlglot_expressions.Literal.string("]+$"),
            safe=True,
        )

        utf8mb4_bin: SQLGlotExpression = sqlglot_expressions.Identifier(
            this="utf8mb4_bin"
        )

        # CASE replacement_expr == args[0] THEN ""
        # DEFAULT
        #   REGEXP_REPLACE(CAST(args[0] AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_bin, regex_replace, '')

        result: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(
                        this=args[0],
                        expression=replacement_expr,
                    ),
                    true=sqlglot_expressions.Literal.string(""),
                )
            ],
            default=sqlglot_expressions.RegexpReplace(
                this=sqlglot_expressions.Collate(
                    this=sqlglot_expressions.Cast(
                        this=args[0], to=sqlglot_expressions.DataType.build("CHAR")
                    ),
                    expression=utf8mb4_bin,
                ),
                expression=regex_replace,
                replacement=sqlglot_expressions.Literal.string(""),
            ),
        )
        return result

    def convert_integer(
        self, args: list[SQLGlotExpression], types: list[PyDoughType]
    ) -> SQLGlotExpression:
        return sqlglot_expressions.Anonymous(
            this="Truncate",
            expressions=[
                sqlglot_expressions.Cast(
                    this=args[0],
                    to=sqlglot_expressions.DataType.build("REAL"),
                ),
                sqlglot_expressions.convert(0),
            ],
        )

    def convert_ordering(
        self, arg: SQLGlotExpression, data_type: PyDoughType
    ) -> SQLGlotExpression:
        if isinstance(data_type, StringType):
            return sqlglot_expressions.Collate(
                this=arg,
                expression=sqlglot_expressions.Identifier(this="utf8mb4_bin"),
            )
        return arg
