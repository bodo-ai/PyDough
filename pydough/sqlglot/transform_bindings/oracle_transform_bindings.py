"""
Definition of SQLGlot transformation bindings for the Oracle dialect.
"""

__all__ = ["OracleTransformBindings"]


import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Expression as SQLGlotExpression

import pydough.pydough_operators as pydop
from pydough.errors.error_types import PyDoughSQLException
from pydough.types import PyDoughType

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
        REGEXP_SUBSTR(
            str,
            '[^' || delim || ']+',
            1,
            CASE
                WHEN idx = 0 THEN 1
                WHEN idx > 0 THEN idx
                ELSE (REGEXP_COUNT(str, delim) + 1) + idx + 1
            END
        )
        """

        assert len(args) == 3

        string_expr, delimiter_expr, index_expr = args
        literal_0: SQLGlotExpression = sqlglot_expressions.Literal.number(0)
        literal_1: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
        regex_1: SQLGlotExpression = sqlglot_expressions.Literal.string("[^")
        regex_2: SQLGlotExpression = sqlglot_expressions.Literal.string("]+")

        case_expr: SQLGlotExpression = sqlglot_expressions.Case(
            ifs=[
                sqlglot_expressions.If(
                    this=sqlglot_expressions.EQ(this=index_expr, expression=literal_0),
                    true=literal_1,
                ),
                sqlglot_expressions.If(
                    this=sqlglot_expressions.GT(this=index_expr, expression=literal_0),
                    true=index_expr,
                ),
            ],
            default=sqlglot_expressions.Add(
                this=sqlglot_expressions.Add(
                    this=sqlglot_expressions.Paren(
                        this=sqlglot_expressions.Add(
                            this=sqlglot_expressions.Anonymous(
                                this="REGEXP_COUNT",
                                expressions=[string_expr, delimiter_expr],
                            ),
                            expression=literal_1,
                        )
                    ),
                    expression=index_expr,
                ),
                expression=literal_1,
            ),
        )

        result: SQLGlotExpression = sqlglot_expressions.Anonymous(
            this="REGEXP_SUBSTR",
            expressions=[
                string_expr,
                sqlglot_expressions.DPipe(
                    this=sqlglot_expressions.DPipe(
                        this=regex_1, expression=delimiter_expr, safe=True
                    ),
                    expression=regex_2,
                    safe=True,
                ),
                literal_1,
                case_expr,
            ],
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

                dow1: SQLGlotExpression = self.convert_dayofweek([args[1]], [types[1]])
                dow2: SQLGlotExpression = self.convert_dayofweek([args[2]], [types[2]])

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

    def apply_datetime_offset(
        self, base: SQLGlotExpression, amt: int, unit: DateTimeUnit
    ) -> SQLGlotExpression:
        new_expr: SQLGlotExpression | None = None

        if amt < 0:
            amt *= -1

        interval: SQLGlotExpression = (
            sqlglot_expressions.Anonymous(
                this="NUMTODSINTERVAL",
                expressions=[
                    sqlglot_expressions.convert(amt),
                    sqlglot_expressions.Literal.string(unit.value),
                ],
            )
            if unit not in [DateTimeUnit.YEAR, DateTimeUnit.MONTH]
            else sqlglot_expressions.Anonymous(
                this="NUMTOYMINTERVAL",
                expressions=[
                    sqlglot_expressions.convert(amt),
                    sqlglot_expressions.Literal.string(unit.value),
                ],
            )
        )
        if amt > 0:
            new_expr = sqlglot_expressions.Add(this=base, expression=interval)
        elif amt < 0:
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
            return sqlglot_expressions.ToChar(this=args[0], format=args[1])
