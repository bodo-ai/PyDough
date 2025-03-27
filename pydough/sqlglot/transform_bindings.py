"""
Definition of binding infrastructure that maps PyDough operators to
implementations of how to convert them to SQLGlot expressions
"""

__all__ = ["SqlGlotTransformBindings"]
import re
import sqlite3
from collections.abc import Callable, Sequence
from enum import Enum
from typing import Union

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Binary, Case, Concat, Is, Paren, Unary
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Func as SQLGlotFunction

import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors.database_connector import DatabaseDialect
from pydough.relational.relational_expressions import RelationalExpression

operator = pydop.PyDoughOperator
transform_binding = Callable[
    [
        Sequence[RelationalExpression] | None,
        Sequence[SQLGlotExpression],
        PyDoughConfigs,
    ],
    SQLGlotExpression,
]

PAREN_EXPRESSIONS = (Binary, Unary, Concat, Is, Case)
"""
The types of SQLGlot expressions that need to be wrapped in parenthesis for the
sake of precedence.
"""


current_ts_pattern: re.Pattern = re.compile(
    r"\s*((now)|(current[ _]?timestamp)|(current[ _]?date))\s*", re.IGNORECASE
)
"""
The REGEX pattern used to detect a valid alias of a string requesting the current
timestamp.
"""

trunc_pattern = re.compile(r"\s*start\s+of\s+(\w+)\s*", re.IGNORECASE)
"""
The REGEX pattern for truncation modifiers in DATETIME call.
"""

offset_pattern = re.compile(r"\s*([+-]?)\s*(\d+)\s+(\w+)\s*", re.IGNORECASE)
"""
The REGEX pattern for offset modifiers in DATETIME call.
"""

year_units = ("years", "year", "y")
"""
The valid string representations of the year unit.
"""

month_units = ("months", "month", "mm")
"""
The valid string representations of the month unit.
"""

week_units = ("weeks", "week", "w")
"""
The valid string representations of the week unit.
"""

day_units = ("days", "day", "d")
"""
The valid string representations of the day unit.
"""

hour_units = ("hours", "hour", "h")
"""
The valid string representations of the hour unit.
"""

minute_units = ("minutes", "minute", "m")
"""
The valid string representations of the minute unit.
"""

second_units = ("seconds", "second", "s")
"""
The valid string representations of the second unit.
"""


class DateTimeUnit(Enum):
    """
    Enum representing the valid date/time units that can be used in PyDough.
    """

    YEAR = "year"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"
    HOUR = "hour"
    MINUTE = "minute"
    SECOND = "second"

    @staticmethod
    def from_string(unit: str) -> Union["DateTimeUnit", None]:
        """
        Converts a string literal representing a date/time unit into a
        DateTimeUnit enum value.
        canonical form if it is recognized as one of the valid date/time unit
        aliases (case-insensitive).

        Args:
            `unit`: The string literal representing the date/time unit.

        Returns:
            The enum form of the date/time unit, or `None` if the unit is
            not one of the recognized date/time unit aliases.
        """
        unit = unit.lower()
        if unit in year_units:
            return DateTimeUnit.YEAR
        elif unit in month_units:
            return DateTimeUnit.MONTH
        elif unit in week_units:
            return DateTimeUnit.WEEK
        elif unit in day_units:
            return DateTimeUnit.DAY
        elif unit in hour_units:
            return DateTimeUnit.HOUR
        elif unit in minute_units:
            return DateTimeUnit.MINUTE
        elif unit in second_units:
            return DateTimeUnit.SECOND
        else:
            return None

    @property
    def truncation_string(self) -> str:
        """
        The format string that can be used to truncate to the specified unit.
        """
        match self:
            case DateTimeUnit.YEAR:
                return "'%Y-01-01 00:00:00'"
            case DateTimeUnit.MONTH:
                return "'%Y-%m-01 00:00:00'"
            case DateTimeUnit.DAY:
                return "'%Y-%m-%d 00:00:00'"
            case DateTimeUnit.HOUR:
                return "'%Y-%m-%d %H:00:00'"
            case DateTimeUnit.MINUTE:
                return "'%Y-%m-%d %H:%M:00'"
            case DateTimeUnit.SECOND:
                return "'%Y-%m-%d %H:%M:%S'"
            case _:
                raise ValueError(
                    f"Unsupported date/time unit for truncation_string: {self}"
                )


def apply_parens(expression: SQLGlotExpression) -> SQLGlotExpression:
    """
    Determine when due to the next SQL operator not using standard
    function syntax, we may need to apply parentheses to the current
    expression to avoid operator precedence issues.

    Args:
        `expression`: The expression to check and potentially wrap in
        parentheses.

    Returns:
        The expression, wrapped in parentheses if necessary.
    """
    if isinstance(expression, PAREN_EXPRESSIONS):
        return Paren(this=expression)
    else:
        return expression


def convert_sqlite_datetime_extract(format_str: str) -> transform_binding:
    """
    Generate a SQLite-compatible datetime conversion expression for the given
    format string. This is used when a dialect does not support extraction
    functions, so `YEAR(x)` becomes `STRFTIME('%Y', x) :: INT`, etc.

    Args:
        `format_str`: The format string corresponding that should be used
        in the `STRFTIME` call to extract a portion of the date/time of the
        operands.

    Returns:
        A new transform binding that corresponds to an extraction operation
        using the specified `format_str` to do so.
    """

    def impl(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ) -> SQLGlotExpression:
        dt_expr: SQLGlotExpression = make_datetime_arg(
            sql_glot_args[0], DatabaseDialect.SQLITE
        )
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.TimeToStr(this=dt_expr, format=format_str),
            to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
        )

    return impl


def convert_sqlite_days_from_start_of_week(
    base: SQLGlotExpression, config: PyDoughConfigs
) -> SQLGlotExpression:
    """
    Calculates the number of days between a date and the start of its week
    in SQLite.

    For example, if Monday is configured as the start of the week:
    - For a Monday date, returns 0
    - For a Tuesday date, returns 1
    - For a Sunday date, returns 6

    The calculation uses SQLite's STRFTIME('%w', date) which returns 0-6 for
    Sunday-Saturday. An offset is applied to shift the result based on the
    configured start of week.

    The formula is: ((STRFTIME('%w', date) + offset) % 7)

    Note: This assumes SQLite's STRFTIME follows POSIX convention where:
    - Sunday = 0
    - Monday = 1
    - Saturday = 6

    Args:
        `base`: The base date/time expression to calculate the start of the week
        from.
        `config`: The PyDough configuration to use to determine the start of the
        week.

    Returns:
        The SQLGlot expression to calculating the number of days from `base` to
        the start of the week. This number is always positive.
    """
    start_of_week: DayOfWeek = config.start_of_week
    offset: int = 0
    match start_of_week:
        case DayOfWeek.SUNDAY:
            return convert_sqlite_datetime_extract("'%w'")(None, [base], config)
        case DayOfWeek.MONDAY:
            offset = 6
        case DayOfWeek.TUESDAY:
            offset = 5
        case DayOfWeek.WEDNESDAY:
            offset = 4
        case DayOfWeek.THURSDAY:
            offset = 3
        case DayOfWeek.FRIDAY:
            offset = 2
        case DayOfWeek.SATURDAY:
            offset = 1
        case _:
            raise ValueError(f"Unsupported start of week: {start_of_week}")
    answer: SQLGlotExpression = sqlglot_expressions.Mod(
        this=apply_parens(
            sqlglot_expressions.Add(
                this=convert_sqlite_datetime_extract("'%w'")(None, [base], config),
                expression=sqlglot_expressions.Literal.number(offset),
            )
        ),
        expression=sqlglot_expressions.Literal.number(7),
    )
    return answer


def convert_days_from_start_of_week(
    base: SQLGlotExpression, config: PyDoughConfigs
) -> SQLGlotExpression:
    """
    Calculates the number of days between a given date and the start of its week.

    The start of week is configured via `config.start_of_week`. For example, if
    start of week is Monday and the date is Wednesday, this returns 2.

    The calculation uses the formula: (weekday + offset) % 7

    This assumes the underlying database follows POSIX conventions where:
    - Sunday is day 0
    - Days increment sequentially (Mon=1, Tue=2, etc.)

    Args:
        `base`: The base date/time expression to calculate the start of the week
        from.
        `config`: The PyDough configuration to use to determine the start of the
        week.

    Returns:
        The SQLGlot expression to calculating the number of days from `base` to
        the start of the week. This number is always positive.
    """
    start_of_week: DayOfWeek = config.start_of_week
    offset: int = 0
    match start_of_week:
        case DayOfWeek.SUNDAY:
            return sqlglot_expressions.Cast(
                this=sqlglot_expressions.DayOfWeek(this=base),
                to=sqlglot_expressions.DataType(
                    this=sqlglot_expressions.DataType.Type.INT
                ),
            )
        case DayOfWeek.MONDAY:
            offset = 6
        case DayOfWeek.TUESDAY:
            offset = 5
        case DayOfWeek.WEDNESDAY:
            offset = 4
        case DayOfWeek.THURSDAY:
            offset = 3
        case DayOfWeek.FRIDAY:
            offset = 2
        case DayOfWeek.SATURDAY:
            offset = 1
        case _:
            raise ValueError(f"Unsupported start of week: {start_of_week}")
    answer: SQLGlotExpression = sqlglot_expressions.Mod(
        this=apply_parens(
            sqlglot_expressions.Add(
                this=sqlglot_expressions.DayOfWeek(this=base),
                expression=sqlglot_expressions.Literal.number(offset),
            )
        ),
        expression=sqlglot_expressions.Literal.number(7),
    )
    return answer


def apply_datetime_truncation(
    base: SQLGlotExpression,
    unit: DateTimeUnit,
    dialect: DatabaseDialect,
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Applies a truncation operation to a date/time expression by a certain unit.

    Args:
        `base`: The base date/time expression to truncate.
        `unit`: The unit to truncate the date/time expression to.
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        The SQLGlot expression to truncate `base`.
    """
    if dialect == DatabaseDialect.SQLITE:
        match unit:
            # For y/m/d, use the `start of` modifier in SQLite.
            case DateTimeUnit.YEAR | DateTimeUnit.MONTH | DateTimeUnit.DAY:
                trunc_expr: SQLGlotExpression = sqlglot_expressions.convert(
                    f"start of {unit.value}"
                )
                if isinstance(base, sqlglot_expressions.Date):
                    base.this.append(trunc_expr)
                    return base
                if (
                    isinstance(base, sqlglot_expressions.Datetime)
                    and len(base.this) == 1
                ):
                    return sqlglot_expressions.Date(
                        this=base.this + [trunc_expr],
                    )
                return sqlglot_expressions.Date(
                    this=[base, trunc_expr],
                )
            # SQLite does not have `start of` modifiers for hours, minutes, or
            # seconds, so we use `strftime` to truncate to the unit.
            case DateTimeUnit.HOUR | DateTimeUnit.MINUTE | DateTimeUnit.SECOND:
                return sqlglot_expressions.TimeToStr(
                    this=base,
                    format=unit.truncation_string,
                )
            case DateTimeUnit.WEEK:
                # Implementation for week.
                # Assumption: By default start of week is Sunday
                # Week starts at 0
                # Sunday = 0, Monday = 1, ..., Saturday = 6
                str1_glot: SQLGlotExpression = sqlglot_expressions.Literal.string("-")
                shifted_weekday: SQLGlotExpression = sqlglot_expressions.Cast(
                    this=convert_sqlite_days_from_start_of_week(base, config),
                    to=sqlglot_expressions.DataType(
                        this=sqlglot_expressions.DataType.Type.TEXT
                    ),
                )
                # string glot for ' days'
                str3_glot: SQLGlotExpression = sqlglot_expressions.Literal.string(
                    " days"
                )
                # This expression is equivalent to  "- ((STRFTIME('%w', base) + offset) % 7) days"
                offset_expr: SQLGlotExpression = convert_concat(
                    None, [str1_glot, shifted_weekday, str3_glot], config
                )
                start_of_day_expr: SQLGlotExpression = (
                    sqlglot_expressions.Literal.string("start of day")
                )
                # Apply the "- <offset> days", then truncate to the start of the day
                if isinstance(base, sqlglot_expressions.Date):
                    base.this.extend([offset_expr, start_of_day_expr])
                    return base
                if (
                    isinstance(base, sqlglot_expressions.Datetime)
                    and len(base.this) == 1
                ):
                    return sqlglot_expressions.Date(
                        this=base.this + [offset_expr, start_of_day_expr],
                    )
                return sqlglot_expressions.Date(
                    this=[base, offset_expr, start_of_day_expr],
                )
    else:
        # For other dialects, we can rely the DATE_TRUNC function.
        return sqlglot_expressions.DateTrunc(
            this=base,
            unit=sqlglot_expressions.Var(this=unit.value),
        )


def apply_datetime_offset(
    base: SQLGlotExpression, amt: int, unit: DateTimeUnit, dialect: DatabaseDialect
) -> SQLGlotExpression:
    """
    Adds/subtracts a datetime interval to to a date/time expression.

    Args:
        `base`: The base date/time expression to add/subtract from.
        `amt`: The amount of the unit to add (if positive) or subtract
        (if negative).
        `unit`: The unit of the interval to add/subtract.
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        The SQLGlot expression to add/subtract the specified interval to/from
        `base`.
    """
    if dialect == DatabaseDialect.SQLITE:
        # For sqlite, use the DATETIME operator to add the interval
        # Convert weeks to days by multiplying by 7
        if unit == DateTimeUnit.WEEK:
            amt *= 7
            unit = DateTimeUnit.DAY
        offset_expr: SQLGlotExpression = sqlglot_expressions.convert(
            f"{amt} {unit.value}"
        )
        if unit in (DateTimeUnit.YEAR, DateTimeUnit.MONTH, DateTimeUnit.DAY):
            if isinstance(base, sqlglot_expressions.Date):
                base.this.append(offset_expr)
                return base
            if isinstance(base, sqlglot_expressions.Datetime):
                return sqlglot_expressions.Datetime(this=[base], expression=offset_expr)
        else:
            assert unit in (
                DateTimeUnit.HOUR,
                DateTimeUnit.MINUTE,
                DateTimeUnit.SECOND,
            )
            if isinstance(base, sqlglot_expressions.Datetime):
                return sqlglot_expressions.Datetime(this=[base], expression=offset_expr)
        return sqlglot_expressions.Datetime(this=[base], expression=offset_expr)
    else:
        # For other dialects, we can rely the DATEADD function.
        return sqlglot_expressions.DateAdd(
            this=base,
            expression=sqlglot_expressions.convert(amt),
            unit=sqlglot_expressions.Var(this=unit.value),
        )


def handle_datetime_base_arg(
    arg: SQLGlotExpression, dialect: DatabaseDialect
) -> SQLGlotExpression:
    """
    Handle the first argument to the DATETIME function, which can be a datetime
    column or a string indicating to fetch the current timestamp.

    Args:
        `arg`: The first argument to the DATETIME function.
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        The SQLGlot expression corresponding to the first argument of the
        DATETIME function.
    """
    # If the argument is a string literal, check if it is one of the special
    # values (ignoring case & leading/trailing spaces) indicating the current
    # datetime should be used.
    if isinstance(arg, sqlglot_expressions.Literal) and arg.is_string:
        if current_ts_pattern.fullmatch(arg.this):
            if dialect == DatabaseDialect.SQLITE:
                return sqlglot_expressions.Datetime(
                    this=[sqlglot_expressions.convert("now")]
                )
            else:
                return sqlglot_expressions.CurrentTimestamp()
    if dialect != DatabaseDialect.SQLITE:
        return sqlglot_expressions.Cast(
            this=arg, to=sqlglot_expressions.DataType.build("TIMESTAMP")
        )
    return sqlglot_expressions.Datetime(this=[arg])


def convert_datetime(dialect: DatabaseDialect) -> transform_binding:
    """
    Converts a call to the `DATETIME` function to a SQLGlot expression.

    Args:
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        A new transform binding that corresponds to a DATETIME function call.
    """

    def impl(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ):
        # Handle the first argument
        assert len(sql_glot_args) > 0
        result: SQLGlotExpression = handle_datetime_base_arg(sql_glot_args[0], dialect)

        # Accumulate the answer by using each modifier argument to build up
        # result via a sequence of truncation and offset operations.
        for i in range(1, len(sql_glot_args)):
            arg: SQLGlotExpression = sql_glot_args[i]
            if not (isinstance(arg, sqlglot_expressions.Literal) and arg.is_string):
                raise NotImplementedError(
                    f"DATETIME function currently requires all arguments after the first argument to be string literals, but received {arg.sql()!r}"
                )
            unit: DateTimeUnit | None
            trunc_match: re.Match | None = trunc_pattern.fullmatch(arg.this)
            offset_match: re.Match | None = offset_pattern.fullmatch(arg.this)
            if trunc_match is not None:
                # If the string is in the form `start of <unit>`, apply
                # truncation.
                unit = DateTimeUnit.from_string(str(trunc_match.group(1)))
                if unit is None:
                    raise ValueError(
                        f"Unsupported DATETIME modifier string: {arg.this!r}"
                    )
                result = apply_datetime_truncation(result, unit, dialect, config)
            elif offset_match is not None:
                # If the string is in the form `±<amt> <unit>`, apply an
                # offset.
                amt = int(offset_match.group(2))
                if str(offset_match.group(1)) == "-":
                    amt *= -1
                unit = DateTimeUnit.from_string(str(offset_match.group(3)))
                if unit is None:
                    raise ValueError(
                        f"Unsupported DATETIME modifier string: {arg.this!r}"
                    )
                result = apply_datetime_offset(result, amt, unit, dialect)
            else:
                raise ValueError(f"Unsupported DATETIME modifier string: {arg.this!r}")
        return result

    return impl


def convert_dayofweek(dialect: DatabaseDialect) -> transform_binding:
    """
    Converts a call to the `DAYOFWEEK` function to a SQLGlot expression.
    This function creates the expression for retrieving the day of the week
    from a date/time expression, taking into account the start of the week.
    Start of week is defined by the `config.start_of_week` setting.
    The day of the week is returned as an integer between 0 and 6, or between
    1 and 7 if the week starts at 1. This is handled by the
    `config.start_week_as_zero` setting.
    For example, if the start of the week is Monday, then the day of the week
    for March 18, 2025 is 1 because March 17, 2025 is a Monday.

    Args:
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        A new transform binding that corresponds to a DAYOFWEEK function call.
    """

    def impl(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ):
        assert len(sql_glot_args) == 1
        if dialect == DatabaseDialect.SQLITE:
            # By default start of week is Sunday
            # Week starts at 0
            # Sunday = 0, Monday = 1, ..., Saturday = 6
            base = sql_glot_args[0]
            start_week_as_zero = config.start_week_as_zero
            # Expression for ((STRFTIME('%w', base) + offset) % 7)
            sqlite_shifted_weekday: SQLGlotExpression = (
                convert_sqlite_days_from_start_of_week(base, config)
            )
            # If the week does not start at zero, we need to add 1 to the result
            if not start_week_as_zero:
                sqlite_shifted_weekday = sqlglot_expressions.Add(
                    this=apply_parens(sqlite_shifted_weekday),
                    expression=sqlglot_expressions.Literal.number(1),
                )
            sqlite_answer: SQLGlotExpression = apply_parens(sqlite_shifted_weekday)
            return sqlite_answer
        else:
            # ANSI implementation
            # By default start of week is Sunday
            # Week starts at 0
            # Sunday = 0, Monday = 1, ..., Saturday = 6
            base = sql_glot_args[0]
            start_week_as_zero = config.start_week_as_zero
            # Expression for ((STRFTIME('%w', base) + offset) % 7)
            ansi_shifted_weekday: SQLGlotExpression = convert_days_from_start_of_week(
                base, config
            )
            # If the week does not start at zero, we need to add 1 to the result
            if not start_week_as_zero:
                ansi_shifted_weekday = sqlglot_expressions.Add(
                    this=apply_parens(ansi_shifted_weekday),
                    expression=sqlglot_expressions.Literal.number(1),
                )
            ansi_answer: SQLGlotExpression = apply_parens(ansi_shifted_weekday)
            return ansi_answer

    return impl


def convert_dayname(dialect: DatabaseDialect) -> transform_binding:
    """
    Converts a call to the `DAYNAME` function to a SQLGlot expression.

    Args:
        `dialect`: The dialect being used to generate the SQL.

    Returns:
        A new transform binding that corresponds to a DAYNAME function call.
    """

    def impl(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ):
        assert len(sql_glot_args) == 1
        base = sql_glot_args[0]
        if dialect == DatabaseDialect.SQLITE:
            # By default start of week is Sunday
            # Week starts at 0
            # Sunday = 0, Monday = 1, ..., Saturday = 6
            sqlite_base_week_day: SQLGlotExpression = convert_sqlite_datetime_extract(
                "'%w'"
            )(None, [base], config)
            sqlite_answer: SQLGlotExpression = sqlglot_expressions.Case()
            for dow, dayname in enumerate(
                [
                    "Sunday",
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                ]
            ):
                sqlite_answer = sqlite_answer.when(
                    sqlglot_expressions.EQ(
                        this=sqlite_base_week_day,
                        expression=sqlglot_expressions.Literal.number(dow),
                    ),
                    sqlglot_expressions.Literal.string(dayname),
                )
            sqlite_answer = apply_parens(sqlite_answer)
            return sqlite_answer
        else:
            # ANSI implementation
            # Assumption: start of week is Sunday
            # Week starts at 0
            # Sunday = 0, Monday = 1, ..., Saturday = 6
            ansi_base_week_day: SQLGlotExpression = sqlglot_expressions.DayOfWeek(
                this=base
            )
            ansi_answer: SQLGlotExpression = sqlglot_expressions.Case()
            for dow, dayname in enumerate(
                [
                    "Sunday",
                    "Monday",
                    "Tuesday",
                    "Wednesday",
                    "Thursday",
                    "Friday",
                    "Saturday",
                ]
            ):
                ansi_answer = ansi_answer.when(
                    sqlglot_expressions.EQ(
                        this=ansi_base_week_day,
                        expression=sqlglot_expressions.Literal.number(dow),
                    ),
                    sqlglot_expressions.Literal.string(dayname),
                )
            ansi_answer = apply_parens(ansi_answer)
            return ansi_answer

    return impl


def convert_iff_case(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for converting the expression `IFF(a, b, c)` to the expression
    `CASE WHEN a THEN b ELSE c END`, since not every dialect supports IFF.

    Args:
        `raw_args`: The operands to `IFF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `IFF`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        A `CASE` expression equivalent to the input `IFF` call.
    """
    assert len(sql_glot_args) == 3
    return (
        sqlglot_expressions.Case()
        .when(sql_glot_args[0], sql_glot_args[1])
        .else_(sql_glot_args[2])
    )


def convert_absent(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for converting the expression `ABSENT(X)` to the expression
    `X IS NULL`

    Args:
        `raw_args`: The operands to `ABSENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `ABSENT`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The `IS NULL` call corresponding to the `ABSENT` call.
    """
    return sqlglot_expressions.Is(
        this=apply_parens(sql_glot_args[0]), expression=sqlglot_expressions.Null()
    )


def convert_present(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for converting the expression `PRESENT(X)` to the expression
    `X IS NOT NULL`

    Args:
        `raw_args`: The operands to `PRESENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `PRESENT`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The `IS NOT NULL` call corresponding to the `PRESENT` call.
    """
    return sqlglot_expressions.Not(
        this=apply_parens(convert_absent(raw_args, sql_glot_args, config))
    )


def convert_keep_if(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for converting the expression `KEEP_IF(X, Y)` to the expression
    `CASE IF Y THEN X END`.
    Args:
        `raw_args`: The operands to `KEEP_IF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `KEEP_IF`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot case expression equivalent to the `KEEP_IF` call.
    """
    return convert_iff_case(
        None,
        [sql_glot_args[1], sql_glot_args[0], sqlglot_expressions.Null()],
        config,
    )


def convert_monotonic(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for converting the expression `MONOTONIC(A, B, C, ...)` to an
    expression equivalent of `(A <= B) AND (B <= C) AND ...`.

    Args:
        `raw_args`: The operands to `MONOTONIC`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `MONOTONIC`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression equivalent to the `MONOTONIC` call.
    """

    if len(sql_glot_args) < 2:
        return sqlglot_expressions.convert(True)

    exprs: list[SQLGlotExpression] = [apply_parens(expr) for expr in sql_glot_args]
    output_expr: SQLGlotExpression = apply_parens(
        sqlglot_expressions.LTE(this=exprs[0], expression=exprs[1])
    )
    for i in range(2, len(exprs)):
        new_expr: SQLGlotExpression = apply_parens(
            sqlglot_expressions.LTE(this=exprs[i - 1], expression=exprs[i])
        )
        output_expr = sqlglot_expressions.And(this=output_expr, expression=new_expr)
    return output_expr


def convert_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for generating a `CONCAT` expression from a list of arguments.
    This is optimized for the case where all arguments are string literals
    because it impacts the quality of the generated SQL for common cases.

    Args:
        `raw_args`: The operands to `CONCAT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        A `CONCAT` expression, or equivalent string literal.
    """
    # Fast path for all arguments as string literals.
    if all(
        isinstance(arg, sqlglot_expressions.Literal) and arg.is_string
        for arg in sql_glot_args
    ):
        return sqlglot_expressions.convert("".join(arg.this for arg in sql_glot_args))
    else:
        inputs: list[SQLGlotExpression] = [apply_parens(arg) for arg in sql_glot_args]
        return Concat(expressions=inputs)


def positive_index(
    string_expr: SQLGlotExpression, neg_index: int, is_zero_based: bool = False
) -> SQLGlotExpression:
    """
    Gives the SQL Glot expression for converting a
    negative index to a positive index in 1 or 0 based indexing
    based on the length of the column.

    Args:
        `string_expr`: The expression to reference
        `neg_index`: The negative index in 0 based index to convert to positive
        `is_zero_based`: Whether the return index is 0-based or 1-based

    Returns:
        SQLGlot expression corresponding to:
        `(LENGTH(string_expr) + neg_index + offset)`,
         where offset is 0, if is_zero_based is True, else 1.
    """
    sql_len = sqlglot_expressions.Length(this=string_expr)
    offset = 0 if is_zero_based else 1
    return apply_parens(
        sqlglot_expressions.Add(
            this=sql_len, expression=sqlglot_expressions.convert(neg_index + offset)
        )
    )


def convert_slice(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for generating a `SLICE` expression from a list of arguments.
    It is expected that len(sql_glot_args) == 4.
    The first argument is the string to slice.
    The second argument is the `start` index.
    The third argument is the `stop` index.
    The fourth argument is the `step`.
    Outline of the logic:
    - Case 1: `(None, None)`
        - Returns the string as is.
    - Case 2: `(start, None)`
        - Positive `start`: Convert to 1-based indexing and slice from `start`.
        - Negative `start`: Compute `LENGTH(string) + start + 1`; clamp to `1`
         if less than `1`.
    - Case 3: `(None, stop)`
        - Positive `stop`: Slice from position `1` to `stop`.
        - Negative `stop`: Compute `LENGTH(string) + stop`; clamp to `0` if
         less than `0` (empty slice).
    - Case 4: `(start, stop)`
        - 1. Both `start` & `stop` >= 0:
            - Convert `start` to 1-based.
            - Set `length = stop - start`.
        - 2. `start < 0`, `stop >= 0`:
            - Convert `start` to 1 based index. If < 1, set to 1.
            - Compute `length = stop - start` (clamp to 0 if negative).
        - 3. `start >= 0`, `stop < 0`:
            - Convert `stop` & `start` to 1 based index.
            - If `stop` < 1, slice is empty (`length = 0`).
            - Else, `length = stop - start`.
        - 4. `start < 0`, `stop < 0`:
            - Convert `start` & `stop` to 1 based index. If `start` < 1, set to 1.
            - If `stop` < 1, slice is empty (`length = 0`).
            - Else, `length = stop - start`.

    Args:
        `raw_args`: The operands to `SLICE`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `SLICE`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.
    Returns:
        The SQLGlot expression matching the functionality of Python based string slicing
        with the caveat that it only supports a step of 1.
    """
    assert len(sql_glot_args) == 4
    string_expr, start, stop, step = sql_glot_args

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
    sql_zero = sqlglot_expressions.convert(0)
    sql_one = sqlglot_expressions.convert(1)
    sql_empty_str = sqlglot_expressions.convert("")

    match (start_idx, stop_idx):
        case (None, None):
            raise string_expr
        case (_, None):
            assert start_idx is not None
            if start_idx > 0:
                return sqlglot_expressions.Substring(
                    this=string_expr,
                    start=sqlglot_expressions.convert(start_idx + 1),
                )
            else:
                # Calculate the positive index equivalent for the negative index
                # e.g., for string "hello" and index -2, converts to index 4 (LENGTH("hello") + (-2) + 1)
                start_idx_glot = positive_index(string_expr, start_idx)

                # Create a SUBSTRING expression with adjusted start position
                answer = sqlglot_expressions.Substring(
                    this=string_expr,  # The original string to slice
                    start=convert_iff_case(
                        None,
                        [
                            # Check if the calculated positive index is less than 1
                            sqlglot_expressions.LT(
                                this=start_idx_glot, expression=sql_one
                            ),
                            sql_one,  # If true, use index 1 (start from beginning)
                            start_idx_glot,  # If false, use the calculated positive index
                        ],
                        config,
                    ),
                )
                return answer
        case (None, _):
            assert stop_idx is not None
            if stop_idx > 0:
                return sqlglot_expressions.Substring(
                    this=string_expr,
                    start=sql_one,
                    length=sqlglot_expressions.convert(stop_idx),
                )
            else:
                # Convert negative stop index to positive index
                # For example, with string "hello" and stop_idx=-2:
                # LENGTH("hello") + (-2) = 3 when is_zero_based=True
                # No +1 adjustment needed since we're using 0-based indexing
                # to calculate the length, of which the higher bound is exclusive.
                stop_idx_glot = positive_index(string_expr, stop_idx, True)

                # Create a SUBSTRING expression that starts from beginning
                return sqlglot_expressions.Substring(
                    this=string_expr,  # The original string to slice
                    start=sql_one,  # Always start from position 1
                    length=convert_iff_case(
                        None,
                        [
                            # Check if the calculated stop position is less than 0
                            sqlglot_expressions.LT(
                                this=stop_idx_glot, expression=sql_zero
                            ),
                            sql_zero,  # If true, length is 0 (empty string)
                            stop_idx_glot,  # If false, use index position as length
                        ],
                        config,
                    ),
                )
        case _:
            assert start_idx is not None
            assert stop_idx is not None
            # Get the positive index if negative
            if start_idx >= 0 and stop_idx >= 0:
                if start_idx > stop_idx:
                    return sql_empty_str
                return sqlglot_expressions.Substring(
                    this=string_expr,
                    start=sqlglot_expressions.convert(start_idx + 1),
                    length=sqlglot_expressions.convert(stop_idx - start_idx),
                )
            if start_idx < 0 and stop_idx >= 0:
                # Calculate the positive index equivalent for the negative start index
                # e.g., for string "hello" and start_idx=-2, converts to index 4 (LENGTH("hello") + (-2) + 1)
                start_idx_glot = positive_index(string_expr, start_idx)

                # Adjust start index to ensure it's not less than 1 (SQL's SUBSTRING is 1-based)
                start_idx_adjusted_glot = convert_iff_case(
                    None,
                    [
                        sqlglot_expressions.LT(this=start_idx_glot, expression=sql_one),
                        sql_one,  # If calculated position < 1, use position 1
                        start_idx_glot,  # Otherwise use calculated position
                    ],
                    config,
                )

                # Convert positive stop_idx to 1-based indexing by adding 1
                # e.g., for stop_idx=3 (0-based), converts to 4 (1-based)
                stop_idx_adjusted_glot = sqlglot_expressions.convert(stop_idx + 1)

                # Create the SUBSTRING expression
                answer = sqlglot_expressions.Substring(
                    this=string_expr,  # The original string to slice
                    start=start_idx_adjusted_glot,  # Use adjusted start position
                    length=convert_iff_case(
                        None,
                        [
                            # Check if the length (stop - start) is negative or zero
                            sqlglot_expressions.LTE(
                                this=sqlglot_expressions.Sub(
                                    this=stop_idx_adjusted_glot,
                                    expression=start_idx_adjusted_glot,
                                ),
                                expression=sql_zero,
                            ),
                            sql_empty_str,  # If length ≤ 0, return empty string
                            # Otherwise calculate actual length
                            sqlglot_expressions.Sub(
                                this=stop_idx_adjusted_glot,
                                expression=start_idx_adjusted_glot,
                            ),
                        ],
                        config,
                    ),
                )
                return answer
            if start_idx >= 0 and stop_idx < 0:
                # Convert negative stop index to its positive equivalent
                # e.g., for string "hello" and stop_idx=-2, converts to index 4 (LENGTH("hello") + (-2) + 1)
                stop_idx_adjusted_glot = positive_index(string_expr, stop_idx)

                # Convert start index to 1-based indexing (SQL's SUBSTRING is 1-based)
                # e.g., for start_idx=1 (0-based), converts to 2 (1-based)
                start_idx_adjusted_glot = sqlglot_expressions.convert(start_idx + 1)

                # Create the SUBSTRING expression
                answer = sqlglot_expressions.Substring(
                    this=string_expr,  # The original string to slice
                    start=start_idx_adjusted_glot,  # Use 1-based start position
                    length=convert_iff_case(
                        None,
                        [
                            # First check: Is the calculated stop position less than 1?
                            sqlglot_expressions.LT(
                                this=stop_idx_adjusted_glot, expression=sql_one
                            ),
                            sql_zero,  # If true, length becomes 0 (empty string)
                            convert_iff_case(
                                None,
                                [  # Second check: Is the length negative?
                                    sqlglot_expressions.LTE(
                                        this=sqlglot_expressions.Sub(
                                            this=stop_idx_adjusted_glot,
                                            expression=start_idx_adjusted_glot,
                                        ),
                                        expression=sql_zero,
                                    ),
                                    sql_empty_str,  # If length ≤ 0, return empty string
                                    sqlglot_expressions.Sub(  # Otherwise calculate actual length
                                        this=stop_idx_adjusted_glot,
                                        expression=start_idx_adjusted_glot,
                                    ),
                                ],
                                config,
                            ),
                        ],
                        config,
                    ),
                )
                return answer
            if start_idx < 0 and stop_idx < 0:
                # Early return if start index is greater than stop index
                # e.g., "hello"[-2:-4] should return empty string
                if start_idx >= stop_idx:
                    return sql_empty_str

                # Convert negative start index to positive equivalent
                # e.g., for string "hello" and start_idx=-2, converts to index 4 (LENGTH("hello") + (-2) + 1)
                pos_start_idx_glot = positive_index(string_expr, start_idx)

                # Adjust start index to ensure it's not less than 1 (SQL's SUBSTRING is 1-based)
                start_idx_adjusted_glot = convert_iff_case(
                    None,
                    [
                        sqlglot_expressions.LT(
                            this=pos_start_idx_glot, expression=sql_one
                        ),
                        sql_one,  # If calculated position < 1, use position 1
                        pos_start_idx_glot,  # Otherwise use calculated position
                    ],
                    config,
                )

                # Convert negative stop index to positive equivalent
                stop_idx_adjusted_glot = positive_index(string_expr, stop_idx)

                # Create the SUBSTRING expression
                return sqlglot_expressions.Substring(
                    this=string_expr,  # The original string to slice
                    start=start_idx_adjusted_glot,  # Use adjusted start position
                    length=convert_iff_case(
                        None,
                        [
                            # Check if the stop position is less than 1
                            sqlglot_expressions.LT(
                                this=stop_idx_adjusted_glot, expression=sql_one
                            ),
                            sql_zero,  # Length becomes 0 if stop_idx is < 1
                            sqlglot_expressions.Sub(  # Else calculate length as (stop - start)
                                this=stop_idx_adjusted_glot,
                                expression=start_idx_adjusted_glot,
                            ),
                        ],
                        config,
                    ),
                )


def convert_concat_ws(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for generating a `CONCAT_WS` expression from a list of arguments.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `CONCAT_WS`.
    """
    return sqlglot_expressions.ConcatWs(expressions=sql_glot_args)


def convert_concat_ws_to_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Converts an expression equivalent to a `CONCAT_WS` call into a chain of
    `CONCAT` calls.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `CONCAT_WS`.
    """
    args: list[SQLGlotExpression] = []
    for i in range(1, len(sql_glot_args)):
        if i > 1:
            args.append(sql_glot_args[0])
        args.append(sql_glot_args[i])
    return sqlglot_expressions.Concat(expressions=args)


def convert_like(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for generating a `LIKE` expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        `raw_args`: The operands to `LIKE`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `LIKE`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `LIKE`.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    pattern: SQLGlotExpression = apply_parens(sql_glot_args[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Convert a `STARTSWITH` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `STARTSWITH`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `STARTSWITH`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `STARTSWITH`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `STARTSWITH`
        by using `LIKE` where the pattern is the original STARTSWITH string,
        prepended with `'%'`.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sql_glot_args[1], sqlglot_expressions.convert("%")],
        config,
    )
    return convert_like(None, [column, pattern], config)


def convert_endswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Convert a `ENDSWITH` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `ENDSWITH`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `ENDSWITH`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `ENDSWITH`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `ENDSWITH`
        by using `LIKE` where the pattern is the original ENDSWITH string,
        prepended with `'%'`.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sqlglot_expressions.convert("%"), sql_glot_args[1]],
        config,
    )
    return convert_like(None, [column, pattern], config)


def convert_contains(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Convert a `CONTAINS` call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert `CONTAINS`
    to a LIKE expression for SQLite.

    Args:
        `raw_args`: The operands to `CONTAINS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONTAINS`, after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `CONTAINS`
        by using `LIKE` where the pattern is the original contains string,
        sandwiched between `'%'` on either side.
    """
    # TODO: (gh #170) update to a different transformation for array/map containment
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [
            sqlglot_expressions.convert("%"),
            sql_glot_args[1],
            sqlglot_expressions.convert("%"),
        ],
        config,
    )
    return convert_like(None, [column, pattern], config)


def pad_helper(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    pad_func: str,
) -> SQLGlotExpression:
    """
    Helper function for LPAD and RPAD.
    Expects sqlglot_args[0] to be the column to pad.
    Expects sqlglot_args[1] and sqlglot_args[2] to be literals.
    Expects sqlglot_args[1] to be the returned length of the padded string.
    Expects sqlglot_args[2] to be the string to pad with.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions. The first operand is expected to be a string.
        `pad_func`: The name of the padding function to use.

    Returns:
        A tuple of sqlglot expressions for the column to pad, the length of the column,
        the required length, padding string and the integer literal of the required length.
    """
    assert pad_func in ["LPAD", "RPAD"]
    assert len(sql_glot_args) == 3

    if (
        isinstance(sql_glot_args[1], sqlglot_expressions.Literal)
        and not sql_glot_args[1].is_string
    ):
        try:
            required_len = int(sql_glot_args[1].this)
            if required_len < 0:
                raise ValueError()
        except ValueError:
            raise ValueError(
                f"{pad_func} function requires the length argument to be a non-negative integer literal."
            )
    else:
        raise ValueError(
            f"{pad_func} function requires the length argument to be a non-negative integer literal."
        )

    if (
        not isinstance(sql_glot_args[2], sqlglot_expressions.Literal)
        or not sql_glot_args[2].is_string
    ):
        raise ValueError(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )
    if len(str(sql_glot_args[2].this)) != 1:
        raise ValueError(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )

    col_glot = sql_glot_args[0]
    col_len_glot = sqlglot_expressions.Length(this=sql_glot_args[0])
    required_len_glot = sqlglot_expressions.convert(required_len)
    pad_string_glot = sqlglot_expressions.convert(
        str(sql_glot_args[2].this) * required_len
    )
    return col_glot, col_len_glot, required_len_glot, pad_string_glot, required_len


def convert_lpad(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Converts and pads the string to the left till the string is the specified
    length.
    If length is 0, return an empty string.
    If length is negative, raise an error.
    If length is positive, pad the string on the left to the specified length.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions. The first operand is expected to be
        a string.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `LPAD(string, length, padding)`. With the caveat that if length is 0,
        it will return an empty string.
    """
    col_glot, col_len_glot, required_len_glot, pad_string_glot, required_len = (
        pad_helper(raw_args, sql_glot_args, "LPAD")
    )
    if required_len == 0:
        return sqlglot_expressions.convert("")

    answer = convert_iff_case(
        None,
        [
            sqlglot_expressions.GTE(this=col_len_glot, expression=required_len_glot),
            sqlglot_expressions.Substring(
                this=col_glot,
                start=sqlglot_expressions.convert(1),
                length=required_len_glot,
            ),
            sqlglot_expressions.Substring(
                this=convert_concat(None, [pad_string_glot, col_glot], config),
                start=apply_parens(
                    sqlglot_expressions.Mul(
                        this=required_len_glot,
                        expression=sqlglot_expressions.convert(-1),
                    )
                ),
            ),
        ],
        config,
    )
    return answer


def convert_rpad(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Converts and pads the string to the right to the specified length.
    If length is 0, return an empty string.
    If length is negative, raise an error.
    If length is positive, pad the string on the right to the specified length.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions. The first operand is expected to be
        a string.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `RPAD(string, length, padding)`. With the caveat that if length is 0,
        it will return an empty string.
    """
    col_glot, _, required_len_glot, pad_string_glot, required_len = pad_helper(
        raw_args, sql_glot_args, "RPAD"
    )
    if required_len == 0:
        return sqlglot_expressions.convert("")

    answer = sqlglot_expressions.Substring(
        this=convert_concat(None, [col_glot, pad_string_glot], config),
        start=sqlglot_expressions.convert(1),
        length=required_len_glot,
    )
    return answer


def convert_isin(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Convert an `ISIN` call expression to a SQLGlot expression. This is done
    because converting to IN is non-standard.

    Args:
        `raw_args`: The operands to `ISIN`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `ISIN`, after they were converted to
        SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `ISIN`
        by doing `x IN y` on its operands.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    # Note: We only handle the case with multiple literals where all
    # literals are in the same literal expression. This code will need
    # to change when we support PyDough expressions like:
    # Collection.WHERE(ISIN(name, plural_subcollection.name))
    values: SQLGlotExpression = sql_glot_args[1]
    return sqlglot_expressions.In(this=column, expressions=values)


def convert_ndistinct(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Converts a `NDISTINCT` call expression to a SQLGlot expression.

    Args:
        `raw_args`: The operands to `NDISTINCT`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `NDISTINCT`, after they were converted
        to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `NDISTINCT`
        by calling `COUNT(DISTINCT)` on its operand.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    return sqlglot_expressions.Count(
        this=sqlglot_expressions.Distinct(expressions=[column])
    )


def create_convert_datetime_unit_function(unit: str):
    """
    Creates a function that extracts a specific date/time unit from a SQLGlot
    expression.

    Args:
        `unit`: The time unit to extract.
    Returns:
        A function that can convert operands into a SQLGlot expression matching
        the functionality of `EXTRACT(unit FROM expression)`.
    """

    def convert_datetime_unit(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ) -> SQLGlotExpression:
        """
            Converts and extracts the specific time unit from a SQLGlot expression.

        Args:
            `raw_args`: The operands passed to the function before they were
            converted to SQLGlot expressions. (Not actively used in this
            implementation.)
            `sql_glot_args`: The operands passed to the function after they were
            converted to SQLGlot expressions.
            `config`: The PyDough configuration.

            Returns:
                The SQLGlot expression matching the functionality of
                `EXTRACT(unit FROM expression)` by extracting the specified time unit
                from the first operand.
        """
        return sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit),
            expression=make_datetime_arg(
                sql_glot_args[0], dialect=DatabaseDialect.ANSI
            ),
        )

    return convert_datetime_unit


def convert_sqrt(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for getting the square root of the operand.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `POWER(x,0.5)`,i.e the square root.
    """

    return sqlglot_expressions.Pow(
        this=sql_glot_args[0], expression=sqlglot_expressions.Literal.number(0.5)
    )


def make_datetime_arg(
    expr: SQLGlotExpression, dialect: DatabaseDialect
) -> SQLGlotExpression:
    """
    Converts a SQLGlot expression to a datetime argument, if needed, including:
    - Converting a string literal for "now" or similar aliases into a call to
    get the current timestamp.
    - Converting a string literal for a datetime into a datetime expression.
    """
    if isinstance(expr, sqlglot_expressions.Literal) and expr.is_string:
        return handle_datetime_base_arg(expr, dialect)
    return expr


def convert_datediff(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for getting the difference between two dates in sqlite.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `DATEDIFF(y, x)`,i.e the difference between two dates.
    """
    assert len(sql_glot_args) == 3
    # Check if unit is a string.
    if not isinstance(sql_glot_args[0], sqlglot_expressions.Literal):
        raise ValueError(
            f"Unsupported argument {sql_glot_args[0]} for DATEDIFF."
            "It should be a string."
        )
    elif not sql_glot_args[0].is_string:
        raise ValueError(
            f"Unsupported argument {sql_glot_args[0]} for DATEDIFF."
            "It should be a string."
        )
    x = make_datetime_arg(sql_glot_args[1], DatabaseDialect.ANSI)
    y = make_datetime_arg(sql_glot_args[2], DatabaseDialect.ANSI)
    unit: DateTimeUnit | None = DateTimeUnit.from_string(sql_glot_args[0].this)
    if unit is None:
        raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")
    answer = sqlglot_expressions.DateDiff(
        unit=sqlglot_expressions.Var(this=unit.value), this=y, expression=x
    )
    return answer


def convert_sqlite_datediff(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for getting the difference between two dates in sqlite.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `DATEDIFF(unit, y, x)`, i.e the difference between two dates.
    """
    assert len(sql_glot_args) == 3
    # Check if unit is a string.
    if not isinstance(sql_glot_args[0], sqlglot_expressions.Literal):
        raise ValueError(
            f"Unsupported argument {sql_glot_args[0]} for DATEDIFF."
            "It should be a string."
        )
    elif not sql_glot_args[0].is_string:
        raise ValueError(
            f"Unsupported argument {sql_glot_args[0]} for DATEDIFF."
            "It should be a string."
        )
    unit: str = sql_glot_args[0].this
    dt_x: SQLGlotExpression = make_datetime_arg(
        sql_glot_args[1], DatabaseDialect.SQLITE
    )
    dt_y: SQLGlotExpression = make_datetime_arg(
        sql_glot_args[2], DatabaseDialect.SQLITE
    )
    match unit.lower():
        case "years" | "year" | "y":
            # Extracts the year from the date and subtracts the years.
            year_x: SQLGlotExpression = convert_sqlite_datetime_extract("'%Y'")(
                None, [dt_x], config
            )
            year_y: SQLGlotExpression = convert_sqlite_datetime_extract("'%Y'")(
                None, [dt_y], config
            )
            # equivalent to: expression - this
            years_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=year_y, expression=year_x
            )
            return years_diff
        case "months" | "month" | "mm":
            # Extracts the difference in years multiplied by 12.
            # Extracts the month from the date and subtracts the months.
            # Adds the difference in months to the difference in years*12.
            # Implementation wise, this is equivalent to:
            # (years_diff * 12 + month_y) - month_x
            # On expansion: (year_y - year_x) * 12 + month_y - month_x
            sql_glot_args_hours = [
                sqlglot_expressions.Literal(this="years", is_string=True),
                dt_x,
                dt_y,
            ]
            _years_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_hours, config
            )
            years_diff_in_months = sqlglot_expressions.Mul(
                this=apply_parens(_years_diff),
                expression=sqlglot_expressions.Literal.number(12),
            )
            month_x = convert_sqlite_datetime_extract("'%m'")(
                None, [sql_glot_args[1]], config
            )
            month_y = convert_sqlite_datetime_extract("'%m'")(
                None, [sql_glot_args[2]], config
            )
            months_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(
                    this=years_diff_in_months, expression=month_y
                ),
                expression=month_x,
            )
            return months_diff
        case "weeks" | "week" | "w":
            # DATEDIFF('week', A, B)
            #   = DATEDIFF('day', DATETIME(A, 'start of week'),
            #               DATETIME(B, 'start of week')) / 7
            dt1 = convert_datetime(DatabaseDialect.SQLITE)(
                None,
                [
                    sql_glot_args[1],
                    sqlglot_expressions.Literal(this="start of week", is_string=True),
                ],
                config,
            )
            dt2 = convert_datetime(DatabaseDialect.SQLITE)(
                None,
                [
                    sql_glot_args[2],
                    sqlglot_expressions.Literal(this="start of week", is_string=True),
                ],
                config,
            )
            sql_glot_args_days = [
                sqlglot_expressions.Literal(this="days", is_string=True),
                dt1,
                dt2,
            ]
            weeks_days_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_days, config
            )
            answer: SQLGlotExpression = sqlglot_expressions.Cast(
                this=sqlglot_expressions.Div(
                    this=apply_parens(weeks_days_diff),
                    expression=sqlglot_expressions.Literal.number(7),
                ),
                to=sqlglot_expressions.DataType(
                    this=sqlglot_expressions.DataType.Type.INT
                ),
            )
            return answer
        case "days" | "day" | "d":
            # Extracts the start of date from the datetime and subtracts the dates.
            date_x = sqlglot_expressions.Date(
                this=[
                    dt_x,
                    sqlglot_expressions.Literal(this="start of day", is_string=True),
                ]
            )
            date_y = sqlglot_expressions.Date(
                this=[
                    dt_y,
                    sqlglot_expressions.Literal(this="start of day", is_string=True),
                ]
            )
            # This calculates 'this-expression'.
            answer = sqlglot_expressions.DateDiff(
                unit=sqlglot_expressions.Var(this="days"),
                this=date_y,
                expression=date_x,
            )
            return answer
        case "hours" | "hour" | "h":
            # Extracts the difference in days multiplied by 24 to get difference in hours.
            # Extracts the hours of x and hours of y.
            # Adds the difference in hours to the (difference in days*24).
            # Implementation wise, this is equivalent to:
            # (days_diff*24 + hours_y) - hours_x
            # On expansion: (( day_y - day_x ) * 24 + hours_y) - hours_x
            sql_glot_args_days = [
                sqlglot_expressions.Literal(this="days", is_string=True),
                dt_x,
                dt_y,
            ]
            _days_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_days, config
            )
            days_diff_in_hours = sqlglot_expressions.Mul(
                this=apply_parens(_days_diff),
                expression=sqlglot_expressions.Literal.number(24),
            )
            hours_x: SQLGlotExpression = convert_sqlite_datetime_extract("'%H'")(
                None, [dt_x], config
            )
            hours_y: SQLGlotExpression = convert_sqlite_datetime_extract("'%H'")(
                None, [dt_y], config
            )
            hours_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(
                    this=days_diff_in_hours, expression=hours_y
                ),
                expression=hours_x,
            )
            return hours_diff
        case "minutes" | "minute" | "m":
            # Extracts the difference in hours multiplied by 60 to get difference in minutes.
            # Extracts the minutes of x and minutes of y.
            # Adds the difference in minutes to the (difference in hours*60).
            # Implementation wise, this is equivalent to:
            # (hours_diff*60 + minutes_y) - minutes_x
            # On expansion: (( hours_y - hours_x )*60 + minutes_y) - minutes_x
            sql_glot_args_hours = [
                sqlglot_expressions.Literal(this="hours", is_string=True),
                dt_x,
                dt_y,
            ]
            _hours_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_hours, config
            )
            hours_diff_in_mins = sqlglot_expressions.Mul(
                this=apply_parens(_hours_diff),
                expression=sqlglot_expressions.Literal.number(60),
            )
            min_x = convert_sqlite_datetime_extract("'%M'")(
                None, [sql_glot_args[1]], config
            )
            min_y = convert_sqlite_datetime_extract("'%M'")(
                None, [sql_glot_args[2]], config
            )
            mins_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(this=hours_diff_in_mins, expression=min_y),
                expression=min_x,
            )
            return mins_diff
        case "seconds" | "second" | "s":
            # Extracts the difference in minutes multiplied by 60 to get difference in seconds.
            # Extracts the seconds of x and seconds of y.
            # Adds the difference in seconds to the (difference in minutes*60).
            # Implementation wise, this is equivalent to:
            # (mins_diff*60 + seconds_y) - seconds_x
            # On expansion: (( mins_y - mins_x )*60 + seconds_y) - seconds_x
            sql_glot_args_minutes = [
                sqlglot_expressions.Literal(this="minutes", is_string=True),
                dt_x,
                dt_y,
            ]
            _mins_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_minutes, config
            )
            minutes_diff_in_secs = sqlglot_expressions.Mul(
                this=apply_parens(_mins_diff),
                expression=sqlglot_expressions.Literal.number(60),
            )
            sec_x = convert_sqlite_datetime_extract("'%S'")(
                None, [sql_glot_args[1]], config
            )
            sec_y = convert_sqlite_datetime_extract("'%S'")(
                None, [sql_glot_args[2]], config
            )
            secs_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(
                    this=minutes_diff_in_secs, expression=sec_y
                ),
                expression=sec_x,
            )
            return secs_diff
        case _:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")


def convert_round(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for rounding a number to a specified precision.
    If no precision is provided, the number is rounded to 0 decimal places.
    If a precision is provided, it must be an integer literal.

    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `ROUND(number, precision)`.
    """
    assert len(sql_glot_args) == 1 or len(sql_glot_args) == 2
    precision_glot: SQLGlotExpression
    if len(sql_glot_args) == 1:
        precision_glot = sqlglot_expressions.Literal.number(0)
    else:
        # Check if the second argument is a integer literal.
        if (
            not isinstance(sql_glot_args[1], sqlglot_expressions.Literal)
            or sql_glot_args[1].is_string
        ):
            raise ValueError(
                f"Unsupported argument {sql_glot_args[1]} for ROUND."
                "The precision argument should be an integer literal."
            )
        try:
            int(sql_glot_args[1].this)
        except ValueError:
            raise ValueError(
                f"Unsupported argument {sql_glot_args[1]} for ROUND."
                "The precision argument should be an integer literal."
            )
        precision_glot = sql_glot_args[1]
    return sqlglot_expressions.Round(
        this=sql_glot_args[0],
        decimals=precision_glot,
    )


def convert_sign(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for getting the sign of the operand.
    It returns 1 if the input is positive, -1 if the input is negative,
    and 0 if the input is zero.
    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `SIGN(X)`.
    """
    assert len(sql_glot_args) == 1
    arg: SQLGlotExpression = sql_glot_args[0]
    zero_glot: SQLGlotExpression = sqlglot_expressions.Literal.number(0)
    one_glot: SQLGlotExpression = sqlglot_expressions.Literal.number(1)
    minus_one_glot: SQLGlotExpression = sqlglot_expressions.Literal.number(-1)
    answer: SQLGlotExpression = convert_iff_case(
        None,
        [
            sqlglot_expressions.EQ(this=arg, expression=zero_glot),
            zero_glot,
            apply_parens(
                convert_iff_case(
                    None,
                    [
                        sqlglot_expressions.LT(this=arg, expression=zero_glot),
                        minus_one_glot,
                        one_glot,
                    ],
                    config,
                ),
            ),
        ],
        config,
    )
    return answer


def convert_strip(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for removing all leading and trailing whitespace from a string.
    If a second argument is provided, it is used as the set of characters
    to remove from the leading and trailing ends of the first argument.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of `STRIP(X, Y)`.
        In Python, this is equivalent to `X.strip(Y)`.
    """
    assert 1 <= len(sql_glot_args) <= 2
    to_strip: SQLGlotExpression = sql_glot_args[0]
    strip_char_glot: SQLGlotExpression
    if len(sql_glot_args) == 1:
        strip_char_glot = sqlglot_expressions.Literal.string("\n\t ")
    else:
        strip_char_glot = sql_glot_args[1]
    return sqlglot_expressions.Trim(
        this=to_strip,
        expression=strip_char_glot,
    )


def convert_find(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
    config: PyDoughConfigs,
) -> SQLGlotExpression:
    """
    Support for getting the index of the first occurrence of a substring within
    a string. The first argument is the string to search within, and the second
    argument is the substring to search for.
    Args:
        `raw_args`: The operands passed to the function before they were
        converted to SQLGlot expressions. (Not actively used in this
        implementation.)
        `sql_glot_args`: The operands passed to the function after they were
        converted to SQLGlot expressions.
        `config`: The PyDough configuration.

    Returns:
        The SQLGlot expression matching the functionality of
        `FIND(this, expression)`,i.e the index of the first occurrence of
        the second argument within the first argument, or -1 if the second
        argument is not found.
    """
    assert len(sql_glot_args) == 2
    answer: SQLGlotExpression = sqlglot_expressions.Sub(
        this=sqlglot_expressions.StrPosition(
            this=sql_glot_args[0], substr=sql_glot_args[1]
        ),
        expression=sqlglot_expressions.Literal.number(1),
    )
    return answer


class SqlGlotTransformBindings:
    """
    Binding infrastructure used to associate PyDough operators with a procedure
    that transforms an invocation of the operator onto certain arguments into a
    SQLGlot expression in a manner that is consistent with the dialect being
    used.
    """

    def __init__(self):
        self.dialect: DatabaseDialect = DatabaseDialect.ANSI
        self.bindings: dict[pydop.PyDoughOperator, transform_binding] = {}
        self.set_dialect(DatabaseDialect.ANSI)

    def set_dialect(self, dialect: DatabaseDialect):
        """
        Switches the dialect used by the function bindings, and changing the
        bindings however necessary.
        """
        self.dialect = dialect
        # Always refresh the default bindings in case the previous dialect
        # overwrote any of them.
        self.add_builtin_bindings()
        match dialect:
            case DatabaseDialect.ANSI:
                pass
            case DatabaseDialect.SQLITE:
                self.add_sqlite_bindings()
            case _:
                raise Exception(
                    f"TODO: support dialect {dialect} in SQLGlot transformation"
                )

    def call(
        self,
        operator: pydop.PyDoughOperator,
        raw_args: Sequence[RelationalExpression],
        sql_glot_args: Sequence[SQLGlotExpression],
        config: PyDoughConfigs,
    ) -> SQLGlotExpression:
        """
        Converts an invocation of a PyDough operator into a SQLGlot expression
        in terms of its operands in a manner consistent with the function
        bindings.

        Args:
            `operator`: the PyDough operator corresponding to the function call
            being converted to SQLGlot.
            `raw_args`: the operands to the function, before they were
            converted to SQLGlot expressions.
            `sql_glot_args`: the operands to the function, after they were
            converted to SQLGlot expressions.
            `config`: The PyDough configuration.

        Returns:
            The SQLGlot expression corresponding to the operator invocation on
            the specified operands.
        """
        if operator not in self.bindings:
            # TODO: (gh #169) add support for UDFs
            raise ValueError(f"Unsupported function {operator}")
        binding: transform_binding = self.bindings[operator]
        return binding(raw_args, sql_glot_args, config)

    def bind_simple_function(
        self, operator: pydop.PyDoughOperator, func: SQLGlotFunction
    ) -> None:
        """
        Adds a function binding for a basic function call.

        Args:
            `operator`: the PyDough operator for the function operation being
            bound.
            `func`: the SQLGlot function for the function it is being bound to.
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
            config: PyDoughConfigs,
        ) -> SQLGlotExpression:
            return func.from_arg_list(sql_glot_args)

        self.bindings[operator] = impl

    def bind_binop(
        self, operator: pydop.PyDoughOperator, func: SQLGlotFunction
    ) -> None:
        """
        Adds a function binding for a binary operator.

        Args:
            `operator`: the PyDough operator for the binary operator being
            bound.
            `func`: the SQLGlot function for the binary operator it is being
            bound to.
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
            config: PyDoughConfigs,
        ) -> SQLGlotExpression:
            assert len(sql_glot_args) >= 2
            # Note: SQLGlot explicit inserts parentheses for binary operations
            # during parsing.
            output_expr = apply_parens(sql_glot_args[0])
            for expr in sql_glot_args[1:]:
                other_expr: SQLGlotExpression = apply_parens(expr)
                # Build the expressions on the left since the operator is left-associative.
                output_expr = func(this=output_expr, expression=other_expr)
            return output_expr

        self.bindings[operator] = impl

    def bind_unop(self, operator: pydop.PyDoughOperator, func: SQLGlotFunction) -> None:
        """
        Adds a function binding for a unary operator.

        Args:
            `operator`: the PyDough operator for the unary operator being
            bound.
            `func`: the SQLGlot function for the unary operator it is being
            bound to.
        """

        def impl(
            raw_args: Sequence[RelationalExpression] | None,
            sql_glot_args: Sequence[SQLGlotExpression],
            config: PyDoughConfigs,
        ) -> SQLGlotExpression:
            assert len(sql_glot_args) == 1
            return func(this=sql_glot_args[0])

        self.bindings[operator] = impl

    def add_builtin_bindings(self) -> None:
        """
        Adds all of the bindings that are when converting to ANSI SQL, or are
        standard across dialects.
        """
        # Aggregation functions
        self.bind_simple_function(pydop.SUM, sqlglot_expressions.Sum)
        self.bind_simple_function(pydop.AVG, sqlglot_expressions.Avg)
        self.bind_simple_function(pydop.COUNT, sqlglot_expressions.Count)
        self.bind_simple_function(pydop.MIN, sqlglot_expressions.Min)
        self.bind_simple_function(pydop.MAX, sqlglot_expressions.Max)
        self.bind_simple_function(pydop.ANYTHING, sqlglot_expressions.AnyValue)
        self.bindings[pydop.NDISTINCT] = convert_ndistinct

        # String functions
        self.bind_simple_function(pydop.LOWER, sqlglot_expressions.Lower)
        self.bind_simple_function(pydop.UPPER, sqlglot_expressions.Upper)
        self.bind_simple_function(pydop.LENGTH, sqlglot_expressions.Length)
        self.bindings[pydop.STARTSWITH] = convert_startswith
        self.bindings[pydop.ENDSWITH] = convert_endswith
        self.bindings[pydop.CONTAINS] = convert_contains
        self.bindings[pydop.LIKE] = convert_like
        self.bindings[pydop.SLICE] = convert_slice
        self.bindings[pydop.JOIN_STRINGS] = convert_concat_ws
        self.bindings[pydop.LPAD] = convert_lpad
        self.bindings[pydop.RPAD] = convert_rpad
        self.bindings[pydop.FIND] = convert_find
        self.bindings[pydop.STRIP] = convert_strip

        # Numeric functions
        self.bind_simple_function(pydop.ABS, sqlglot_expressions.Abs)
        self.bind_simple_function(pydop.ROUND, sqlglot_expressions.Round)
        self.bindings[pydop.SIGN] = convert_sign
        self.bindings[pydop.ROUND] = convert_round

        # Conditional functions
        self.bind_simple_function(pydop.DEFAULT_TO, sqlglot_expressions.Coalesce)
        self.bindings[pydop.IFF] = convert_iff_case
        self.bindings[pydop.ISIN] = convert_isin
        self.bindings[pydop.PRESENT] = convert_present
        self.bindings[pydop.ABSENT] = convert_absent
        self.bindings[pydop.KEEP_IF] = convert_keep_if
        self.bindings[pydop.MONOTONIC] = convert_monotonic

        # Datetime functions
        self.bindings[pydop.YEAR] = create_convert_datetime_unit_function("YEAR")
        self.bindings[pydop.MONTH] = create_convert_datetime_unit_function("MONTH")
        self.bindings[pydop.DAY] = create_convert_datetime_unit_function("DAY")
        self.bindings[pydop.HOUR] = create_convert_datetime_unit_function("HOUR")
        self.bindings[pydop.MINUTE] = create_convert_datetime_unit_function("MINUTE")
        self.bindings[pydop.SECOND] = create_convert_datetime_unit_function("SECOND")
        self.bindings[pydop.DATEDIFF] = convert_datediff
        self.bindings[pydop.DATETIME] = convert_datetime(DatabaseDialect.ANSI)
        self.bindings[pydop.DAYOFWEEK] = convert_dayofweek(DatabaseDialect.ANSI)
        self.bindings[pydop.DAYNAME] = convert_dayname(DatabaseDialect.ANSI)

        # Binary operators
        self.bind_binop(pydop.ADD, sqlglot_expressions.Add)
        self.bind_binop(pydop.SUB, sqlglot_expressions.Sub)
        self.bind_binop(pydop.MUL, sqlglot_expressions.Mul)
        self.bind_binop(pydop.DIV, sqlglot_expressions.Div)
        self.bind_binop(pydop.EQU, sqlglot_expressions.EQ)
        self.bind_binop(pydop.GEQ, sqlglot_expressions.GTE)
        self.bind_binop(pydop.GRT, sqlglot_expressions.GT)
        self.bind_binop(pydop.LEQ, sqlglot_expressions.LTE)
        self.bind_binop(pydop.LET, sqlglot_expressions.LT)
        self.bind_binop(pydop.NEQ, sqlglot_expressions.NEQ)
        self.bind_binop(pydop.BAN, sqlglot_expressions.And)
        self.bind_binop(pydop.BOR, sqlglot_expressions.Or)
        self.bind_binop(pydop.POW, sqlglot_expressions.Pow)
        self.bind_binop(pydop.POWER, sqlglot_expressions.Pow)
        self.bindings[pydop.SQRT] = convert_sqrt

        # Unary operators
        self.bind_unop(pydop.NOT, sqlglot_expressions.Not)

    def add_sqlite_bindings(self) -> None:
        """
        Adds the bindings & overrides that are specific to SQLite.
        """
        # Use IF function instead of CASE if the SQLite version is recent
        # enough.
        if sqlite3.sqlite_version >= "3.32":
            self.bind_simple_function(pydop.IFF, sqlglot_expressions.If)

        self.bindings[pydop.DATETIME] = convert_datetime(DatabaseDialect.SQLITE)
        self.bindings[pydop.DAYOFWEEK] = convert_dayofweek(DatabaseDialect.SQLITE)
        self.bindings[pydop.DAYNAME] = convert_dayname(DatabaseDialect.SQLITE)

        # Datetime function overrides
        self.bindings[pydop.YEAR] = convert_sqlite_datetime_extract("'%Y'")
        self.bindings[pydop.MONTH] = convert_sqlite_datetime_extract("'%m'")
        self.bindings[pydop.DAY] = convert_sqlite_datetime_extract("'%d'")
        self.bindings[pydop.HOUR] = convert_sqlite_datetime_extract("'%H'")
        self.bindings[pydop.MINUTE] = convert_sqlite_datetime_extract("'%M'")
        self.bindings[pydop.SECOND] = convert_sqlite_datetime_extract("'%S'")
        self.bindings[pydop.DATEDIFF] = convert_sqlite_datediff

        # String function overrides
        if sqlite3.sqlite_version < "3.44.1":
            self.bindings[pydop.JOIN_STRINGS] = convert_concat_ws_to_concat
