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
from pydough.database_connectors.database_connector import DatabaseDialect
from pydough.relational.relational_expressions import RelationalExpression

operator = pydop.PyDoughOperator
transform_binding = Callable[
    [Sequence[RelationalExpression] | None, Sequence[SQLGlotExpression]],
    SQLGlotExpression,
]

PAREN_EXPRESSIONS = (Binary, Unary, Concat, Is, Case)
"""
The types of SQLGlot expressions that need to be wrapped in parenthesis for the
sake of precedence.
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
    ) -> SQLGlotExpression:
        return sqlglot_expressions.Cast(
            this=sqlglot_expressions.TimeToStr(
                this=sql_glot_args[0], format=format_str
            ),
            to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
        )

    return impl


def apply_datetime_truncation(
    base: SQLGlotExpression, unit: DateTimeUnit, dialect: DatabaseDialect
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
        offset_expr: SQLGlotExpression = sqlglot_expressions.convert(
            f"{amt} {unit.value}"
        )
        if isinstance(base, sqlglot_expressions.Datetime) or (
            isinstance(base, sqlglot_expressions.Date)
            and unit in (DateTimeUnit.YEAR, DateTimeUnit.MONTH, DateTimeUnit.DAY)
        ):
            base.this.append(offset_expr)
            return base
        return sqlglot_expressions.Datetime(
            this=[base, sqlglot_expressions.convert(f"{amt} {unit.value}")],
        )
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
        if str(arg.this).lower().strip() in (
            "now",
            "current_timestamp",
            "current_date",
            "current timestamp",
            "current date",
        ):
            if dialect == DatabaseDialect.SQLITE:
                return sqlglot_expressions.Datetime(
                    this=[sqlglot_expressions.convert("now")]
                )
            else:
                return sqlglot_expressions.CurrentTimestamp()
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
                result = apply_datetime_truncation(result, unit, dialect)
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


def convert_iff_case(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `IFF(a, b, c)` to the expression
    `CASE WHEN a THEN b ELSE c END`, since not every dialect supports IFF.

    Args:
        `raw_args`: The operands to `IFF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `IFF`, after they were
        converted to SQLGlot expressions.

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
) -> SQLGlotExpression:
    """
    Support for converting the expression `ABSENT(X)` to the expression
    `X IS NULL`

    Args:
        `raw_args`: The operands to `ABSENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `ABSENT`, after they were
        converted to SQLGlot expressions.

    Returns:
        The `IS NULL` call corresponding to the `ABSENT` call.
    """
    return sqlglot_expressions.Is(
        this=apply_parens(sql_glot_args[0]), expression=sqlglot_expressions.Null()
    )


def convert_present(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `PRESENT(X)` to the expression
    `X IS NOT NULL`

    Args:
        `raw_args`: The operands to `PRESENT`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `PRESENT`, after they were
        converted to SQLGlot expressions.

    Returns:
        The `IS NOT NULL` call corresponding to the `PRESENT` call.
    """
    return sqlglot_expressions.Not(
        this=apply_parens(convert_absent(raw_args, sql_glot_args))
    )


def convert_keep_if(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `KEEP_IF(X, Y)` to the expression
    `CASE IF Y THEN X END`.
    Args:
        `raw_args`: The operands to `KEEP_IF`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `KEEP_IF`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot case expression equivalent to the `KEEP_IF` call.
    """
    return convert_iff_case(
        None, [sql_glot_args[1], sql_glot_args[0], sqlglot_expressions.Null()]
    )


def convert_monotonic(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for converting the expression `MONOTONIC(A, B, C, ...)` to an
    expression equivalent of `(A <= B) AND (B <= C) AND ...`.

    Args:
        `raw_args`: The operands to `MONOTONIC`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `MONOTONIC`, after they were
        converted to SQLGlot expressions.

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
        - Negative `start`: Compute `LENGTH(string) + start + 1`; clamp to `1` if less than `1`.
    - Case 3: `(None, stop)`
        - Positive `stop`: Slice from position `1` to `stop`.
        - Negative `stop`: Compute `LENGTH(string) + stop`; clamp to `0` if less than `0` (empty slice).
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
                            ),
                        ],
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
                    ),
                )


def convert_concat_ws(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for generating a `CONCAT_WS` expression from a list of arguments.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `CONCAT_WS`.
    """
    return sqlglot_expressions.ConcatWs(expressions=sql_glot_args)


def convert_concat_ws_to_concat(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Converts an expression equivalent to a `CONCAT_WS` call into a chain of
    `CONCAT` calls.

    Args:
        `raw_args`: The operands to `CONCAT_WS`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `CONCAT_WS`, after they were
        converted to SQLGlot expressions.

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
) -> SQLGlotExpression:
    """
    Support for generating a `LIKE` expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        `raw_args`: The operands to `LIKE`, before they were
        converted to SQLGlot expressions.
        `sql_glot_args`: The operands to `LIKE`, after they were
        converted to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `LIKE`.
    """
    column: SQLGlotExpression = apply_parens(sql_glot_args[0])
    pattern: SQLGlotExpression = apply_parens(sql_glot_args[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
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

    Returns:
        The SQLGlot expression matching the functionality of `STARTSWITH`
        by using `LIKE` where the pattern is the original STARTSWITH string,
        prepended with `'%'`.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sql_glot_args[1], sqlglot_expressions.convert("%")],
    )
    return convert_like(None, [column, pattern])


def convert_endswith(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
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

    Returns:
        The SQLGlot expression matching the functionality of `ENDSWITH`
        by using `LIKE` where the pattern is the original ENDSWITH string,
        prepended with `'%'`.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    pattern: SQLGlotExpression = convert_concat(
        None,
        [sqlglot_expressions.convert("%"), sql_glot_args[1]],
    )
    return convert_like(None, [column, pattern])


def convert_contains(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
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
    )
    return convert_like(None, [column, pattern])


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
) -> SQLGlotExpression:
    """
    Converts and pads the string to the left till the string is the specified length.
    If length is 0, return an empty string.
    If length is negative, raise an error.
    If length is positive, pad the string on the left to the specified length.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions. The first operand is expected to be a string.

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
                this=convert_concat(None, [pad_string_glot, col_glot]),
                start=apply_parens(
                    sqlglot_expressions.Mul(
                        this=required_len_glot,
                        expression=sqlglot_expressions.convert(-1),
                    )
                ),
            ),
        ],
    )
    return answer


def convert_rpad(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Converts and pads the string to the right to the specified length.
    If length is 0, return an empty string.
    If length is negative, raise an error.
    If length is positive, pad the string on the right to the specified length.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions. The first operand is expected to be a string.

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
        this=convert_concat(None, [col_glot, pad_string_glot]),
        start=sqlglot_expressions.convert(1),
        length=required_len_glot,
    )
    return answer


def convert_isin(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Convert an `ISIN` call expression to a SQLGlot expression. This is done
    because converting to IN is non-standard.

    Args:
        `raw_args`: The operands to `ISIN`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `ISIN`, after they were converted to
        SQLGlot expressions.

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
) -> SQLGlotExpression:
    """
    Converts a `NDISTINCT` call expression to a SQLGlot expression.

    Args:
        `raw_args`: The operands to `NDISTINCT`, before they were converted to
        SQLGlot expressions.
        `sql_glot_args`: The operands to `NDISTINCT`, after they were converted
        to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of `NDISTINCT`
        by calling `COUNT(DISTINCT)` on its operand.
    """
    column: SQLGlotExpression = sql_glot_args[0]
    return sqlglot_expressions.Count(
        this=sqlglot_expressions.Distinct(expressions=[column])
    )


def create_convert_time_unit_function(unit: str):
    """
    Creates a function that extracts a specific time unit
    (e.g., HOUR, MINUTE, SECOND) from a SQLGlot expression.

    Args:
        `unit`: The time unit to extract. Must be one of 'HOUR', 'MINUTE',
                or 'SECOND'.
    Returns:
        A function that can convert operands into a SQLGlot expression matching
        the functionality of `EXTRACT(unit FROM expression)`.
    """

    def convert_time_unit(
        raw_args: Sequence[RelationalExpression] | None,
        sql_glot_args: Sequence[SQLGlotExpression],
    ) -> SQLGlotExpression:
        """
        Converts and extracts the specific time unit from a SQLGlot expression.

        Args:
            `raw_args`: The operands passed to the function before they were converted to
            SQLGlot expressions. (Not actively used in this implementation.)
            `sql_glot_args`: The operands passed to the function after they were converted
            to SQLGlot expressions. The first operand is expected to be a timestamp or
                                    datetime.

        Returns:
            The SQLGlot expression matching the functionality of
            `EXTRACT(unit FROM expression)` by extracting the specified time unit
            from the first operand.
        """
        return sqlglot_expressions.Extract(
            this=sqlglot_expressions.Var(this=unit), expression=sql_glot_args[0]
        )

    return convert_time_unit


def convert_sqrt(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for getting the square root of the operand.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of
        `POWER(x,0.5)`,i.e the square root.
    """

    return sqlglot_expressions.Pow(
        this=sql_glot_args[0], expression=sqlglot_expressions.Literal.number(0.5)
    )


def convert_datediff(
    raw_args: Sequence[RelationalExpression] | None,
    sql_glot_args: Sequence[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Support for getting the difference between two dates in sqlite.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions.

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
    x = sql_glot_args[1]
    y = sql_glot_args[2]
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
) -> SQLGlotExpression:
    """
    Support for getting the difference between two dates in sqlite.

    Args:
        `raw_args`: The operands passed to the function before they were converted to
        SQLGlot expressions. (Not actively used in this implementation.)
        `sql_glot_args`: The operands passed to the function after they were converted
        to SQLGlot expressions.

    Returns:
        The SQLGlot expression matching the functionality of
        `DATEDIFF(unit, y, x)`,i.e the difference between two dates.
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
    match unit.lower():
        case "years" | "year" | "y":
            # Extracts the year from the date and subtracts the years.
            year_x: SQLGlotExpression = convert_sqlite_datetime_extract("'%Y'")(
                None, [sql_glot_args[1]]
            )
            year_y: SQLGlotExpression = convert_sqlite_datetime_extract("'%Y'")(
                None, [sql_glot_args[2]]
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
                sql_glot_args[1],
                sql_glot_args[2],
            ]
            _years_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_hours
            )
            years_diff_in_months = sqlglot_expressions.Mul(
                this=apply_parens(_years_diff),
                expression=sqlglot_expressions.Literal.number(12),
            )
            month_x = convert_sqlite_datetime_extract("'%m'")(None, [sql_glot_args[1]])
            month_y = convert_sqlite_datetime_extract("'%m'")(None, [sql_glot_args[2]])
            months_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(
                    this=years_diff_in_months, expression=month_y
                ),
                expression=month_x,
            )
            return months_diff
        case "days" | "day" | "d":
            # Extracts the start of date from the datetime and subtracts the dates.
            date_x = sqlglot_expressions.Date(
                this=sql_glot_args[1],
                expressions=[
                    sqlglot_expressions.Literal(this="start of day", is_string=True)
                ],
            )
            date_y = sqlglot_expressions.Date(
                this=sql_glot_args[2],
                expressions=[
                    sqlglot_expressions.Literal(this="start of day", is_string=True)
                ],
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
                sql_glot_args[1],
                sql_glot_args[2],
            ]
            _days_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_days
            )
            days_diff_in_hours = sqlglot_expressions.Mul(
                this=apply_parens(_days_diff),
                expression=sqlglot_expressions.Literal.number(24),
            )
            hours_x: SQLGlotExpression = convert_sqlite_datetime_extract("'%H'")(
                None, [sql_glot_args[1]]
            )
            hours_y: SQLGlotExpression = convert_sqlite_datetime_extract("'%H'")(
                None, [sql_glot_args[2]]
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
                sql_glot_args[1],
                sql_glot_args[2],
            ]
            _hours_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_hours
            )
            hours_diff_in_mins = sqlglot_expressions.Mul(
                this=apply_parens(_hours_diff),
                expression=sqlglot_expressions.Literal.number(60),
            )
            min_x = convert_sqlite_datetime_extract("'%M'")(None, [sql_glot_args[1]])
            min_y = convert_sqlite_datetime_extract("'%M'")(None, [sql_glot_args[2]])
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
                sql_glot_args[1],
                sql_glot_args[2],
            ]
            _mins_diff: SQLGlotExpression = convert_sqlite_datediff(
                raw_args, sql_glot_args_minutes
            )
            minutes_diff_in_secs = sqlglot_expressions.Mul(
                this=apply_parens(_mins_diff),
                expression=sqlglot_expressions.Literal.number(60),
            )
            sec_x = convert_sqlite_datetime_extract("'%S'")(None, [sql_glot_args[1]])
            sec_y = convert_sqlite_datetime_extract("'%S'")(None, [sql_glot_args[2]])
            secs_diff: SQLGlotExpression = sqlglot_expressions.Sub(
                this=sqlglot_expressions.Add(
                    this=minutes_diff_in_secs, expression=sec_y
                ),
                expression=sec_x,
            )
            return secs_diff
        case _:
            raise ValueError(f"Unsupported argument '{unit}' for DATEDIFF.")


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

        Returns:
            The SQLGlot expression corresponding to the operator invocation on
            the specified operands.
        """
        if operator not in self.bindings:
            # TODO: (gh #169) add support for UDFs
            raise ValueError(f"Unsupported function {operator}")
        binding: transform_binding = self.bindings[operator]
        return binding(raw_args, sql_glot_args)

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

        # Numeric functions
        self.bind_simple_function(pydop.ABS, sqlglot_expressions.Abs)
        self.bind_simple_function(pydop.ROUND, sqlglot_expressions.Round)

        # Conditional functions
        self.bind_simple_function(pydop.DEFAULT_TO, sqlglot_expressions.Coalesce)
        self.bindings[pydop.IFF] = convert_iff_case
        self.bindings[pydop.ISIN] = convert_isin
        self.bindings[pydop.PRESENT] = convert_present
        self.bindings[pydop.ABSENT] = convert_absent
        self.bindings[pydop.KEEP_IF] = convert_keep_if
        self.bindings[pydop.MONOTONIC] = convert_monotonic

        # Datetime functions
        self.bind_unop(pydop.YEAR, sqlglot_expressions.Year)
        self.bind_unop(pydop.MONTH, sqlglot_expressions.Month)
        self.bind_unop(pydop.DAY, sqlglot_expressions.Day)
        self.bindings[pydop.HOUR] = create_convert_time_unit_function("HOUR")
        self.bindings[pydop.MINUTE] = create_convert_time_unit_function("MINUTE")
        self.bindings[pydop.SECOND] = create_convert_time_unit_function("SECOND")
        self.bindings[pydop.DATEDIFF] = convert_datediff
        self.bindings[pydop.DATETIME] = convert_datetime(DatabaseDialect.ANSI)

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
