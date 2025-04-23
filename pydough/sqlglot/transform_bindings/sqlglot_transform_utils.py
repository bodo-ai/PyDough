"""
Helper utilities used by SQLGlot function transforms. These are not
necessarily specific to any particular SQL dialect.
"""

__all__ = [
    "DateTimeUnit",
    "apply_parens",
    "current_ts_pattern",
    "offset_pattern",
    "pad_helper",
    "positive_index",
    "trunc_pattern",
]

import re
from enum import Enum
from typing import Union

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Binary, Case, Concat, Is, Paren, Unary
from sqlglot.expressions import Expression as SQLGlotExpression

PAREN_EXPRESSIONS = (Binary, Unary, Concat, Is, Case)
"""
The types of SQLGlot expressions that need to be wrapped in parenthesis for the
sake of precedence.
"""


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

quarter_units = ("quarters", "quarter", "q")
"""
The valid string representations of the quarter unit.
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
    QUARTER = "quarter"
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
        elif unit in quarter_units:
            return DateTimeUnit.QUARTER
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
            case DateTimeUnit.QUARTER:
                raise ValueError("Quarter unit does not have a truncation string.")
            case DateTimeUnit.MONTH:
                return "'%Y-%m-01 00:00:00'"
            case DateTimeUnit.WEEK:
                raise ValueError("Week unit does not have a truncation string.")
            case DateTimeUnit.DAY:
                return "'%Y-%m-%d 00:00:00'"
            case DateTimeUnit.HOUR:
                return "'%Y-%m-%d %H:00:00'"
            case DateTimeUnit.MINUTE:
                return "'%Y-%m-%d %H:%M:00'"
            case DateTimeUnit.SECOND:
                return "'%Y-%m-%d %H:%M:%S'"

    @property
    def extraction_string(self) -> str:
        """
        The format string that can be used to extract the specified unit.
        """
        match self:
            case DateTimeUnit.YEAR:
                return "'%Y'"
            case DateTimeUnit.QUARTER:
                raise ValueError("Quarter unit does not have an extraction string.")
            case DateTimeUnit.MONTH:
                return "'%m'"
            case DateTimeUnit.WEEK:
                return "'%w'"
            case DateTimeUnit.DAY:
                return "'%d'"
            case DateTimeUnit.HOUR:
                return "'%H'"
            case DateTimeUnit.MINUTE:
                return "'%M'"
            case DateTimeUnit.SECOND:
                return "'%S'"


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


def pad_helper(
    pad_func: str,
    args: list[SQLGlotExpression],
) -> SQLGlotExpression:
    """
    Helper function for LPAD and RPAD.
    Expects args[0] to be the column to pad.
    Expects args[1] and args[2] to be literals.
    Expects args[1] to be the returned length of the padded string.
    Expects args[2] to be the string to pad with.

    Args:
        `pad_func`: The name of the padding function to use.
        `args`: The operands passed to the function after they were converted
        to SQLGlot expressions. The first operand is expected to be a string.

    Returns:
        A tuple of sqlglot expressions for the column to pad, the length of the column,
        the required length, padding string and the integer literal of the required length.
    """
    assert pad_func in ["LPAD", "RPAD"]
    assert len(args) == 3

    if isinstance(args[1], sqlglot_expressions.Literal) and not args[1].is_string:
        try:
            required_len = int(args[1].this)
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

    if not isinstance(args[2], sqlglot_expressions.Literal) or not args[2].is_string:
        raise ValueError(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )
    if len(str(args[2].this)) != 1:
        raise ValueError(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )

    col_glot = args[0]
    col_len_glot = sqlglot_expressions.Length(this=args[0])
    required_len_glot = sqlglot_expressions.convert(required_len)
    pad_string_glot = sqlglot_expressions.convert(str(args[2].this) * required_len)
    return col_glot, col_len_glot, required_len_glot, pad_string_glot, required_len
