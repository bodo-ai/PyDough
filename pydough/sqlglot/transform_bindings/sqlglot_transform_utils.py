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
from typing import TYPE_CHECKING, Union

import sqlglot.expressions as sqlglot_expressions
from pandas import Series
from sqlglot.expressions import Binary, Case, Concat, Is, Paren, Unary
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.errors import PyDoughSQLException
from pydough.sqlglot.sqlglot_helpers import normalize_column_name
from pydough.types import PyDoughType
from pydough.user_collections.dataframe_collection import DataframeGeneratedCollection
from pydough.user_collections.range_collection import RangeGeneratedCollection

if TYPE_CHECKING:
    from pydough.sqlglot.transform_bindings.base_transform_bindings import (
        BaseTransformBindings,
    )

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
                raise PyDoughSQLException(
                    "Quarter unit does not have a truncation string."
                )
            case DateTimeUnit.MONTH:
                return "'%Y-%m-01 00:00:00'"
            case DateTimeUnit.WEEK:
                raise PyDoughSQLException(
                    "Week unit does not have a truncation string."
                )
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
                raise PyDoughSQLException(
                    "Quarter unit does not have an extraction string."
                )
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
                raise PyDoughSQLException(
                    f"{pad_func} function requires the length argument to be a non-negative integer literal."
                )
        except ValueError:
            raise PyDoughSQLException(
                f"{pad_func} function requires the length argument to be a non-negative integer literal."
            )
    else:
        raise PyDoughSQLException(
            f"{pad_func} function requires the length argument to be a non-negative integer literal."
        )

    if not isinstance(args[2], sqlglot_expressions.Literal) or not args[2].is_string:
        raise PyDoughSQLException(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )
    if len(str(args[2].this)) != 1:
        raise PyDoughSQLException(
            f"{pad_func} function requires the padding argument to be a string literal of length 1."
        )

    col_glot = args[0]
    col_len_glot = sqlglot_expressions.Length(this=args[0])
    required_len_glot = sqlglot_expressions.convert(required_len)
    pad_string_glot = sqlglot_expressions.convert(str(args[2].this) * required_len)
    return col_glot, col_len_glot, required_len_glot, pad_string_glot, required_len


def expand_variance(
    args: list[SQLGlotExpression], types: list[PyDoughType], type: str
) -> SQLGlotExpression:
    """
    Converts a variance calculation (population or sample) to an equivalent
    SQLGlot expression.

    Args:
        `args`: The arguments to the variance function.
        `types`: The types of the arguments.
        `type`: The type of variance to calculate.

    Returns:
        The SQLGlot expression to calculate the population variance
        of the argument.
    """
    arg = args[0]
    # Formula: (SUM(X*X) - (SUM(X)*SUM(X) / COUNT(X))) / COUNT(X) for population variance
    # For sample variance, divide by (COUNT(X) - 1) instead of COUNT(X)

    # SUM(X*X)
    square_expr = apply_parens(
        sqlglot_expressions.Pow(
            this=arg, expression=sqlglot_expressions.Literal.number(2)
        )
    )
    sum_squares_expr = sqlglot_expressions.Sum(this=square_expr)

    # SUM(X)
    sum_expr = sqlglot_expressions.Sum(this=arg)

    # COUNT(X)
    count_expr = sqlglot_expressions.Count(this=arg)

    # (SUM(X)*SUM(X))
    sum_squared_expr = sqlglot_expressions.Pow(
        this=sum_expr, expression=sqlglot_expressions.Literal.number(2)
    )

    # ((SUM(X)*SUM(X)) / COUNT(X))
    mean_sum_squared_expr = apply_parens(
        sqlglot_expressions.Div(
            this=apply_parens(sum_squared_expr), expression=apply_parens(count_expr)
        )
    )

    # (SUM(X*X) - (SUM(X)*SUM(X) / COUNT(X)))
    numerator = sqlglot_expressions.Sub(
        this=sum_squares_expr, expression=apply_parens(mean_sum_squared_expr)
    )

    if type == "population":
        # Divide by COUNT(X)
        return apply_parens(
            sqlglot_expressions.Div(
                this=apply_parens(numerator), expression=apply_parens(count_expr)
            )
        )
    elif type == "sample":
        # Divide by (COUNT(X) - 1)
        denominator = sqlglot_expressions.Sub(
            this=count_expr, expression=sqlglot_expressions.Literal.number(1)
        )
        return apply_parens(
            sqlglot_expressions.Div(
                this=apply_parens(numerator), expression=apply_parens(denominator)
            )
        )
    else:
        raise ValueError(f"Unsupported type: {type}")


def expand_std(
    args: list[SQLGlotExpression], types: list[PyDoughType], type: str
) -> SQLGlotExpression:
    """
    Converts a standard deviation calculation to an equivalent
    SQLGlot expression.

    Args:
        `args`: The arguments to the standard deviation function.
        `types`: The types of the arguments.
        `type`: The type of standard deviation to calculate.

    Returns:
        The SQLGlot expression to calculate the standard deviation
        of the argument.
    """
    variance = expand_variance(args, types, type)
    return sqlglot_expressions.Pow(
        this=variance, expression=sqlglot_expressions.Literal.number(0.5)
    )


def rename_user_collection(
    known_collections: list[str],
    current_name: str,
) -> str:
    """
    Helper function to rename a user collection to avoid naming conflicts with other
    collections in the same query.

    Args:
    `known_collections`: The list of known collection names that are user-generated.
    `current_name`: The current name of the collection to potentially rename.

    Returns:
        A name for the collection that is unique among the known collections.
    """
    if current_name not in known_collections:
        # If the current name is not in the list of known collections, it's used
        # as it is.
        known_collections.append(current_name)
        return current_name
    else:
        # Otherwise, it's renamed by adding an index to the end of the name.
        idx: int = known_collections.count(current_name) + 1
        known_collections.append(current_name)
        return f"{current_name}_{idx}"


def create_constant_table(
    table_name: str,
    column_names: list[str],
    rows: list[SQLGlotExpression],
    transform_bindings: "BaseTransformBindings",
) -> SQLGlotExpression:
    """
    Generate a SQL that represents a constant table using the given list of
    columns and rows. The final SQLGlot expression corresponds to the SQL:
    When transform_bindings.values_alias_column is `True`:
    SELECT {column1 as column_names[0], ...} FROM ( VALUES {rows} ) AS {table_name}

    otherwise:
    SELECT {column_names} FROM ( VALUES {rows} ) AS table_name ({column_names})

    Args:
        `table_name`: The name of the table
        `column_names`: List with all column names
        `rows`: List of rows containg the data from the dataframe.
        `transform_bindings`: The tranform bindings class that called this function

    Returns:
        The SQLGlot expression for the constant table.

    Note: This function is used for MySQL and SQlite to generate range
    collections. And used for MySQL, SQlite, Snowflake and Postgres to generate
    dataframe collection.
    """
    assert column_names != []

    table_quoted, name_normalized = normalize_column_name(table_name)
    expr_table: SQLGlotExpression = sqlglot_expressions.Identifier(
        this=name_normalized, quoted=table_quoted
    )

    # Normalize the column names, deleting quotes and keeping track of which
    # column names were quoted to begin with.
    normalized_column_names: list[tuple[bool, str]] = [
        normalize_column_name(col) for col in column_names
    ]

    columns_list: list[SQLGlotExpression] = [
        sqlglot_expressions.Identifier(this=column, quoted=quoted)
        for quoted, column in normalized_column_names
    ]

    result: SQLGlotExpression

    if len(rows) == 0:
        # Example:
        # column_list = ['X', 'Y'] and types = ['INT', 'VARCHAR']
        # SELECT CAST(NULL AS INT) AS X, CAST(NULL AS VARCHAR) AS Y WHERE FALSE
        result = create_empty_constant_table(columns_list, ["INT"] * len(columns_list))
    else:
        # Create the VALUES expression
        values_expr: SQLGlotExpression = sqlglot_expressions.Values(expressions=rows)

        table_alias = sqlglot_expressions.TableAlias(
            this=expr_table, columns=columns_list
        )

        # Subquery with alias for the VALUES expression
        aliased_values = sqlglot_expressions.Subquery(
            this=values_expr, alias=table_alias
        )

        # List of columns to select
        select_columns: list[SQLGlotExpression] = []

        for idx, column in enumerate(columns_list):
            # Sqlite referred by default its values' columns as column1, column2
            # and so on. Aliases are used to rename those. Other dialects can
            # rename their columns directly.
            if transform_bindings.values_alias_column:
                column_enum: str = f"column{idx + 1}"
                select_columns.append(
                    sqlglot_expressions.Alias(
                        this=sqlglot_expressions.Column(this=column_enum), alias=column
                    )
                )
            else:
                # This is used for MySQL, Postgres and Snowflake
                select_columns.append(sqlglot_expressions.Column(this=column))

        result = sqlglot_expressions.Select(expressions=select_columns).from_(
            aliased_values
        )

    return result


def create_empty_constant_table(
    column_list: list[SQLGlotExpression], types: list[str]
) -> SQLGlotExpression:
    """
    Construct a SQLGlot expression representing an empty user-defined constant
    table with specified columns and types.

    For example, for column_list = ['X', 'Y'] and types = ['INT', 'VARCHAR'],
    the resulting SQLGlot expression corresponds to the SQL:
        SELECT CAST(NULL AS INT) AS X, CAST(NULL AS VARCHAR) AS Y
        WHERE FALSE

    Args:
        `column_list`: List of all the column names.
        `types`: List of types for each column respectively.

    Notes:
        - The length of column_list and types must match.

    Returns:
        The SQLGlot expression for an empty user collection.
    """

    assert len(column_list) == len(types)

    expressions: list[SQLGlotExpression] = []

    for idx, column in enumerate(column_list):
        expressions.append(
            sqlglot_expressions.Alias(
                this=sqlglot_expressions.Cast(
                    this=sqlglot_expressions.Null(),
                    to=sqlglot_expressions.DataType.build(types[idx]),
                ),
                alias=column,
            )
        )

    return sqlglot_expressions.Select(expressions=expressions).where(
        sqlglot_expressions.Boolean(this=False)
    )


def is_empty_range(collection: RangeGeneratedCollection) -> bool:
    """
    Helper function to determine if a range collection is empty.

    Args:
        `collection`: The RangeGeneratedCollection to check.
    Returns:
        True if the range is empty, False otherwise.
    """

    # Determine if the range is empty. An empty range occurs when:
    # - step > 0 and start >= end
    # - step < 0 and start <= end
    # - step == 0
    return (
        (collection.step > 0 and collection.start >= collection.end)
        or (collection.step < 0 and collection.start <= collection.end)
        or (collection.step == 0)
    )


def generate_range_rows(
    collection: RangeGeneratedCollection,
    transform_bindings: "BaseTransformBindings",
) -> list[SQLGlotExpression]:
    """
    Helper function to generate the rows for a given range collection

    Args:
        `collection`: The RangeGeneratedCollection to check.
        `transform_bindings`: The tranform bindings class that called this function

    Returns:
        List of sqlglot expressions for the range, empty if it is an empty range
    """

    if is_empty_range(collection):
        return []

    range_rows: list[SQLGlotExpression] = [
        # [(i), ... ]
        sqlglot_expressions.Tuple(expressions=[sqlglot_expressions.Literal.number(i)])
        if transform_bindings.values_tuple
        # [ROW(i), ... ]
        else sqlglot_expressions.Anonymous(
            this="ROW", expressions=[sqlglot_expressions.Literal.number(i)]
        )
        for i in range(collection.start, collection.end, collection.step)
    ]

    return range_rows


def generate_dataframe_rows(
    collection: DataframeGeneratedCollection,
    transform_bindings: "BaseTransformBindings",
) -> list[SQLGlotExpression]:
    """
    Helper function to generate the rows for a given dataframe collection

    Args:
        `collection`: The dataframe collection for generate the rows.
        `transform_bindings`: The tranform bindings class that called this function

    Returns:
        List of sqlglot expressions for the dataframe.

    SQL Example:
        Using item[row].[column], it generates the follwing list.
        If transform_bindings.values_tuple is True the list will look like
        this:
            list[(item1.1, item1.2, ...), (item2.1, item2.2, ...), ...]

        otherwise it will look like:
            list[ROW(item1.1, item1.2, ...), ROW(item2.1, item2.2, ...), ...]
    """
    dataframe_rows: list[SQLGlotExpression] = []

    for i in range(len(collection.dataframe)):
        # For each row
        row: Series = collection.dataframe.iloc[i]

        # Contain the items (data) in the expression
        # Example: sqlglot.Literal.number(data) for a numeric type
        expr_list: list[SQLGlotExpression] = []

        for type_idx, item in enumerate(row):
            expr_list.append(
                transform_bindings.generate_dataframe_item_expression(
                    item, collection.types[type_idx]
                )
            )

        # Append the row with its items to the final result
        dataframe_rows.append(
            # [(i), ... ]
            sqlglot_expressions.Tuple(expressions=expr_list)
            if transform_bindings.values_tuple
            # [ROW(i), ... ]
            else sqlglot_expressions.Anonymous(this="ROW", expressions=expr_list)
        )

    return dataframe_rows
