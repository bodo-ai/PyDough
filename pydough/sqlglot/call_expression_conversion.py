"""
File that handles conversion of call expressions to SQLGlot expressions
when the translation is not straightforward, possibly due to a gap in
SQLGlot.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.dialects.sqlite import SQLite as SQLiteDialect
from sqlglot.expressions import Binary, Concat, Paren
from sqlglot.expressions import Expression as SQLGlotExpression


def apply_parens(expression: SQLGlotExpression) -> SQLGlotExpression:
    """
    Determine when due to the next SQL operator not using standard
    function syntax, we may need to apply parentheses to the current
    expression to avoid operator precedence issues.

    Args:
        expression (SQLGlotExpression): The expression to check.

    Returns:
        SQLGlotExpression: The expression with parentheses applied if
            necessary.
    """
    if isinstance(expression, (Binary, Concat)):
        return Paren(this=expression)
    else:
        return expression


def convert_concat(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Support for generating a CONCAT expression from a list of arguments.
    This is optimized for the case where all arguments are string literals
    because it impacts the quality of the generated SQL for common cases.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments to
            concatenate.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: A CONCAT expression or equivalent string literal.
    """
    # Fast path for all arguments as string literals.
    if all(
        isinstance(arg, sqlglot_expressions.Literal) and arg.is_string
        for arg in arguments
    ):
        return sqlglot_expressions.convert("".join(arg.this for arg in arguments))
    else:
        inputs: list[SQLGlotExpression] = [apply_parens(arg) for arg in arguments]
        return Concat(expressions=inputs)


def convert_like(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Support for generating a LIKE expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of like.
    """
    column: SQLGlotExpression = apply_parens(arguments[0])
    pattern: SQLGlotExpression = apply_parens(arguments[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a STARTSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert STARTSWITH
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of startswith.
    """
    column: SQLGlotExpression = arguments[0]
    pattern: SQLGlotExpression = convert_concat(
        [arguments[1], sqlglot_expressions.convert("%")],
        dialect,
    )
    return convert_like([column, pattern], dialect)


def convert_endswith(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a ENDSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert ENDSWITH
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of endswith.
    """
    column: SQLGlotExpression = arguments[0]
    pattern: SQLGlotExpression = convert_concat(
        [sqlglot_expressions.convert("%"), arguments[1]],
        dialect,
    )
    return convert_like([column, pattern], dialect)


def convert_contains(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a CONTAINS call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert CONTAINS
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of contains.
    """
    # TODO: Update when contains maps to multiple functions (e.g. ARRAY_CONTAINS).
    column: SQLGlotExpression = arguments[0]
    pattern: SQLGlotExpression = convert_concat(
        [
            sqlglot_expressions.convert("%"),
            arguments[1],
            sqlglot_expressions.convert("%"),
        ],
        dialect,
    )
    return convert_like([column, pattern], dialect)


def convert_isin(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a ISIN call expression to a SQLGlot expression. This
    is done because converting to IN is non-standard.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of isin.
    """
    column: SQLGlotExpression = apply_parens(arguments[0])
    # Note: We only handle the case with multiple literals where all
    # literals are in the same literal expression. This code will need
    # to change when we support PyDough expressions like:
    # Collection.WHERE(ISIN(name, plural_subcollection.name))
    values: SQLGlotExpression = arguments[1]
    return sqlglot_expressions.In(this=column, expressions=values)


def _convert_sqlite_datetime(
    column: SQLGlotExpression, format_str: str
) -> SQLGlotExpression:
    """
    Generate a SQLite-compatible datetime conversion expression for the given
    format string.

    Args:
        column (SQLGlotExpression): The column to convert.
        format_str (str): The format string.
    """
    return sqlglot_expressions.Cast(
        this=sqlglot_expressions.TimeToStr(this=column, format=format_str),
        to=sqlglot_expressions.DataType(this=sqlglot_expressions.DataType.Type.INT),
    )


def convert_year(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a YEAR call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert YEAR
    to equivalent SQL operation in SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of year.
    """
    column: SQLGlotExpression = arguments[0]
    if isinstance(dialect, SQLiteDialect):
        return _convert_sqlite_datetime(column, "'%Y'")
    else:
        return sqlglot_expressions.Year(this=column)


def convert_month(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a MONTH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert MONTH
    to equivalent SQL operation in SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of month.
    """
    column: SQLGlotExpression = arguments[0]
    if isinstance(dialect, SQLiteDialect):
        return _convert_sqlite_datetime(column, "'%m'")
    else:
        return sqlglot_expressions.Year(this=column)


def convert_day(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a DAY call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert DAY
    to equivalent SQL operation in SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of day.
    """
    column: SQLGlotExpression = arguments[0]
    if isinstance(dialect, SQLiteDialect):
        return _convert_sqlite_datetime(column, "'%d'")
    else:
        return sqlglot_expressions.Year(this=column)


def convert_ndistinct(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Convert a NDISTINCT call expression to a SQLGlot expression.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of NDISTINCT.
    """
    column: SQLGlotExpression = arguments[0]
    return sqlglot_expressions.Count(
        this=sqlglot_expressions.Distinct(expressions=[column])
    )


def convert_if(
    arguments: list[SQLGlotExpression], dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Converts an IFF call expression to a SQLGlot expression. This is done
    because older versions of SQLite (such as those in github codespaces)
    require the use of the CASE statement.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality.
    """
    if isinstance(dialect, SQLiteDialect):
        import sqlite3

        if sqlite3.sqlite_version < "3.32":
            # TODO: Enable an explicit test via an environment variable.
            return (
                sqlglot_expressions.Case()
                .when(arguments[0], arguments[1])
                .else_(arguments[2])
            )
    return sqlglot_expressions.If.from_arg_list(arguments)
