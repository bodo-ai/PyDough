"""
File that handles conversion of call expressions to SQLGlot expressions
when the translation is not straightforward, possibly due to a gap in
SQLGlot.
"""

import sqlglot.expressions as sqlglot_expressions
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


def convert_concat(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Support for generating a CONCAT expression from a list of arguments.
    This is optimized for the case where all arguments are string literals
    because it impacts the quality of the generated SQL for common cases.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments to
            concatenate.

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


def convert_like(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Support for generating a LIKE expression from a list of arguments.
    This is given a function because it is a conversion target.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of like.
    """
    column: SQLGlotExpression = apply_parens(arguments[0])
    pattern: SQLGlotExpression = apply_parens(arguments[1])
    return sqlglot_expressions.Like(this=column, expression=pattern)


def convert_startswith(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Convert a STARTSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert STARTSWITH
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of startswith.
    """
    column: SQLGlotExpression = arguments[0]
    pattern: SQLGlotExpression = convert_concat(
        [sqlglot_expressions.convert("%"), arguments[1]]
    )
    return convert_like([column, pattern])


def convert_endswith(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Convert a ENDSWITH call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert ENDSWITH
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of endswith.
    """
    column: SQLGlotExpression = arguments[0]
    pattern: SQLGlotExpression = convert_concat(
        [arguments[1], sqlglot_expressions.convert("%")]
    )
    return convert_like([column, pattern])


def convert_contains(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Convert a CONTAINS call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert CONTAINS
    to a LIKE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

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
        ]
    )
    return convert_like([column, pattern])


def convert_isin(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Convert a ISIN call expression to a SQLGlot expression. This
    is done because converting to IN is non-standard.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of isin.
    """
    column: SQLGlotExpression = apply_parens(arguments[0])
    # Note: We only handle the case with multiple literals where all
    # literals are in the same literal expression.
    values: SQLGlotExpression = arguments[1]
    return sqlglot_expressions.In(this=column, expressions=values)


def convert_iff(arguments: list[SQLGlotExpression]) -> SQLGlotExpression:
    """
    Convert a IFF call expression to a SQLGlot expression. This
    is done because SQLGlot does not automatically convert IFF
    to a CASE expression for SQLite.

    Args:
        arguments (list[SQLGlotExpression]): The list of arguments.

    Returns:
        SQLGlotExpression: The SQLGlot expression matching the functionality
            of iff.
    """
    condition: SQLGlotExpression = arguments[0]
    true_expr: SQLGlotExpression = arguments[1]
    false_expr: SQLGlotExpression = arguments[2]
    return (
        sqlglot_expressions.Case()
        .when(condition=condition, then=true_expr)
        .else_(false_expr)
    )
