"""
This file contains functionality for interacting with SQLGlot expressions
that can act as wrappers around the internal implementation of SQLGlot.
"""

import sqlglot.expressions as sqlglot_expressions
from sqlglot.expressions import Alias as SQLGlotAlias
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier

__all__ = ["get_glot_name", "is_boolean_expression", "set_glot_alias", "unwrap_alias"]


def get_glot_name(expr: SQLGlotExpression) -> str | None:
    """
    Get the name of a SQLGlot expression. If the expression has an alias,
    return the alias. Otherwise, return the name of any identifier. If
    an expression has neither, return None.

    Args:
        `expr`: The expression to get the name of.

    Returns:
        The name of the expression or None if no name is found.
    """
    if expr.alias:
        return expr.alias
    elif isinstance(expr, Identifier):
        return expr.this
    else:
        return None


def set_glot_alias(expr: SQLGlotExpression, alias: str | None) -> SQLGlotExpression:
    """
    Returns the SQLGlot expression with an alias via the
    as functionality. If the alias already matches the name of the
    expression, then we do not modify the expression. This is not
    guaranteed to copy the original expression or avoid modifying
    the original expression.

    Args:
        `expr`: The expression to update.
        `alias`: The alias to set.

    Returns:
        The updated expression.
    """
    if alias is None:
        return expr
    old_name = get_glot_name(expr)
    if old_name == alias:
        return expr
    else:
        return expr.as_(alias)


def unwrap_alias(expr: SQLGlotExpression) -> SQLGlotExpression:
    """
    Unwraps an alias from a SQLGlot expression. If the expression
    is an alias, return the inner expression. Otherwise, return the
    original expression.

    Args:
        `expr`: The expression to unwrap.

    Returns:
        The unwrapped expression.
    """
    return expr.this if isinstance(expr, SQLGlotAlias) else expr


def is_boolean_expression(expr: SQLGlotExpression) -> bool:
    """Check if the given expression is a boolean expression.

    Args:
        expr The expression to check.

    Returns:
        True if the expression is a boolean expression, False otherwise.
    """
    boolean_types = (
        sqlglot_expressions.In,
        sqlglot_expressions.EQ,
        sqlglot_expressions.NEQ,
        sqlglot_expressions.GT,
        sqlglot_expressions.GTE,
        sqlglot_expressions.LT,
        sqlglot_expressions.LTE,
        sqlglot_expressions.And,
        sqlglot_expressions.Or,
        sqlglot_expressions.Not,
        sqlglot_expressions.Between,
        sqlglot_expressions.Like,
        sqlglot_expressions.ILike,
        sqlglot_expressions.Is,
    )
    if isinstance(expr, boolean_types):
        return True

    return False
