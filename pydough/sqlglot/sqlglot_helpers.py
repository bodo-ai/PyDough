"""
This file contains functionality for interacting with SQLGlot expressions
that can act as wrappers around the internal implementation of SQLGlot.
"""

from sqlglot.expressions import Alias as SQLGlotAlias
from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier

__all__ = ["get_glot_name", "set_glot_alias", "unwrap_alias"]


def get_glot_name(expr: SQLGlotExpression) -> str | None:
    """
    Get the name of a SQLGlot expression. If the expression has an alias,
    return the alias. Otherwise, return the name of any identifier. If
    an expression has neither, return None.

    Args:
        expr (SQLGlotExpression): The expression to get the name of.

    Returns:
        str | None: The name of the expression or None if no name is found.
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
        expr (SQLGlotExpression): The expression to update.
        alias (str | None): The alias to set.

    Returns:
        SQLGlotExpression: The updated expression.
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
        expr (SQLGlotExpression): The expression to unwrap.

    Returns:
        SQLGlotExpression: The unwrapped expression.
    """
    return expr.this if isinstance(expr, SQLGlotAlias) else expr
