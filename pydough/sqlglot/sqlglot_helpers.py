"""
This file contains functionality for interacting with SQLGlot expressions
that can act as wrappers around the internal implementation of SQLGlot.
"""

from sqlglot.expressions import (
    Alias as SQLGlotAlias,
)
from sqlglot.expressions import (
    Column,
    Identifier,
    Window,
    maybe_copy,
    maybe_parse,
)
from sqlglot.expressions import Expression as SQLGlotExpression

__all__ = ["get_glot_name", "set_glot_alias", "unwrap_alias"]


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
        quoted: bool = False

        if (alias.startswith('"') and alias.endswith('"')) or (
            alias.startswith("`") and alias.endswith("`")
        ):
            alias = alias[1:-1]
            alias = alias.replace('""', '"')
            alias = alias.replace("``", "`")

            if isinstance(expr, Identifier):
                quoted = expr.args.get("quoted", False)
            elif isinstance(expr, Column):
                quoted = expr.this.args.get("quoted", False)

        return generate_glot_alias(expr, alias, quoted=quoted)


def generate_glot_alias(
    expr: SQLGlotExpression, alias: str, quoted: bool
) -> SQLGlotAlias:
    """
    Generates a SQLGlot Alias expression for the given expression
    and alias.

    This is the overridden and simplified version of sqlglot.expressions.alias().

    Args:
        `expr`: The expression to wrap in an alias.
        `alias`: The alias to use.

    Returns:
        The generated Alias expression.
    """
    exp = maybe_parse(expr, dialect=None, copy=True)
    alias = generate_identifier(alias, quoted=quoted)

    # Part of this code is ommitted because por this particular case the table
    # argument is not provided and not needed. The omitted code handles
    # the case where a table argument is provided to set column aliases.
    # if table: ...

    # We don't set the "alias" arg for Window expressions, because that would add an IDENTIFIER node in
    # the AST, representing a "named_window" [1] construct (eg. bigquery). What we want is an ALIAS node
    # for the complete Window expression.
    #
    # [1]: https://cloud.google.com/bigquery/docs/reference/standard-sql/window-function-calls

    if "alias" in exp.arg_types and not isinstance(exp, Window):
        exp.set("alias", alias)
        return exp

    return SQLGlotAlias(this=exp, alias=alias)


def generate_identifier(name, quoted=None, copy=True):
    """
    Generates a SQLGlot Identifier expression for the given name.

    This is the overridden of sqlglot.expressions.to_identifier(). This function
    simplifies the original by removing the SAFE_IDENTIFIER_RE check.

    Args:
        name: The name to turn into an identifier.
        quoted: Whether to force quote the identifier.
        copy: Whether to copy name if it's an Identifier.

    Returns:
        The identifier ast node.
    """
    if name is None:
        return None

    if isinstance(name, Identifier):
        identifier = maybe_copy(name, copy)
    elif isinstance(name, str):
        identifier = Identifier(
            this=name,
            # originally was:
            # quoted=not SAFE_IDENTIFIER_RE.match(name) if quoted is None else quoted,
            quoted=quoted if quoted is not None else False,
        )
    else:
        raise ValueError(
            f"Name needs to be a string or an Identifier, got: {name.__class__}"
        )
    return identifier


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
