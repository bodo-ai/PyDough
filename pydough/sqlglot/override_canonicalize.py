"""
Overridden version of the canonicalize.py file from sqlglot/optimizer.
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.optimizer.canonicalize import (
    _replace_int_predicate,
    add_text_to_concat,
    coerce_type,
    ensure_bools,
    remove_ascending_order,
    remove_redundant_casts,
    replace_date_funcs,
)


def canonicalize(
    expression: exp.Expression, dialect: DialectType = None
) -> exp.Expression:
    """Converts a sql expression into a standard form.

    This method relies on annotate_types because many of the
    conversions rely on type inference.

    Args:
        expression: The expression to canonicalize.
    """

    dialect = Dialect.get_or_raise(dialect)

    def _canonicalize(expression: exp.Expression) -> exp.Expression:
        expression = fix_expression_type(expression, dialect)
        expression = add_text_to_concat(expression)
        expression = replace_date_funcs(expression)
        expression = coerce_type(expression, dialect.PROMOTE_TO_INFERRED_DATETIME_TYPE)
        expression = remove_redundant_casts(expression)
        expression = ensure_bools(expression, _replace_int_predicate)
        expression = remove_ascending_order(expression)
        return expression

    return exp.replace_tree(expression, _canonicalize)


def fix_expression_type(node: exp.Expression, dialect: Dialect) -> exp.Expression:
    if dialect.__class__.__name__ != "Postgres":
        return node

    if isinstance(node, exp.Sub) and (
        node.this.type.this == exp.DataType.Type.TIMESTAMP
        or node.expression.type.this == exp.DataType.Type.TIMESTAMP
        or isinstance(node.this, exp.TimestampTrunc)
        or isinstance(node.expression, exp.TimestampTrunc)
    ):
        # Replace node.type to timestamp to prevent cast in date's sub
        # it verifies if node.this or node.expression are type TIMESTAMP or
        # TimestampTrunc
        node.type.set("this", exp.DataType.Type.TIMESTAMP)

    if isinstance(node, exp.Mul) and (
        isinstance(node.this, exp.Window) or isinstance(node.expression, exp.Window)
    ):
        node.type.set("this", exp.DataType.Type.INT)

    return node
