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
        expression = fix_expression_type(expression)
        expression = add_text_to_concat(expression)
        expression = replace_date_funcs(expression)
        expression = coerce_type(expression, dialect.PROMOTE_TO_INFERRED_DATETIME_TYPE)
        expression = remove_redundant_casts(expression)
        expression = ensure_bools(expression, _replace_int_predicate)
        expression = remove_ascending_order(expression)
        return expression

    return exp.replace_tree(expression, _canonicalize)


def fix_expression_type(node: exp.Expression) -> exp.Expression:
    if (
        isinstance(node, exp.Extract)
        and isinstance(node.expression, exp.Anonymous)
        and node.expression.this == "AGE"
    ):
        # Replace node.expression using a new expression with the updated type
        new_expression: exp.Expression = node.expression.copy()
        new_expression.type = exp.DataType.Type.TIMESTAMP
        node.expression.replace(new_expression)

    return node
