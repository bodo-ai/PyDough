"""
Finds all of the identifiers associate with a SQLGlot
expression.
"""

from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier

__all__ = ["find_identifiers"]


def _visit_expression(expr: SQLGlotExpression, identifiers: set[Identifier]) -> None:
    """
    Visits a SQLGlotExpression to try find any identifiers.

    Args:
        expr (SQLGlotExpression): An expression.
        identifiers (set[Identifier]): The set of identifiers
            that have been encountered so far.
    """
    if isinstance(expr, Identifier):
        identifiers.add(expr)
    else:
        for arg in expr.args.values():
            if isinstance(arg, SQLGlotExpression):
                _visit_expression(arg, identifiers)


def find_identifiers(expr: SQLGlotExpression):
    output: set[Identifier] = set()
    _visit_expression(expr, output)
    return output
