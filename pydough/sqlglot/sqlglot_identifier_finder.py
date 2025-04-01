"""
Finds all of the identifiers associate with a SQLGlot
expression.
"""

from sqlglot.expressions import Expression as SQLGlotExpression
from sqlglot.expressions import Identifier

__all__ = ["find_identifiers", "find_identifiers_in_list"]


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
            if isinstance(arg, list):
                for item in arg:
                    if isinstance(item, SQLGlotExpression):
                        _visit_expression(item, identifiers)


def find_identifiers(expr: SQLGlotExpression) -> set[Identifier]:
    """
    Find all the unique identifiers in a SQLGlot expression.

    Args:
        expr (SQLGlotExpression): The SQLGlotExpression to search

    Returns:
        set[Identifier]: The set of unique identifiers found in the
            expression.
    """
    output: set[Identifier] = set()
    _visit_expression(expr, output)
    return output


def find_identifiers_in_list(exprs: list[SQLGlotExpression]) -> set[Identifier]:
    """
    Find all unique identifiers in a list of SQLGlot expressions.

    Args:
        exprs (list[SQLGlotExpression]): A list of SQLGlot expressions
            to search.

    Returns:
        set[Identifier]: The set of unique identifiers found in the
            expressions.
    """
    output: set[Identifier] = set()
    for expr in exprs:
        _visit_expression(expr, output)
    return output
