""" """

import pydough.pydough_operators as pydop

from .relational_expressions import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
)


def get_conjunctions(expr: RelationalExpression) -> set[RelationalExpression]:
    """
    Extract conjunctions from the given expression.
    """
    if isinstance(expr, LiteralExpression) and expr.value:
        return set()
    if isinstance(expr, CallExpression) and expr.op == pydop.BAN:
        result = set()
        for arg in expr.inputs:
            result.update(get_conjunctions(arg))
        return result
    return {expr}
