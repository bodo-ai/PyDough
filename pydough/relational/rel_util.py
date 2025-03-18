""" """

from relational_expressions import (
    CallExpression,
    RelationalExpression,
)

import pydough.pydough_operators as pydop


def get_conjunctions(expr: RelationalExpression) -> set[RelationalExpression]:
    """
    Extract conjunctions from the given expression.
    """
    if isinstance(expr, CallExpression) and expr.op == pydop.BAN:
        result = set()
        for arg in expr.inputs:
            result.update(get_conjunctions(arg))
    return {expr}
