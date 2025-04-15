"""
Suite where all registered operators are accessible as a combined unit.
"""

__all__ = ["builtin_registered_operators"]

import inspect

import pydough.pydough_operators.expression_operators.registered_expression_operators as REP

from .base_operator import PyDoughOperator


def builtin_registered_operators() -> dict[str, PyDoughOperator]:
    """
    A dictionary of all registered operators pre-built from the PyDough source,
    where the key is the operator name and the value is the operator object.
    """
    operators: dict[str, PyDoughOperator] = {}
    for name, obj in inspect.getmembers(REP):
        if name in REP.__all__:
            operators[name] = obj
    return operators
