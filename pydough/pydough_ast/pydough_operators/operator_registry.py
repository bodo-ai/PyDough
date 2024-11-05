"""
TODO: add file-level docstring
"""

__all__ = ["builtin_registered_operators"]

from .operator_ast import PyDoughOperatorAST
from typing import MutableMapping
import pydough.pydough_ast.pydough_operators.expression_operators.registered_expression_operators as REP
import inspect


def builtin_registered_operators() -> MutableMapping[str, PyDoughOperatorAST]:
    """
    A dictionary of all registered operators pre-built from the PyDough source,
    where the key is the operator name and the value is the operator object.
    """
    operators: MutableMapping[str, PyDoughOperatorAST] = {}
    for name, obj in inspect.getmembers(REP):
        if name in REP.__all__:
            operators[name] = obj
    return operators
