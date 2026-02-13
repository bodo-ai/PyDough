"""
Suite where all registered operators are accessible as a combined unit.
"""

__all__ = ["builtin_registered_operators", "get_operator_by_name"]

import inspect

from pydough.errors import PyDoughUnqualifiedException

from .base_operator import PyDoughOperator
from .expression_operators import (
    ExpressionFunctionOperator,
)
from .expression_operators import registered_expression_operators as REP


def builtin_registered_operators() -> dict[str, PyDoughOperator]:
    """
    A dictionary of all registered operators pre-built from the PyDough source,
    where the key is the operator name and the value is the operator object.
    """
    operators: dict[str, PyDoughOperator] = {}
    for name, obj in inspect.getmembers(REP):
        if name in REP.__all__ and obj.public:
            operators[name] = obj
    return operators


def get_operator_by_name(name: str) -> ExpressionFunctionOperator:
    """
    Retrieves a registered PyDough operator by its a name.

    Args:
        name: The name of the operator to retrieve.

    Returns:
        The `ExpressionFunctionOperator` corresponding to the given name.

    Raises:
        `PyDoughUnqualifiedException`: If the operator with the given name is
        not found.
    """

    # Find the operator directly using inspect
    for op_name, obj in inspect.getmembers(REP):
        if op_name == name and op_name in REP.__all__ and obj.public:
            return obj
    # If not found, raise an exception
    raise PyDoughUnqualifiedException(f"Operator {name} not found.")
