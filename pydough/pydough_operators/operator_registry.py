"""
Suite where all registered operators are accessible as a combined unit.
"""

__all__ = ["builtin_registered_operators", "get_operator_by_name"]

import inspect

from .base_operator import PyDoughOperator
from .expression_operators import (
    ExpressionFunctionOperator,
    KeywordBranchingExpressionFunctionOperator,
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


def get_operator_by_name(name: str, **kwargs) -> ExpressionFunctionOperator:
    """
    Get an operator by name.
    """
    from pydough.unqualified import PyDoughUnqualifiedException

    # Find the operator directly using inspect
    for op_name, obj in inspect.getmembers(REP):
        if op_name == name and op_name in REP.__all__ and obj.public:
            operator = obj
            break
    else:
        raise PyDoughUnqualifiedException(f"Operator {name} not found.")

    # Check if this is a keyword branching operator
    if isinstance(operator, KeywordBranchingExpressionFunctionOperator):
        # Find the matching implementation based on kwargs
        impl = operator.find_matching_implementation(kwargs)
        if impl is None:
            kwarg_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
            raise PyDoughUnqualifiedException(
                f"No matching implementation found for {name}({kwarg_str})."
            )
        return impl
    elif len(kwargs) > 0:
        raise PyDoughUnqualifiedException(
            f"PyDough function call {name} does not support "
            "keyword arguments at this time."
        )

    return operator
