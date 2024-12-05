"""
TODO: add file-level docstring
"""

__all__ = ["has_hasnot_rewrite"]


from pydough.types import Int64Type

from .abstract_pydough_ast import PyDoughAST
from .expressions import (
    ExpressionFunctionCall,
    Literal,
    PyDoughExpressionAST,
)
from .pydough_operators import (
    BAN,
    COUNT,
    EQU,
    GRT,
    HAS,
    HASNOT,
)


def has_hasnot_rewrite(
    exp: PyDoughExpressionAST, allow_has_hasnot: bool
) -> PyDoughExpressionAST:
    """
    TODO
    """
    if isinstance(exp, ExpressionFunctionCall):
        new_args: list[PyDoughAST] = []
        if exp.operator in (HAS, HASNOT) and not allow_has_hasnot:
            cmp_op = GRT if exp.operator == HAS else EQU
            return ExpressionFunctionCall(
                cmp_op,
                [ExpressionFunctionCall(COUNT, exp.args), Literal(0, Int64Type())],
            )
        elif exp.operator == BAN:
            for arg in exp.args:
                arg = (
                    has_hasnot_rewrite(arg, allow_has_hasnot)
                    if isinstance(arg, PyDoughExpressionAST)
                    else arg
                )
                if isinstance(arg, ExpressionFunctionCall) and arg.operator == BAN:
                    new_args.extend(arg.args)
                else:
                    new_args.append(arg)
            return ExpressionFunctionCall(BAN, new_args)
        else:
            for arg in exp.args:
                if isinstance(arg, PyDoughExpressionAST):
                    new_args.append(has_hasnot_rewrite(arg, False))
                else:
                    new_args.append(arg)
            return ExpressionFunctionCall(exp.operator, new_args)
    else:
        return exp
