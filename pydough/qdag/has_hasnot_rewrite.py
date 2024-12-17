"""
Definition of the PyDough QDAG expression rewrite step that handles HAS and
HASNOT.
"""

__all__ = ["has_hasnot_rewrite"]


from pydough.types import Int64Type

from .abstract_pydough_qdag import PyDoughQDAG
from .expressions import (
    ExpressionFunctionCall,
    Literal,
    PyDoughExpressionQDAG,
)


def has_hasnot_rewrite(
    exp: PyDoughExpressionQDAG, allow_has_hasnot: bool
) -> PyDoughExpressionQDAG:
    """
    Recursively transforms a PyDough expression QDAG node to achieve the
    following goals:
    - Flatten all conjunctions, e.g. `(x & y) & (a & b) -> (x & y & a & b)`
    - Rewrite all `HAS(X)` as `COUNT(X) > 0` unless in the conjunction of a
    `WHERE` clause.
    - Rewrite all `HASNOT(X)` as `COUNT(X) == 0` unless in the conjunction of a
    `WHERE` clause.

    Args:
        - `exp`: the PyDough expression node being transformed.
        - `allow_has_hasnot`: whether the call is being done in the conjunction
        of a `WHERE` clause, meaning that `HAS` and `HASNOT` should not be
        rewritten. This should only be True when the original callsite is a
        `WHERE` clause.

    Returns:
        The transformed PyDough expression QDAG node.
    """
    from pydough.pydough_operators import (
        BAN,
        COUNT,
        EQU,
        GRT,
        HAS,
        HASNOT,
    )

    if isinstance(exp, ExpressionFunctionCall):
        new_args: list[PyDoughQDAG] = []
        if exp.operator in (HAS, HASNOT) and not allow_has_hasnot:
            # Rewrite HAS and HASNOT into COUNT comparisons unless we are
            # still in the conjunction of a WHERE clause
            cmp_op = GRT if exp.operator == HAS else EQU
            return ExpressionFunctionCall(
                cmp_op,
                [ExpressionFunctionCall(COUNT, exp.args), Literal(0, Int64Type())],
            )
        elif exp.operator == BAN:
            # When processing an AND call, flatten its children that are also
            # AND calls, and process their arguments as if we were still in a
            # conjunction.
            for arg in exp.args:
                arg = (
                    has_hasnot_rewrite(arg, allow_has_hasnot)
                    if isinstance(arg, PyDoughExpressionQDAG)
                    else arg
                )
                if isinstance(arg, ExpressionFunctionCall) and arg.operator == BAN:
                    new_args.extend(arg.args)
                else:
                    new_args.append(arg)
            return ExpressionFunctionCall(BAN, new_args)
        else:
            # For any other function call, just recursively transform any
            # arguments that are expressions, but disable HAS/HASNOT since the
            # function call means we are no longer inside of a conjunction.
            for arg in exp.args:
                if isinstance(arg, PyDoughExpressionQDAG):
                    new_args.append(has_hasnot_rewrite(arg, False))
                else:
                    new_args.append(arg)
            return ExpressionFunctionCall(exp.operator, new_args)
    else:
        # Anything except a function call is un-transformed.
        return exp
