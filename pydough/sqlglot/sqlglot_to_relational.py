"""
TODO
"""

__all__ = ["convert_sqlglot_to_relational"]


from sqlglot import expressions as sqlglot_expressions

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
)
from pydough.types import (
    BooleanType,
    DatetimeType,
    NumericType,
    StringType,
    UnknownType,
)


class GlotRelFail(Exception):
    """
    TODO
    """


def glot_to_rel(glot_expr: sqlglot_expressions.Expression) -> RelationalExpression:
    """
    TODO
    """

    # Convert all of the sub-expressions to a relational expression. This
    # step is stored in a "thunk" so it only happens under certain conditions.
    def sub_rels() -> list[RelationalExpression]:
        return [glot_to_rel(e) for e in glot_expr.iter_expressions()]

    match glot_expr:
        case sqlglot_expressions.TimeStrToTime():
            return CallExpression(
                pydop.DATETIME,
                DatetimeType(),
                sub_rels(),
            )
        case sqlglot_expressions.Literal():
            literal_expr = glot_expr.to_py()
            if isinstance(literal_expr, str):
                return LiteralExpression(literal_expr, StringType())
            elif isinstance(literal_expr, bool):
                return LiteralExpression(literal_expr, BooleanType())
            elif isinstance(literal_expr, int) or isinstance(literal_expr, float):
                return LiteralExpression(literal_expr, NumericType())
            else:
                raise GlotRelFail()
            return LiteralExpression(
                glot_expr.this,
                StringType(),
            )
        case sqlglot_expressions.Null():
            return LiteralExpression(None, UnknownType())
        case sqlglot_expressions.Lower():
            return CallExpression(pydop.LOWER, StringType(), sub_rels())
        case sqlglot_expressions.Upper():
            return CallExpression(pydop.UPPER, StringType(), sub_rels())
        case sqlglot_expressions.Length():
            return CallExpression(pydop.LENGTH, NumericType(), sub_rels())
        case sqlglot_expressions.Abs():
            return CallExpression(pydop.ABS, StringType(), sub_rels())
        case _:
            raise GlotRelFail()


def convert_sqlglot_to_relational(
    glot_expr: sqlglot_expressions.Expression,
) -> RelationalExpression | None:
    """
    Attempt to convert a sqlglot expression to a relational expression.

    Args:
        `glot_expr`: The sqlglot expression to convert.

    Returns:
        The converted relational expression, or None if the attempt failed.
    """
    try:
        return glot_to_rel(glot_expr)
    except GlotRelFail:
        return None
