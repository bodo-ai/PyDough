"""
TODO
"""

__all__ = ["MaskLiteralComparisonShuttle"]

from sqlglot import expressions as sqlglot_expressions
from sqlglot import parse_one

import pydough.pydough_operators as pydop
from pydough.configs import PyDoughConfigs
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
)
from pydough.sqlglot import convert_sqlglot_to_relational

from .relational_simplification import SimplificationShuttle


class MaskLiteralComparisonShuttle(RelationalExpressionShuttle):
    """
    TODO
    """

    def __init__(self, configs: PyDoughConfigs):
        self.simplifier: SimplificationShuttle = SimplificationShuttle(configs)

    def simplify_masked_literal(
        self, value: RelationalExpression
    ) -> RelationalExpression:
        """
        TODO
        """
        if (
            not isinstance(value, CallExpression)
            or not isinstance(value.op, pydop.MaskedExpressionFunctionOperator)
            or value.op.is_unprotect
            or len(value.inputs) != 1
            or not isinstance(value.inputs[0], LiteralExpression)
        ):
            return value
        try:
            arg_sql_str: str = sqlglot_expressions.convert(value.inputs[0].value).sql()
            total_sql_str: str = value.op.format_string.format(arg_sql_str)
            glot_expr: sqlglot_expressions.Expression = parse_one(total_sql_str)
            new_expr: RelationalExpression | None = convert_sqlglot_to_relational(
                glot_expr
            )
            if new_expr is not None:
                return new_expr
            self.simplifier.reset()
            return value.accept_shuttle(self.simplifier)
        except Exception:
            return value

        return value

    def protect_literal_comparison(
        self,
        original_call: CallExpression,
        call_arg: CallExpression,
        literal_arg: LiteralExpression,
    ) -> CallExpression:
        """
        TODO
        """
        if (
            not isinstance(call_arg.op, pydop.MaskedExpressionFunctionOperator)
            or not call_arg.op.is_unprotect
        ):
            return original_call

        masked_literal: RelationalExpression

        if original_call.op in (pydop.EQU, pydop.NEQ):
            masked_literal = CallExpression(
                pydop.MaskedExpressionFunctionOperator(
                    call_arg.op.masking_metadata, False
                ),
                call_arg.data_type,
                [literal_arg],
            )
            masked_literal = self.simplify_masked_literal(masked_literal)
        elif original_call.op == pydop.ISIN and isinstance(
            literal_arg.value, (list, tuple)
        ):
            new_elems: list[RelationalExpression] = []
            for val in literal_arg.value:
                new_val: RelationalExpression = CallExpression(
                    pydop.MaskedExpressionFunctionOperator(
                        call_arg.op.masking_metadata, False
                    ),
                    call_arg.data_type,
                    [LiteralExpression(val, literal_arg.data_type)],
                )
                new_val = self.simplify_masked_literal(new_val)
                new_elems.append(new_val)

            masked_literal = LiteralExpression(new_elems, original_call.data_type)
        else:
            return original_call

        return CallExpression(
            original_call.op,
            original_call.data_type,
            [call_arg.inputs[0], masked_literal],
        )

    def visit_call_expression(
        self, call_expression: CallExpression
    ) -> RelationalExpression:
        if call_expression.op in (pydop.EQU, pydop.NEQ):
            # UNMASK(expr) = literal  -->  expr = MASK(literal)
            # UNMASK(expr) != literal  -->  expr != MASK(literal)
            if isinstance(call_expression.inputs[0], CallExpression) and isinstance(
                call_expression.inputs[1], LiteralExpression
            ):
                call_expression = self.protect_literal_comparison(
                    call_expression,
                    call_expression.inputs[0],
                    call_expression.inputs[1],
                )
            # literal = UNMASK(expr)  -->  MASK(literal) = expr
            # literal != UNMASK(expr)  -->  MASK(literal) != expr
            if isinstance(call_expression.inputs[1], CallExpression) and isinstance(
                call_expression.inputs[0], LiteralExpression
            ):
                call_expression = self.protect_literal_comparison(
                    call_expression,
                    call_expression.inputs[1],
                    call_expression.inputs[0],
                )
        # UNMASK(expr) IN (x, y, z)  -->  expr IN (MASK(x), MASK(y), MASK(z))
        if (
            call_expression.op == pydop.ISIN
            and isinstance(call_expression.inputs[0], CallExpression)
            and isinstance(call_expression.inputs[1], LiteralExpression)
        ):
            call_expression = self.protect_literal_comparison(
                call_expression, call_expression.inputs[0], call_expression.inputs[1]
            )
        return super().visit_call_expression(call_expression)
