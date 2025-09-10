"""
TODO
"""

__all__ = ["MaskLiteralComparisonShuttle"]

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
)


class MaskLiteralComparisonShuttle(RelationalExpressionShuttle):
    """
    TODO
    """

    def is_unprotect_call(self, expr: RelationalExpression) -> bool:
        """
        TODO
        """
        return (
            isinstance(expr, CallExpression)
            and isinstance(expr.op, pydop.MaskedExpressionFunctionOperator)
            and expr.op.is_unprotect
        )

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
        elif original_call.op == pydop.ISIN and isinstance(
            literal_arg.value, (list, tuple)
        ):
            masked_literal = LiteralExpression(
                [
                    CallExpression(
                        pydop.MaskedExpressionFunctionOperator(
                            call_arg.op.masking_metadata, False
                        ),
                        call_arg.data_type,
                        [LiteralExpression(v, literal_arg.data_type)],
                    )
                    for v in literal_arg.value
                ],
                original_call.data_type,
            )
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
            if isinstance(call_expression.inputs[0], CallExpression) and isinstance(
                call_expression.inputs[1], LiteralExpression
            ):
                call_expression = self.protect_literal_comparison(
                    call_expression,
                    call_expression.inputs[0],
                    call_expression.inputs[1],
                )
            if isinstance(call_expression.inputs[1], CallExpression) and isinstance(
                call_expression.inputs[0], LiteralExpression
            ):
                call_expression = self.protect_literal_comparison(
                    call_expression,
                    call_expression.inputs[1],
                    call_expression.inputs[0],
                )
        if (
            call_expression.op == pydop.ISIN
            and isinstance(call_expression.inputs[0], CallExpression)
            and isinstance(call_expression.inputs[1], LiteralExpression)
        ):
            call_expression = self.protect_literal_comparison(
                call_expression, call_expression.inputs[0], call_expression.inputs[1]
            )
        return super().visit_call_expression(call_expression)
