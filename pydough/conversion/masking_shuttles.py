"""
Logic for replacing `UNMASK(x) == literal` (and similar expressions) with
`x == MASK(literal)`.
"""

__all__ = ["MaskLiteralComparisonShuttle"]

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
)
from pydough.types import ArrayType, PyDoughType, UnknownType


class MaskLiteralComparisonShuttle(RelationalExpressionShuttle):
    """
    A shuttle that recursively performs the following replacements:
    - `UNMASK(x) == literal`  -> `x == MASK(literal)`
    - `literal == UNMASK(x)`  -> `MASK(literal) == x`
    - `UNMASK(x) != literal`  -> `x != MASK(literal)`
    - `literal != UNMASK(x)`  -> `MASK(literal) != x`
    - `UNMASK(x) IN (literal1, ..., literalN)`  -> `x IN (MASK(literal1), ..., MASK(literalN))`
    """

    def rewrite_masked_literal_comparison(
        self,
        original_call: CallExpression,
        call_arg: CallExpression,
        literal_arg: LiteralExpression,
    ) -> CallExpression:
        """
        Performs a rewrite of a comparison between a call to UNMASK and a
        literal, which is either equality, inequality, or containment.

        Args:
            `original_call`: The original call expression representing the
            comparison.
            `call_arg`: The argument to the comparison that is a call to
            UNMASK, which is treated as the left-hand side of the comparison.
            `literal_arg`: The argument to the comparison that is a literal,
            which is treated as the right-hand side of the comparison.

        Returns:
            A new call expression representing the rewritten comparison, or
            the original call expression if no rewrite was performed.
        """

        # Verify that the call argument is indeed an UNMASK operation, otherwise
        # fall back to the original.
        if (
            not isinstance(call_arg.op, pydop.MaskedExpressionFunctionOperator)
            or not call_arg.op.is_unmask
        ):
            return original_call

        masked_literal: RelationalExpression

        if original_call.op in (pydop.EQU, pydop.NEQ):
            # If the operation is equality or inequality, we can simply wrap the
            # literal in a call to MASK by toggling is_unmask to False.
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
            # If the operation is containment, and the literal is a list/tuple,
            # we need to build a list by wrapping each element of the tuple in
            # a MASK call.
            inner_type: PyDoughType
            if isinstance(literal_arg.data_type, ArrayType):
                inner_type = literal_arg.data_type.elem_type
            else:
                inner_type = UnknownType()
            masked_literal = LiteralExpression(
                [
                    CallExpression(
                        pydop.MaskedExpressionFunctionOperator(
                            call_arg.op.masking_metadata, False
                        ),
                        call_arg.data_type,
                        [LiteralExpression(v, inner_type)],
                    )
                    for v in literal_arg.value
                ],
                original_call.data_type,
            )
        else:
            # Otherwise, return the original.
            return original_call

        # Now that we have the masked literal, we can return a new call
        # expression with the same operators as before, but where the left hand
        # side argument is the expression that was being unmasked, and the right
        # hand side is the masked literal.
        return CallExpression(
            original_call.op,
            original_call.data_type,
            [call_arg.inputs[0], masked_literal],
        )

    def visit_call_expression(
        self, call_expression: CallExpression
    ) -> RelationalExpression:
        # If the call expression is equality or inequality, dispatch to the
        # rewrite logic if one argument is a call expression and the other is
        # a literal.
        if call_expression.op in (pydop.EQU, pydop.NEQ):
            if isinstance(call_expression.inputs[0], CallExpression) and isinstance(
                call_expression.inputs[1], LiteralExpression
            ):
                call_expression = self.rewrite_masked_literal_comparison(
                    call_expression,
                    call_expression.inputs[0],
                    call_expression.inputs[1],
                )
            if isinstance(call_expression.inputs[1], CallExpression) and isinstance(
                call_expression.inputs[0], LiteralExpression
            ):
                call_expression = self.rewrite_masked_literal_comparison(
                    call_expression,
                    call_expression.inputs[1],
                    call_expression.inputs[0],
                )

        # If the call expression is containment, dispatch to the rewrite logic
        # if the first argument is a call expression and the second is a
        # literal.
        if (
            call_expression.op == pydop.ISIN
            and isinstance(call_expression.inputs[0], CallExpression)
            and isinstance(call_expression.inputs[1], LiteralExpression)
        ):
            call_expression = self.rewrite_masked_literal_comparison(
                call_expression, call_expression.inputs[0], call_expression.inputs[1]
            )

        # Regardless of whether the rewrite occurred or not, invoke the regular
        # logic which will recursively transform the arguments.
        return super().visit_call_expression(call_expression)
