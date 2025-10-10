"""
TODO
"""

__all__ = ["MaskServerCandidateShuttle"]

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    ColumnReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
    WindowCallExpression,
)


class MaskServerCandidateShuttle(RelationalExpressionShuttle):
    """
    TODO
    """

    ALLOWED_MASK_OPERATORS: set[pydop.PyDoughExpressionOperator] = {
        pydop.BAN,
        pydop.BOR,
        pydop.NOT,
        pydop.EQU,
        pydop.NEQ,
        pydop.GRT,
        pydop.GEQ,
        pydop.LET,
        pydop.LEQ,
        pydop.NEQ,
        pydop.ISIN,
        pydop.STARTSWITH,
        pydop.ENDSWITH,
        pydop.LOWER,
        pydop.UPPER,
        pydop.MONOTONIC,
        pydop.YEAR,
        pydop.MONTH,
        pydop.DAY,
        pydop.ADD,
        pydop.SUB,
        pydop.MUL,
        pydop.DIV,
    }
    """
    TODO: ADD DESCRIPTION
    """

    def __init__(self) -> None:
        # TODO ADD COMMENTS
        self.candidate_pool: dict[
            RelationalExpression,
            tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression],
        ] = {}
        self.processed_candidates: set[RelationalExpression] = set()
        self.stack: list[
            tuple[
                tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression]
                | None,
                bool,
            ]
        ] = []

    def reset(self):
        self.stack.clear()

    def visit_call_expression(self, expr: CallExpression) -> RelationalExpression:
        # TODO ADD COMMENTS
        for arg in expr.inputs:
            arg.accept_shuttle(self)
        mask_ops: set[
            tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression]
        ] = set()
        disallowed: bool = False
        for _ in range(len(expr.inputs)):
            stack_term, arg_disallowed = self.stack.pop()
            if stack_term is not None:
                mask_ops.add(stack_term)
            disallowed |= arg_disallowed

        if (
            isinstance(expr.op, pydop.MaskedExpressionFunctionOperator)
            and expr.op.is_unmask
        ):
            self.stack.append(((expr.op, expr.inputs[0]), False))
        elif disallowed:
            self.stack.append((None, True))
        elif len(mask_ops) == 1 and expr.op in self.ALLOWED_MASK_OPERATORS:
            input_term: tuple[
                pydop.MaskedExpressionFunctionOperator, RelationalExpression
            ] = mask_ops.pop()
            if expr not in self.processed_candidates:
                self.candidate_pool[expr] = input_term
                self.processed_candidates.add(expr)
            self.stack.append((input_term, False))
        else:
            self.stack.append((None, True))
        return expr

    def visit_column_reference(
        self, column_reference: ColumnReference
    ) -> RelationalExpression:
        self.stack.append((None, True))
        return column_reference

    def visit_literal_expression(
        self, literal: LiteralExpression
    ) -> RelationalExpression:
        self.stack.append((None, False))
        return literal

    def visit_window_expression(
        self, window_expression: WindowCallExpression
    ) -> RelationalExpression:
        result: RelationalExpression = super().visit_window_expression(
            window_expression
        )
        self.stack.append((None, True))
        return result
