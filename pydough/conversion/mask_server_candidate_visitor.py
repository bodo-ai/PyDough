"""
Logic for the visitor that is run across all expressions to identify candidates
for Mask Server rewrite conversion.
"""

__all__ = ["MaskServerCandidateVisitor"]

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    ColumnReference,
    CorrelatedReference,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionVisitor,
    WindowCallExpression,
)
from pydough.types import UnknownType


class MaskServerCandidateVisitor(RelationalExpressionVisitor):
    """
    TODO
    """

    OPERATORS_TO_SERVER_NAMES: dict[pydop.PyDoughExpressionOperator, str] = {
        pydop.BAN: "AND",
        pydop.BOR: "OR",
        pydop.NOT: "NOT",
        pydop.EQU: "EQUAL",
        pydop.NEQ: "NOT_EQUAL",
        pydop.GRT: "GT",
        pydop.GEQ: "GTE",
        pydop.LET: "LT",
        pydop.LEQ: "LTE",
        pydop.STARTSWITH: "STARTSWITH",
        pydop.ENDSWITH: "ENDSWITH",
        pydop.LOWER: "LOWER",
        pydop.UPPER: "UPPER",
        pydop.MONOTONIC: "BETWEEN",
        pydop.YEAR: "YEAR",
        pydop.MONTH: "MONTH",
        pydop.DAY: "DAY",
        pydop.ADD: "ADD",
        pydop.SUB: "SUB",
        pydop.MUL: "MUL",
        pydop.DIV: "DIV",
    }
    """
    TODO: ADD DESCRIPTION
    """

    def __init__(self) -> None:
        self.candidate_pool: dict[
            RelationalExpression,
            tuple[
                pydop.MaskedExpressionFunctionOperator,
                RelationalExpression,
                list[str | int | float | None | bool],
            ],
        ] = {}
        """
        TODO: ADD COMMENTS
        """

        self.processed_candidates: set[RelationalExpression] = set()
        """
        TODO: ADD COMMENTS
        """

        self.stack: list[
            tuple[
                tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression]
                | None,
                list[str | int | float | None | bool] | None,
            ]
        ] = []
        """
        TODO: ADD COMMENTS
        """

    def reset(self):
        self.stack.clear()

    def visit_call_expression(self, expr: CallExpression) -> None:
        # TODO: ADD COMMENTS
        for arg in expr.inputs:
            arg.accept_shuttle(self)
        mask_ops: set[
            tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression]
        ] = set()
        arg_exprs: list[list[str | int | float | None | bool] | None] = []
        for _ in range(len(expr.inputs)):
            stack_term, expression_list = self.stack.pop()
            if stack_term is not None:
                mask_ops.add(stack_term)
            arg_exprs.append(expression_list)
        arg_exprs.reverse()

        input_op: pydop.MaskedExpressionFunctionOperator
        input_expr: RelationalExpression
        combined_exprs: list[str | int | float | None | bool] | None
        if (
            isinstance(expr.op, pydop.MaskedExpressionFunctionOperator)
            and expr.op.is_unmask
        ):
            self.stack.append(((expr.op, expr.inputs[0]), ["__col__"]))
        elif len(mask_ops) != 1:
            self.stack.append((None, None))
        else:
            input_op, input_expr = mask_ops.pop()
            combined_exprs = self.convert_call_to_server_expression(expr, arg_exprs)
            if combined_exprs is not None and expr not in self.processed_candidates:
                self.candidate_pool[expr] = (input_op, input_expr, combined_exprs)
                self.processed_candidates.add(expr)
            self.stack.append(((input_op, input_expr), combined_exprs))

    def visit_column_reference(self, column_reference: ColumnReference) -> None:
        self.stack.append((None, None))

    def visit_literal_expression(self, literal: LiteralExpression) -> None:
        self.stack.append((None, self.convert_literal_to_server_expression(literal)))

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        for arg in window_expression.inputs:
            arg.accept_shuttle(self)
            self.stack.pop()
        for arg in window_expression.partition_inputs:
            arg.accept_shuttle(self)
            self.stack.pop()
        for order in window_expression.order_inputs:
            order.expr.accept_shuttle(self)
            self.stack.pop()
        self.stack.append((None, None))

    def visit_correlated_reference(self, correlated_reference: CorrelatedReference):
        pass

    def convert_call_to_server_expression(
        self,
        call: CallExpression,
        input_exprs: list[list[str | int | float | None | bool] | None],
    ) -> list[str | int | float | None | bool] | None:
        """
        TODO: ADD COMMENTS
        """
        result: list[str | int | float | None | bool] = []
        if call.op == pydop.ISIN and len(call.inputs) == 2:
            return self.convert_isin_call_to_server_expression(call.inputs, input_exprs)
        if call.op not in self.OPERATORS_TO_SERVER_NAMES:
            return None
        operator_name = self.OPERATORS_TO_SERVER_NAMES[call.op]
        result.append(operator_name)
        result.append(len(call.inputs))
        for inp in input_exprs:
            if inp is None:
                return None
            result.extend(inp)
        return result

    def convert_isin_call_to_server_expression(
        self,
        inputs: list[RelationalExpression],
        input_exprs: list[list[str | int | float | None | bool] | None],
    ) -> list[str | int | float | None | bool] | None:
        """
        TODO: ADD COMMENTS
        """
        if len(inputs) != 2:
            raise ValueError("ISIN operator requires exactly two inputs.")
        result: list[str | int | float | None | bool] = ["IN"]
        if input_exprs[0] is None:
            return None
        assert isinstance(inputs[1], LiteralExpression) and isinstance(
            inputs[1].value, (list, tuple)
        ), "ISIN right-hand side must be a list or tuple literal."
        in_list: list[str | int | float | None | bool] = []
        for v in inputs[1].value:
            literal_list: list[str | int | float | None | bool] | None = (
                self.convert_literal_to_server_expression(
                    LiteralExpression(v, UnknownType())
                )
            )
            if literal_list is None:
                return None
            in_list.extend(literal_list)
        result.append(len(inputs[1].value) + 1)
        result.extend(input_exprs[0])
        result.extend(in_list)
        return result

    def convert_literal_to_server_expression(
        self, literal: LiteralExpression
    ) -> list[str | int | float | None | bool] | None:
        """
        TODO: ADD COMMENTS
        """
        if literal.value is None:
            return ["NULL"]
        elif isinstance(literal.value, bool):
            return ["TRUE" if literal.value else "FALSE"]
        elif isinstance(literal.value, (int, float, str)):
            return [literal.value]
        else:
            return None
