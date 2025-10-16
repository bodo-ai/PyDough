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
    A mapping of all PyDough operators that can be handled by the Mask Server,
    mapping each such operator to the string name used in the linear string
    serialization format recognized by the Mask Server.

    Note: ISIN is handled separately.
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
        The internal datastructure used to keep track of all candidate
        expressions identified during a traversal of a relational tree. Each
        candidate expression maps to a tuple of:
        1. The single unmasking operator contained within the expression.
        2. The input expression that is being unmasked.
        3. The linear serialization of the entire expression as a list, where
           invocations of UNMASK(input_expr) are replaced with the token
           "__col__".
        """

        self.processed_candidates: set[RelationalExpression] = set()
        """
        The set of all relational expressions that have already been added to
        the candidate pool at lest once. This is used to avoid adding the same
        candidate multiple times if it is encountered multiple times during a
        traversal of the relational tree, since the candidate pool will be
        cleared once all of the candidates in the pool are processed in a batch
        request to the mask server.
        """

        self.stack: list[
            tuple[
                tuple[pydop.MaskedExpressionFunctionOperator, RelationalExpression]
                | None,
                list[str | int | float | None | bool] | None,
            ]
        ] = []
        """
        The stack is used to keep track of information relating to
        sub-expressions of the current expression. When visiting an expression,
        the stack will contain one entry for each input to the expression,
        where each entry is a tuple of:
        1. Either None, or the single unmasking operator and input expression
           contained within the input expression, if any.
        2. Either None, or the linear serialization of the input expression as
           a list, where invocations of UNMASK(input_expr) are replaced with
           the token "__col__".
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
        # Literals do not contain the UNMASK operator, but can have a linear
        # serialization that can be sent to the Mask Server, so we convert the
        # literal to the appropriate list format and push that onto the stack.
        self.stack.append((None, self.convert_literal_to_server_expression(literal)))

    def visit_window_expression(self, window_expression: WindowCallExpression) -> None:
        # Window functions cannot be sent to the mask server, but their inputs
        # potentially can be.
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
        # Correlated references cannot be sent to the mask server.
        self.stack.append((None, None))

    def convert_call_to_server_expression(
        self,
        call: CallExpression,
        input_exprs: list[list[str | int | float | None | bool] | None],
    ) -> list[str | int | float | None | bool] | None:
        """
        Converts a function call to the linear serialization format recognized
        by the Mask Server, using the provided list of linear serializations for
        each input to the function call. If the function call cannot be
        converted, returns None.

        Args:
            `call`: The function call to convert.
            `input_exprs`: A list of linear serializations for each input to
            the function call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the function call, or None if the function call
            could not be converted.
        """

        # If the function call is an ISIN, handle it separately since it has a
        # different format than the other operators.
        if call.op == pydop.ISIN and len(call.inputs) == 2:
            return self.convert_isin_call_to_server_expression(call.inputs, input_exprs)

        # Besides ISIN, if the function call is not one of the operators that
        # can be handled by the Mask Server, return None since it cannot be
        # converted.
        elif call.op not in self.OPERATORS_TO_SERVER_NAMES:
            return None

        # Build up the list with the first two entries: the name of the function
        # call operator, and the number of inputs to the function call.
        result: list[str | int | float | None | bool] = []
        operator_name = self.OPERATORS_TO_SERVER_NAMES[call.op]
        result.append(operator_name)
        result.append(len(call.inputs))

        # For each input to the function call, append its linear serialization
        # to the result list. If any input could not be converted, return None.
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
        Converts a relational expression for an ISIN call into the linear
        serialization list format recognized by the Mask Server, using the
        provided list of linear serializations for the first input, versus a
        manual unfolding of the second input which must be a literal list.

        Args:
            `inputs`: The two inputs to the ISIN call.
            `input_exprs`: A list of linear serializations for each input to
            the ISIN call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted.
        """
        if len(inputs) != 2:
            raise ValueError("ISIN operator requires exactly two inputs.")

        # Start the output list with the operator name. If the first input
        # could not be converted, return None.
        if input_exprs[0] is None:
            return None
        assert isinstance(inputs[1], LiteralExpression) and isinstance(
            inputs[1].value, (list, tuple)
        ), "ISIN right-hand side must be a list or tuple literal."

        # Unfold the second input, which must be a literal list, into the
        # output list. If any element of the list cannot be converted, return
        # None.
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

        # The result list is:
        # 1. The operator name "IN"
        # 2. The total number of arguments, including the element to check
        #    versus the number of elements in the list.
        # 3. The linear serialization of the first input expression.
        # 4. The unfolded elements of the literal list from the second input.
        result: list[str | int | float | None | bool] = ["IN"]
        result.append(len(inputs[1].value) + 1)
        result.extend(input_exprs[0])
        result.extend(in_list)
        return result

    def convert_literal_to_server_expression(
        self, literal: LiteralExpression
    ) -> list[str | int | float | None | bool] | None:
        """
        Converts a literal expression to the linear serialization format
        recognized by the Mask Server. If the literal cannot be converted,
        returns None.

        Args:
            `literal`: The literal expression to convert.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the literal, or None if the literal could not be
            converted.
        """
        if literal.value is None:
            return ["NULL"]
        elif isinstance(literal.value, bool):
            return ["TRUE" if literal.value else "FALSE"]
        elif isinstance(literal.value, (int, float, str)):
            return [literal.value]
        else:
            return None
