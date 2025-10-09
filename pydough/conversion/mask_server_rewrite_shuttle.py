"""
TODO
"""

__all__ = ["MaskServerRewriteShuttle"]

import pydough.pydough_operators as pydop
from pydough.mask_server import (
    MaskServerInfo,
    MaskServerInput,
    MaskServerOutput,
    MaskServerResponse,
)
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
)
from pydough.types import ArrayType, BooleanType, UnknownType

from .mask_server_candidate_shuttle import MaskServerCandidateShuttle


class MaskServerRewriteShuttle(RelationalExpressionShuttle):
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

    NOTE: ISIN is handled separately.
    """

    def __init__(
        self, server_info: MaskServerInfo, candidate_shuttle: MaskServerCandidateShuttle
    ) -> None:
        self.server_info: MaskServerInfo = server_info
        self.candidate_shuttle: MaskServerCandidateShuttle = candidate_shuttle
        self.responses: dict[
            RelationalExpression, tuple[RelationalExpression, MaskServerOutput] | None
        ] = {}

    def visit_call_expression(self, expr: CallExpression) -> RelationalExpression:
        # TODO: ADD COMMENTS
        if expr in self.candidate_shuttle.candidate_pool:
            self.process_batch()

        response: tuple[RelationalExpression, MaskServerOutput] | None = (
            self.responses.get(expr, None)
        )
        if response is not None:
            return self.convert_response_to_relational(*response)
        return super().visit_call_expression(expr)

    def process_batch(self) -> None:
        """
        TODO
        """
        batch: list[MaskServerInput] = []
        ancillary_info: list[tuple[RelationalExpression, RelationalExpression]] = []
        for expr, (
            mask_op,
            input_expr,
        ) in self.candidate_shuttle.candidate_pool.items():
            ancillary_info.append((expr, input_expr))
            batch.append(
                MaskServerInput(
                    table_path=mask_op.table_path,
                    column_name=mask_op.masking_metadata.column_name,
                    expression=self.convert_to_server_expression(expr),
                )
            )
            print()
            print(
                f"BATCH ITEM: ({mask_op.table_path}.{mask_op.masking_metadata.column_name}): {batch[-1].expression}"
            )
        responses: list[MaskServerOutput] = (
            self.server_info.simplify_simple_expression_batch(batch)
        )
        assert len(responses) == len(ancillary_info)
        for (expr, input_expr), response in zip(ancillary_info, responses):
            if response.response_case != MaskServerResponse.UNSUPPORTED:
                self.responses[expr] = (input_expr, response)
            else:
                self.responses[expr] = None
            self.candidate_shuttle.processed_candidates.add(expr)
        self.candidate_shuttle.candidate_pool.clear()

    def convert_literal_to_server_expression(
        self, literal: LiteralExpression
    ) -> list[str | int | float | None | bool]:
        """
        TODO
        """
        if literal.value is None:
            return ["NULL"]
        elif isinstance(literal.value, bool):
            return ["TRUE" if literal.value else "FALSE"]
        elif isinstance(literal.value, (int, float)):
            return [literal.value]
        elif isinstance(literal.value, str):
            return [literal.value]
        else:
            raise ValueError(
                f"Unsupported literal type for mask server conversion: {type(literal.value)}"
            )

    def convert_to_server_expression(
        self, expr: RelationalExpression
    ) -> list[str | int | float | None | bool]:
        """
        TODO
        """
        if isinstance(expr, LiteralExpression):
            return self.convert_literal_to_server_expression(expr)
        elif isinstance(expr, CallExpression):
            if isinstance(expr.op, pydop.MaskedExpressionFunctionOperator):
                return ["__col__"]
            elif expr.op in self.OPERATORS_TO_SERVER_NAMES:
                return self.convert_call_to_server_expression(
                    self.OPERATORS_TO_SERVER_NAMES[expr.op], expr.inputs
                )
            elif expr.op == pydop.ISIN:
                return self.convert_isin_call_to_server_expression(expr.inputs)
            else:
                raise ValueError(
                    f"Unsupported operator for mask server conversion: {expr.op}"
                )
        else:
            raise ValueError(
                f"Unsupported expression type for mask server conversion: {type(expr)}"
            )

    def convert_call_to_server_expression(
        self, operator_name: str, inputs: list[RelationalExpression]
    ) -> list[str | int | float | None | bool]:
        """
        TODO
        """
        result: list[str | int | float | None | bool] = [operator_name]
        result.append(len(inputs))
        for inp in inputs:
            result.extend(self.convert_to_server_expression(inp))
        return result

    def convert_isin_call_to_server_expression(
        self, inputs: list[RelationalExpression]
    ) -> list[str | int | float | None | bool]:
        """
        TODO
        """
        if len(inputs) != 2:
            raise ValueError("ISIN operator requires exactly two inputs.")
        result: list[str | int | float | None | bool] = ["IN"]
        args: list[str | int | float | None | bool] = self.convert_to_server_expression(
            inputs[0]
        )
        assert isinstance(inputs[1], LiteralExpression) and isinstance(
            inputs[1].value, (list, tuple)
        ), "ISIN right-hand side must be a list or tuple literal."
        for v in inputs[1].value:
            args.extend(
                self.convert_literal_to_server_expression(
                    LiteralExpression(v, UnknownType())
                )
            )
        result.append(len(inputs[1].value))
        result.extend(args)
        return result

    def convert_response_to_relational(
        self, input_expr: RelationalExpression, response: MaskServerOutput
    ) -> RelationalExpression:
        """
        TODO
        """
        result: RelationalExpression
        match response.response_case:
            case MaskServerResponse.IN_ARRAY | MaskServerResponse.NOT_IN_ARRAY:
                result = self.build_in_array_expression(input_expr, response)
            case _:
                raise ValueError(
                    f"Unsupported mask server response case: {response.response_case}"
                )
        return result

    def build_in_array_expression(
        self, input_expr: RelationalExpression, response: MaskServerOutput
    ) -> RelationalExpression:
        """
        TODO
        """
        assert response.response_case in (
            MaskServerResponse.IN_ARRAY,
            MaskServerResponse.NOT_IN_ARRAY,
        )
        assert isinstance(response.payload, list)
        if len(response.payload) == 0:
            # If the payload is empty, we can return a literal true/false
            # depending on whether it is IN or NOT IN
            return LiteralExpression(
                response.response_case == MaskServerResponse.NOT_IN_ARRAY, BooleanType()
            )
        elif len(response.payload) == 1:
            # If the payload has one element, we can return a simple equality
            # or inequality, depending on whether it is IN or NOT IN
            return CallExpression(
                pydop.EQU
                if response.response_case == MaskServerResponse.IN_ARRAY
                else pydop.NEQ,
                BooleanType(),
                [
                    input_expr,
                    LiteralExpression(response.payload[0], UnknownType()),
                ],
            )
        else:
            # Otherwise, we need to return an ISIN expression with an array
            # literal, and if doing NOT IN then negate the whole thing.
            array_literal: LiteralExpression = LiteralExpression(
                response.payload, ArrayType(UnknownType())
            )
            result: RelationalExpression = CallExpression(
                pydop.ISIN, BooleanType(), [input_expr, array_literal]
            )
            if response.response_case == MaskServerResponse.NOT_IN_ARRAY:
                result = CallExpression(pydop.NOT, BooleanType(), [result])
            return result
