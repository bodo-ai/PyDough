"""
Logic for the shuttle that performs Mask Server rewrite conversion on candidates
identified by the candidate visitor.
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

from .mask_server_candidate_visitor import MaskServerCandidateVisitor


class MaskServerRewriteShuttle(RelationalExpressionShuttle):
    """
    A shuttle that rewrites candidate expressions for Mask Server conversion
    identified by a `MaskServerCandidateVisitor`, by batching requests to the
    Mask Server and replacing the candidate expressions with the appropriate
    responses from the server.
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
        self, server_info: MaskServerInfo, candidate_visitor: MaskServerCandidateVisitor
    ) -> None:
        self.server_info: MaskServerInfo = server_info
        self.candidate_visitor: MaskServerCandidateVisitor = candidate_visitor
        self.responses: dict[
            RelationalExpression, tuple[RelationalExpression, MaskServerOutput] | None
        ] = {}

    def visit_call_expression(self, expr: CallExpression) -> RelationalExpression:
        # TODO: ADD COMMENTS
        if expr in self.candidate_visitor.candidate_pool:
            self.process_batch()

        response: tuple[RelationalExpression, MaskServerOutput] | None = (
            self.responses.get(expr, None)
        )
        if response is not None:
            return self.convert_response_to_relational(*response)
        return super().visit_call_expression(expr)

    def process_batch(self) -> None:
        """
        TODO: ADD COMMENTS
        """
        batch: list[MaskServerInput] = []
        ancillary_info: list[tuple[RelationalExpression, RelationalExpression]] = []
        for expr, (
            mask_op,
            input_expr,
            expression_list,
        ) in self.candidate_visitor.candidate_pool.items():
            ancillary_info.append((expr, input_expr))
            batch.append(
                MaskServerInput(
                    table_path=mask_op.table_path,
                    column_name=mask_op.masking_metadata.column_name,
                    expression=expression_list,
                )
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
            self.candidate_visitor.processed_candidates.add(expr)
        self.candidate_visitor.candidate_pool.clear()

    def convert_response_to_relational(
        self, input_expr: RelationalExpression, response: MaskServerOutput
    ) -> RelationalExpression:
        """
        TODO: ADD COMMENTS
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
        TODO: ADD COMMENTS
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
