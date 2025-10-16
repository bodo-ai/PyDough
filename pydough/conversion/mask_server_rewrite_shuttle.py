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
    A mapping of all PyDough operators that can be handled by the Mask Server,
    mapping each such operator to the string name used in the linear string
    serialization format recognized by the Mask Server.

    Note: ISIN is handled separately.
    """

    def __init__(
        self, server_info: MaskServerInfo, candidate_visitor: MaskServerCandidateVisitor
    ) -> None:
        self.server_info: MaskServerInfo = server_info
        self.candidate_visitor: MaskServerCandidateVisitor = candidate_visitor
        self.responses: dict[RelationalExpression, RelationalExpression | None] = {}
        """
        A mapping of relational expressions from the candidate visitor that have
        been processed by the Mask Server. Each expression maps to either None
        (if the server could not handle it) or the rewritten expression based on
        the outcome of the server request.
        """

    def visit_call_expression(self, expr: CallExpression) -> RelationalExpression:
        # If this expression is in the candidate pool, process all of the
        # candidates in the pool in a batch sent to the Mask Server. The
        # candidate pool will then be cleared, preventing duplicate processing
        # of the same expression. The responses will be stored in self.responses
        # for later lookup.
        if expr in self.candidate_visitor.candidate_pool:
            self.process_batch()

        # If a Mask Server response has been stored for this expression,
        # utilize it to convert the expression to its simplified form.
        response: RelationalExpression | None = self.responses.get(expr, None)
        if response is not None:
            return response

        # Otherwise, use the regular process to recursively transform the inputs
        # to the function call.
        return super().visit_call_expression(expr)

    def process_batch(self) -> None:
        """
        Invokes the logic to dump the contents of the candidate pool to the
        Mask Server in a single batch, and process the responses to store them
        in self.responses for later lookup.
        """
        batch: list[MaskServerInput] = []
        ancillary_info: list[tuple[RelationalExpression, RelationalExpression]] = []

        # Loop over every candidate in the pool, building up the batch request
        # by adding the MaskServerInput for each candidate, and storing the
        # tuple of the original expression and the underlying input that is
        # being unmasked for later use when processing the response. The two
        # lists, the batch and ancillary info, remain in sync by index so they
        # can be zipped together later.
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

        # Send the batch to the Mask Server, and process each response
        # alongside the ancillary info. Afterwards, self.responses should
        # contain an entry for every candidate that was in the pool, mapping it
        # to None in the case of failure, or the rewritten expression in the
        # case of success.
        responses: list[MaskServerOutput] = (
            self.server_info.simplify_simple_expression_batch(batch)
        )
        assert len(responses) == len(ancillary_info)
        for (expr, input_expr), response in zip(ancillary_info, responses):
            if response.response_case != MaskServerResponse.UNSUPPORTED:
                self.responses[expr] = self.convert_response_to_relational(
                    input_expr, response
                )
            else:
                self.responses[expr] = None
            self.candidate_visitor.processed_candidates.add(expr)

        # Wipe the candidate pool to prevent duplicate processing, since every
        # candidate already in the pool has now been added to self.responses.
        self.candidate_visitor.candidate_pool.clear()

    def convert_response_to_relational(
        self, input_expr: RelationalExpression, response: MaskServerOutput
    ) -> RelationalExpression | None:
        """
        Takes in the original input expression that is being unmasked within
        a larger candidate expression for Mask Server rewrite, as well as the
        response from the Mask Server, and converts it to a relational
        expression that can be used to replace the original candidate
        expression.

        Args:
            `input_expr`: The original input expression that is being unmasked.
            `response`: The response from the Mask Server for the candidate.

        Returns:
            A relational expression that can be used to replace the original
            candidate expression. Alternatively, returns None if the response
            could not be converted (e.g. it is a pattern PyDough does not yet
            support).
        """
        result: RelationalExpression
        match response.response_case:
            case MaskServerResponse.IN_ARRAY | MaskServerResponse.NOT_IN_ARRAY:
                result = self.build_in_array_expression(input_expr, response)
            case _:
                return None
        return result

    def build_in_array_expression(
        self, input_expr: RelationalExpression, response: MaskServerOutput
    ) -> RelationalExpression:
        """
        Implements the logic of `convert_response_to_relational` specifically
        for the case where the Mask Server response indicates that the original
        expression, containing the input expression, can be replaced with an
        IN or NOT IN expression with a list of literals.

        Args:
            `input_expr`: The original input expression that is being unmasked.
            `response`: The response from the Mask Server for the candidate.
            This response is assumed to be of type IN_ARRAY or NOT_IN_ARRAY.

        Returns:
            A relational expression that can be used to replace the original
            candidate expression.
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
