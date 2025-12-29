"""
Logic for the shuttle that performs Mask Server rewrite conversion on candidates
identified by the candidate visitor.
"""

__all__ = ["MaskServerRewriteShuttle"]

import pydough.pydough_operators as pydop
from pydough.relational import (
    CallExpression,
    LiteralExpression,
    RelationalExpression,
    RelationalExpressionShuttle,
)
from pydough.types import ArrayType, BooleanType, UnknownType

from .mask_server import (
    MaskServerInfo,
    MaskServerInput,
    MaskServerOutput,
    MaskServerResponse,
)
from .mask_server_candidate_visitor import MaskServerCandidateVisitor
from .min_cover_set import choose_minimal_covering_set


class MaskServerRewriteShuttle(RelationalExpressionShuttle):
    """
    A shuttle that rewrites candidate expressions for Mask Server conversion
    identified by a `MaskServerCandidateVisitor`, by batching requests to the
    Mask Server and replacing the candidate expressions with the appropriate
    responses from the server.
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
            assert mask_op.masking_metadata.server_masked
            assert mask_op.masking_metadata.server_dataset_id is not None
            batch.append(
                MaskServerInput(
                    dataset_id=mask_op.masking_metadata.server_dataset_id,
                    table_path=mask_op.table_path,
                    column_name=mask_op.masking_metadata.column_name,
                    expression=expression_list,
                )
            )
            self.candidate_visitor.processed_candidates.add(expr)

        # Wipe the candidate pool to prevent duplicate processing, since every
        # candidate already in the pool has now been handled.
        self.candidate_visitor.candidate_pool.clear()

        # First, send the dry response batch to the Mask Server to identify
        # which predicates can be re-written.
        dry_run_results: list[MaskServerOutput] = (
            self.server_info.simplify_simple_expression_batch(batch, True)
        )

        batch, ancillary_info = self.identify_predicates_to_send(
            dry_run_results,
            batch,
            ancillary_info,
            heritage_tree=self.candidate_visitor.heritage_tree,
        )
        self.candidate_visitor.heritage_tree.clear()

        # Abort if the batch is now empty after filtering.
        if len(batch) == 0:
            return

        # Send the batch to the Mask Server, and process each response
        # alongside the ancillary info. Afterwards, self.responses should
        # contain an entry for every candidate that was in the pool, mapping it
        # to None in the case of failure, or the rewritten expression in the
        # case of success.
        responses: list[MaskServerOutput] = (
            self.server_info.simplify_simple_expression_batch(batch, False)
        )
        assert len(responses) == len(ancillary_info)
        for (expr, input_expr), response in zip(ancillary_info, responses):
            if response.response_case != MaskServerResponse.UNSUPPORTED:
                self.responses[expr] = self.convert_response_to_relational(
                    input_expr, response
                )
            else:
                self.responses[expr] = None

    def identify_predicates_to_send(
        self,
        dry_run_results: list[MaskServerOutput],
        batch: list[MaskServerInput],
        ancillary_info: list[tuple[RelationalExpression, RelationalExpression]],
        heritage_tree: dict[RelationalExpression, set[RelationalExpression | None]],
    ) -> tuple[
        list[MaskServerInput], list[tuple[RelationalExpression, RelationalExpression]]
    ]:
        """
        Takes in the results of a dry run to the Mask Server, and identifies
        which predicates should actually be sent to the server for processing in
        order to minimize the total number of requests while still ensuring
        that all necessary predicates are covered.

        Args:
            `dry_run_results`: The results from the dry run to the Mask Server.
            `batch`: The original batch of Mask Server inputs sent in the dry
            run.
            `ancillary_info`: The original ancillary info sent in the dry run.
            `heritage_tree`: A mapping of each expression to its set of parent
            expressions in the relational tree. `None` is also included in the
            set if the expression ever appears standalone without a parent.

        Returns:
            A tuple containing the new batch of Mask Server inputs to send, and
            the new ancillary info corresponding to that batch.
        """
        # Extract the underlying expressions from the ancillary info, and
        # identify  the indices of the expressions that were successful in the
        # dry run by checking the response cases.
        expressions: list[RelationalExpression] = [expr for expr, _ in ancillary_info]
        successes: list[int] = [
            idx
            for idx, result in enumerate(dry_run_results)
            if result.response_case != MaskServerResponse.UNSUPPORTED
        ]

        # Run the algorithm to identify the indices of which successful dry run
        # responses from the list should be kept.
        keep_idxs: set[int] = choose_minimal_covering_set(
            expressions, successes, heritage_tree
        )

        # Build the new batch and ancillary info lists by filtering to only
        # those indices.
        new_batch: list[MaskServerInput] = [
            elem for idx, elem in enumerate(batch) if idx in keep_idxs
        ]
        new_ancillary_info: list[tuple[RelationalExpression, RelationalExpression]] = [
            anc_elem for idx, anc_elem in enumerate(ancillary_info) if idx in keep_idxs
        ]
        return new_batch, new_ancillary_info

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
        # Extract the list of literals from the response payload. If the list
        # contains a NULL, remove it since SQL IN lists cannot contain NULLs,
        # then mark it as such so we can add the null check later.
        in_list: list = response.payload
        contains_null: bool = None in in_list
        while None in in_list:
            in_list.remove(None)
        result: RelationalExpression
        if len(in_list) == 0:
            # If the payload is empty, we can return a literal true/false
            # depending on whether it is IN or NOT IN. If there was a null, then
            # instead we just check if the expression is/isn't null.
            if contains_null:
                result = CallExpression(
                    pydop.ABSENT
                    if response.response_case == MaskServerResponse.IN_ARRAY
                    else pydop.PRESENT,
                    BooleanType(),
                    [input_expr],
                )
            else:
                result = LiteralExpression(
                    response.response_case == MaskServerResponse.NOT_IN_ARRAY,
                    BooleanType(),
                )
        elif len(in_list) == 1:
            # If the payload has one element, we can return a simple equality
            # or inequality, depending on whether it is IN or NOT IN.
            result = CallExpression(
                pydop.EQU
                if response.response_case == MaskServerResponse.IN_ARRAY
                else pydop.NEQ,
                BooleanType(),
                [
                    input_expr,
                    LiteralExpression(in_list[0], UnknownType()),
                ],
            )
        else:
            # Otherwise, we need to return an ISIN expression with an array
            # literal, and if doing NOT IN then negate the whole thing.
            array_literal: LiteralExpression = LiteralExpression(
                in_list, ArrayType(UnknownType())
            )
            result = CallExpression(
                pydop.ISIN, BooleanType(), [input_expr, array_literal]
            )
            if response.response_case == MaskServerResponse.NOT_IN_ARRAY:
                result = CallExpression(pydop.NOT, BooleanType(), [result])

        # If the original payload contained a NULL, we need to add an extra
        # check to the result to account for that, since SQL IN lists cannot
        # contain NULLs.
        # - If the list is empty after removing nulls, then the present/absent
        #   check has already been added.
        # - Otherwise, if doing IN -> `ABSENT(x) OR ISIN(x, ...)`.
        # - Otherwise, if doing NOT_IN -> `PRESENT(x) AND NOT(ISIN(x, ...))`.
        if contains_null and len(in_list) > 0:
            null_op: pydop.PyDoughExpressionOperator = (
                pydop.ABSENT
                if response.response_case == MaskServerResponse.IN_ARRAY
                else pydop.PRESENT
            )
            bool_op: pydop.PyDoughExpressionOperator = (
                pydop.BOR
                if response.response_case == MaskServerResponse.IN_ARRAY
                else pydop.BAN
            )
            is_null_check: CallExpression = CallExpression(
                null_op,
                BooleanType(),
                [input_expr],
            )
            result = CallExpression(
                bool_op,
                BooleanType(),
                [is_null_check, result],
            )

        return result
