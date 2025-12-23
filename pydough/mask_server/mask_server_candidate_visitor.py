"""
Logic for the visitor that is run across all expressions to identify candidates
for Mask Server rewrite conversion.
"""

__all__ = ["MaskServerCandidateVisitor"]

import datetime
import re

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
from pydough.sqlglot.transform_bindings.sqlglot_transform_utils import (
    DateTimeUnit,
    current_ts_pattern,
    offset_pattern,
    trunc_pattern,
)
from pydough.types import UnknownType


class MaskServerCandidateVisitor(RelationalExpressionVisitor):
    """
    A relational expression visitor that identifies candidate expressions for
    Mask Server rewrite conversion, and stores them in a candidate pool for
    later processing by a `MaskServerRewriteShuttle`. The candidate pool
    contains expressions with the following criteria, including both
    atomic instances of the patterns, and larger expressions that contain
    these patterns as sub-expressions:
    1. An expression that contains exactly one unique unmasking operator (i.e. a
       `MaskedExpressionFunctionOperator` with `is_unmask=True`). The contents
       of the unmasking operator can be any valid expression.
    2. Literals are allowed anywhere in the expression.
    3. No other expressions are allowed (outside the contents of the unmasking
       operator) except for function calls used to combine other valid
       expressions, where the function calls must be one of the operators
       supported by the Mask Server (see `OPERATORS_TO_SERVER_NAMES`, as well as
       the `ISIN` operator).
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
        pydop.CONTAINS: "CONTAINS",
        pydop.LIKE: "LIKE",
        pydop.LOWER: "LOWER",
        pydop.UPPER: "UPPER",
        pydop.YEAR: "YEAR",
        pydop.QUARTER: "QUARTER",
        pydop.MONTH: "MONTH",
        pydop.DAY: "DAY",
        pydop.HOUR: "HOUR",
        pydop.MINUTE: "MINUTE",
        pydop.SECOND: "SECOND",
        pydop.ADD: "ADD",
        pydop.SUB: "SUB",
        pydop.MUL: "MUL",
        pydop.DIV: "DIV",
        pydop.ABS: "ABS",
        pydop.SMALLEST: "LEAST",
        pydop.LARGEST: "GREATEST",
        pydop.DEFAULT_TO: "COALESCE",
        pydop.IFF: "IFF",
    }
    """
    A mapping of all PyDough operators that can be handled by the Mask Server,
    mapping each such operator to the string name used in the linear string
    serialization format recognized by the Mask Server.

    Note: the following operators are handled separately:
    - `ISIN`
    - `SLICE`
    - `JOIN_STRINGS`
    - `DATETIME`
    - `DATEDIFF`
    - `MONOTONIC`
    """

    PREDICATE_OPERATORS: set[str] = {
        "EQUAL",
        "NOT_EQUAL",
        "GT",
        "GTE",
        "LT",
        "LTE",
        "STARTSWITH",
        "ENDSWITH",
        "CONTAINS",
        "LIKE",
        "IN",
        "AND",
        "OR",
        "NOT",
    }
    """
    The set of strings from `OPERATORS_TO_SERVER_NAMES` that correspond to
    predicate operators. Only expressions whose outermost layer is a predicate
    operator will be added to the candidate pool. This also includes other
    operators from the mask server not used by `OPERATORS_TO_SERVER_NAMES` but
    that are used by special handling cases, like how the `ISIN` operator
    in PyDough becomes the `IN` operator in the mask server.
    """

    SERVER_OPERATOR_NAMES: set[str] = {
        *OPERATORS_TO_SERVER_NAMES.values(),
        "NOT_IN",
        "SLICE",
        "CONCAT",
        "DATETIME",
        "DATEDIFF",
        "DATETRUNC",
        "REGEXP",
    }
    """
    The set of all operator names recognized by the Mask Server in its linear
    serialization format, needed because when a string literal is used that
    matches one of these reserved names, it must be wrapped in the QUOTE
    function to avoid confusion.
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
        the candidate pool at least once. This is used to avoid adding the same
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

        self.heritage_tree: dict[
            RelationalExpression, set[RelationalExpression | None]
        ] = {}
        """
        A mapping of each expression to its set of parent expressions in the
        relational tree. `None` is also included in the set if the expression
        ever appears standalone (i.e., as the root of a relational expression in
        the tree). This is used later as a core part of the algorithm for
        `choose_minimal_covering_set`. Each expression can map to multiple
        parents since the same expression instance can appear in multiple places
        within the relational tree.
        """

        self.ancestry_stack: list[RelationalExpression | None] = [None]
        """
        A stack used to keep track of the ancestry of the current expression
        being visited. The top of the stack is always the parent of the current
        expression. This is used to build the `heritage_tree` mapping.
        """

    def reset(self):
        self.stack.clear()
        self.heritage_tree.clear()
        self.ancestry_stack = [None]

    def visit_call_expression(self, expr: CallExpression) -> None:
        # First, recursively visit all of the inputs to the function call, then
        # extract the data from the stack to determine whether this expression
        # is a candidate for Mask Server rewrite conversion. Reverse the order
        # of the stack entries since they were pushed in order of visitation,
        # but need to be processed in the original input order.
        self.ancestry_stack.append(expr)
        for arg in expr.inputs:
            arg.accept_shuttle(self)
        self.ancestry_stack.pop()
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

        self.heritage_tree[expr] = self.heritage_tree.get(expr, set())
        self.heritage_tree[expr].add(self.ancestry_stack[-1])

        input_op: pydop.MaskedExpressionFunctionOperator
        input_expr: RelationalExpression
        combined_exprs: list[str | int | float | None | bool] | None

        # A call in the form `UNMASK(input_expr)` is the atomic `__col__`
        # expression that forms the base case for all candidate expressions, if
        # the column is server-masked.
        if (
            isinstance(expr.op, pydop.MaskedExpressionFunctionOperator)
            and expr.op.is_unmask
            and expr.op.masking_metadata.server_masked
            and expr.op.masking_metadata.server_dataset_id is not None
        ):
            self.stack.append(((expr.op, expr.inputs[0]), ["__col__"]))

        # If there are zero unmasking operators in the inputs, or more than
        # one, this expression is not a candidate.
        elif len(mask_ops) != 1:
            self.stack.append((None, None))

        # Otherwise, verify that the function call operator is one that can be
        # handled by the Mask Server, and if so, build the linear serialization
        # for the entire expression. If it cannot be handled, return None.
        else:
            input_op, input_expr = mask_ops.pop()
            combined_exprs = self.convert_call_to_server_expression(expr, arg_exprs)
            if combined_exprs is not None and expr not in self.processed_candidates:
                # Insert the expression and its corresponding data (the unmask
                # operator, the input expression, and the linear serialization)
                # into the candidate pool, but only if the expression's
                # outermost layer is a predicate call.
                if (
                    len(combined_exprs) > 0
                    and combined_exprs[0] in self.PREDICATE_OPERATORS
                ):
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
        # different format than the other operators, and we don't need the
        # second input to be converted since it must be a literal list.
        if call.op == pydop.ISIN and len(call.inputs) == 2:
            return self.convert_isin_call_to_server_expression(call.inputs, input_exprs)

        # If any of the inputs were not able to be converted, return None since
        # then the call cannot be converted.
        if None in input_exprs:
            return None

        # Dispatch to the specified conversion method for each operator that
        # has dedicated logic, besides ISIN which was already handled.
        match call.op:
            case pydop.MONOTONIC:
                return self.convert_monotonic_call_to_server_expression(input_exprs)
            case pydop.SLICE:
                return self.convert_slice_call_to_server_expression(input_exprs)
            case pydop.JOIN_STRINGS:
                return self.convert_join_strings_call_to_server_expression(input_exprs)
            case pydop.DATETIME:
                return self.convert_datetime_call_to_server_expression(input_exprs)
            case pydop.DATEDIFF:
                return self.convert_datediff_call_to_server_expression(input_exprs)
            case op if op in self.OPERATORS_TO_SERVER_NAMES:
                # Default handling for all the remaining operators that are
                # just translated 1:1 with from `OPERATORS_TO_SERVER_NAMES`.
                # First, build up the list with the first two entries: the name
                # of the function call operator, and the number of inputs to the
                # function call.
                result: list[str | int | float | None | bool] = []
                operator_name: str = self.OPERATORS_TO_SERVER_NAMES[call.op]
                result.append(operator_name)
                result.append(len(call.inputs))
                # For each input to the function call, append its linear
                # serialization to the result list. We know they are not None
                # from the earlier check.
                for inp in input_exprs:
                    assert inp is not None
                    result.extend(inp)
                return result
            case _:
                # Any other operator is unsupported.
                return None

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

    def convert_monotonic_call_to_server_expression(
        self, input_exprs: list[list[str | int | float | None | bool] | None]
    ) -> list[str | int | float | None | bool] | None:
        """
        Converts a PyDough MONOTONIC operation to the linear serialization
        format recognized by the Mask Server. MONOTONIC(a, b, c) is converted to
        be equivalent to `(a <= b) AND (b <= c)`.

        Args:
            `input_exprs`: A list of linear serializations for each input to
            the MONOTONIC call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the MONOTONIC operation, or None if the MONOTONIC
            operation could not be converted.
        """
        assert len(input_exprs) == 3, (
            "MONOTONIC operator requires exactly three inputs."
        )
        if input_exprs[0] is None or input_exprs[1] is None or input_exprs[2] is None:
            return None
        arg0: list[str | int | float | None | bool] = input_exprs[0]
        arg1: list[str | int | float | None | bool] = input_exprs[1]
        arg2: list[str | int | float | None | bool] = input_exprs[2]
        return ["AND", 2, "LTE", 2, *arg0, *arg1, "LTE", 2, *arg1, *arg2]

    def convert_slice_call_to_server_expression(
        self, input_exprs: list[list[str | int | float | None | bool] | None]
    ) -> list[str | int | float | None | bool] | None:
        """
        Attempts to convert a PyDough SLICE operation to the linear
        serialization format recognized by the Mask Server. This requires
        converting the slice from Python form `input_expr[start:stop:step]` to
        the more SQL-like form `SUBSTRING(input_expr, start, length)`, but
        still using 0-based indexing for start (just like Python).

        Args:
            `input_exprs`: A list of linear serializations for each input to
            the SLICE call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the SLICE operation, or None if the SLICE
            operation could not be converted.
        """
        assert len(input_exprs) == 4, "SLICE operator requires exactly four inputs."
        # Start by building the output list with the operator name, the number
        # of arguments (3), and the linear serialization of the input
        # expression. If the input expression could not be converted, return
        # None.
        result: list[str | int | float | None | bool] = ["SLICE", 3]
        if input_exprs[0] is None:
            return None
        result.extend(input_exprs[0])

        # Attempt to extract the start, stop, and step values from the remaining
        # arguments to the slice operation and convert them to start vs length.
        # For now, only supports the form where step is 1, and start/stop are
        # both positive integer literals, with stop > start. Alternatively,
        # allows taking a prefix since that case is similarly well defined.
        start_int: int
        length_int: int
        start_literal = input_exprs[1]
        stop_literal = input_exprs[2]
        step_literal = input_exprs[3]
        if (
            start_literal is None
            or stop_literal is None
            or len(start_literal) != 1
            or len(stop_literal) != 1
            or step_literal not in ([1], ["NULL"])
        ):
            return None
        match (start_literal[0], stop_literal[0]):
            case (int(start), int(stop)) if start >= 0 and stop > start:
                start_int = start
                length_int = stop - start
            case ("NULL", int(stop)) if stop > 0:
                start_int = 0
                length_int = stop
            case _:
                return None

        result.append(start_int)
        result.append(length_int)
        return result

    def convert_join_strings_call_to_server_expression(
        self, input_exprs: list[list[str | int | float | None | bool] | None]
    ) -> list[str | int | float | None | bool] | None:
        """
        Converts the JOIN_STRINGS PyDough operator to an equivalent variadic
        CONCAT operation in the linear serialization format recognized by
        the Mask Server:

        `JOIN_STRINGS('', a, b, c)` becomes `CONCAT(3, a, b, c)`
        `JOIN_STRINGS(s, a, b, c)` becomes `CONCAT(5, a, s, b, s, c)`

        Args:
            `input_exprs`: A list of linear serializations for each input to
            the JOIN_STRINGS call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted. The first input is the delimiter
            expression, and each subsequent input is a string expression to
            be joined.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the JOIN_STRINGS operation, or None if the
            JOIN_STRINGS operation could not be converted.
        """
        assert len(input_exprs) >= 3, (
            "JOIN_STRINGS operator requires at least three inputs."
        )
        # If the delimiter expression could not be converted, return None.
        delimiter_expr: list[str | int | float | None | bool] | None = input_exprs[0]
        if delimiter_expr is None:
            return None

        # Start building the result list with the operator name.
        result: list[str | int | float | None | bool] = ["CONCAT"]

        # If the delimiter is the empty string, then the number of arguments
        # is simply the number of input expressions minus one (the delimiter),
        # and all of the remaining arguments should just be appended directly.
        remaining_args: list[list[str | int | float | None | bool] | None] = (
            input_exprs[1:]
        )
        if delimiter_expr == [""]:
            result.append(len(remaining_args))
            for expr in remaining_args:
                if expr is None:
                    return None
                result.extend(expr)
            return result

        # Otherwise, the remaining arguments are interleaved with the delimiter.
        result.append(2 * len(remaining_args) - 1)
        for i, expr in enumerate(remaining_args):
            if expr is None:
                return None
            result.extend(expr)
            if i < len(remaining_args) - 1:
                result.extend(delimiter_expr)
        return result

    def convert_datetime_call_to_server_expression(
        self, input_exprs: list[list[str | int | float | None | bool] | None]
    ) -> list[str | int | float | None | bool] | None:
        """
        Attempts to convert a PyDough DATETIME operation to the linear
        serialization format recognized by the Mask Server. The DATETIME
        operation is treated as a series of transformations on an initial
        input expression, where each transformation is either a truncation
        (DATETRUNC) or an addition (DATEADD).

        Args:
            `input_exprs`: A list of linear serializations for each input to
            the DATETIME call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted. The first input is the seed expression,
            and each subsequent input is a string representing either a
            truncation or addition operation.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the DATETIME operation, or None if the DATETIME
            operation could not be converted.
        """
        # Skip cases where DATETIME is called on an argument just to cast it.
        if len(input_exprs) < 2:
            return None

        # Start with the input argument, then iteratively apply each phase of
        # the transformation with DATETIME as either a truncation or addition.
        # Reject if the seed is a literal indicating the current timestamp.
        result: list[str | int | float | None | bool]
        if input_exprs[0] is None or (
            len(input_exprs[0]) == 1
            and isinstance(input_exprs[0][0], str)
            and current_ts_pattern.fullmatch(input_exprs[0][0])
        ):
            return None
        else:
            result = input_exprs[0]
            for arg in input_exprs[1:]:
                if arg is None or len(arg) != 1 or not isinstance(arg[0], str):
                    return None
                # Use regex to determine if this is a truncation or addition,
                # and dispatch to the appropriate conversion method. If it is
                # neither, or the conversion method failed, return None.
                # Otherwise, the result becomes the new input to the next phase.
                trunc_match: re.Match | None = trunc_pattern.fullmatch(arg[0])
                offset_match: re.Match | None = offset_pattern.fullmatch(arg[0])
                new_result: list[str | int | float | None | bool] | None = None
                if trunc_match is not None:
                    new_result = self.convert_datetrunc_call_to_server_expression(
                        result, str(trunc_match.group(1))
                    )
                elif offset_match is not None:
                    new_result = self.convert_dateadd_call_to_server_expression(
                        result,
                        str(offset_match.group(1)),
                        int(offset_match.group(2)),
                        str(offset_match.group(3)),
                    )
                if new_result is None:
                    return None
                result = new_result

        return result

    def convert_datetrunc_call_to_server_expression(
        self, input_expr: list[str | int | float | None | bool], unit_str: str
    ) -> list[str | int | float | None | bool] | None:
        """
        Attempt to convert a DATETRUNC call to the linear serialization format
        recognized by the Mask Server.

        Args:
            `input_expr`: A linear serialization for the input to the
            DATETRUNC call, as a list of strings/ints/floats/bools/None.
            `unit_str`: The string representing the unit to truncate to.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the DATETRUNC operation, or None if the DATETRUNC
            operation could not be converted.
        """
        unit: DateTimeUnit | None = DateTimeUnit.from_string(unit_str)
        # Reject if the unit is not recognized, or is a WEEK (for now).
        if unit is None or unit == DateTimeUnit.WEEK:
            return None
        result: list[str | int | float | None | bool] = ["DATETRUNC", 2]
        result.append(unit.value)
        result.extend(input_expr)
        return result

    def convert_dateadd_call_to_server_expression(
        self,
        input_expr: list[str | int | float | None | bool],
        sign_str: str,
        amount: int,
        unit_str: str,
    ) -> list[str | int | float | None | bool] | None:
        """
        Attempt to convert a DATEADD call to the linear serialization format
        recognized by the Mask Server.

        Args:
            `input_expr`: A linear serialization for the input to the
            DATEADD call, as a list of strings/ints/floats/bools/None.
            `sign_str`: The string representing the sign of the amount to add (
            either "+", "-", or "", with empty being the same as "+").
            `amount`: The integer amount to add (can be negative).
            `unit_str`: The string representing the unit to add.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the DATEADD operation, or None if the DATEADD
            operation could not be converted.
        """
        unit: DateTimeUnit | None = DateTimeUnit.from_string(unit_str)
        if unit is None or unit == DateTimeUnit.WEEK:
            return None
        result: list[str | int | float | None | bool] = ["DATEADD", 3]
        if sign_str == "-":
            amount = -amount
        result.append(amount)
        result.append(unit.value + "s")
        result.extend(input_expr)
        return result

    def convert_datediff_call_to_server_expression(
        self, input_exprs: list[list[str | int | float | None | bool] | None]
    ) -> list[str | int | float | None | bool] | None:
        """
        Attempt to convert a DATEDIFF call to the linear serialization format
        recognized by the Mask Server. The datediff is transformed by having
        its first argument, the units, normalized into one of the following:
        - "years"
        - "quarters"
        - "months"
        - "days"
        - "hours"
        - "minutes"
        - "seconds"

        Weeks are ignored for now.

        Args:
            `input_exprs`: A list of linear serializations for each input to
            the DATEDIFF call, where each input serialization is either a
            list of strings/ints/floats/bools/None, or None if the input
            could not be converted.

        Returns:
            A list of strings/ints/floats/bools/None representing the linear
            serialization of the DATEDIFF operation, or None if the DATEDIFF
            operation could not be converted.
        """
        result: list[str | int | float | None | bool] = ["DATEDIFF", 3]
        assert len(input_exprs) == 3, "DATEDIFF operator requires exactly three inputs."

        # Extract and normalize the unit argument, rejecting weeks for now.
        unit_expr = input_exprs[0]
        if (
            unit_expr is None
            or len(unit_expr) != 1
            or not isinstance(unit_expr[0], str)
        ):
            return None
        unit: DateTimeUnit | None = DateTimeUnit.from_string(unit_expr[0])
        if unit is None or unit == DateTimeUnit.WEEK:
            return None
        result.append(unit.value + "s")

        # Append the linear serializations for the start and end expressions.
        start_expr = input_exprs[1]
        end_expr = input_exprs[2]
        if start_expr is None or end_expr is None:
            return None
        result.extend(start_expr)
        result.extend(end_expr)
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
        elif isinstance(literal.value, datetime.datetime):
            return [literal.value.strftime("%Y-%m-%d %H:%M:%S")]
        elif isinstance(literal.value, datetime.date):
            return [literal.value.isoformat()]
        else:
            return None
