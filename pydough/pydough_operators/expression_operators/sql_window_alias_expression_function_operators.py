"""
Definition of function operator class for operators that can be created by
users and are a 1:1 correspondence with a SQL window function in the database
dialect.
"""

__all__ = ["SqlWindowAliasExpressionFunctionOperator"]


from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
    build_deducer_from_json,
    build_verifier_from_json,
)

from .expression_window_operators import ExpressionWindowOperator


class SqlWindowAliasExpressionFunctionOperator(ExpressionWindowOperator):
    """
    Implementation class for PyDough window function operators that are a 1:1
    alias to a window function call in the database SQL dialect. For example,
    a function `NVAL` could be an alias for the window function `NTH_VALUE`.

    The arguments in the `OVER` clause are handled by the general PyDough
    window function operator handling, so this class only deals with the
    arguments.
    """

    def __init__(
        self,
        function_name: str,
        sql_function_alias: str,
        allows_frame: bool,
        requires_order: bool,
        verifier_json: dict | None,
        deducer_json: dict | None,
        description: str | None,
    ):
        verifier: TypeVerifier = build_verifier_from_json(verifier_json)
        deducer: ExpressionTypeDeducer = build_deducer_from_json(deducer_json)
        self._sql_function_alias: str = sql_function_alias
        self._description: str | None = description
        super().__init__(
            function_name, verifier, deducer, allows_frame, requires_order, True
        )

    @property
    def sql_function_alias(self) -> str:
        return self._sql_function_alias

    @property
    def description(self) -> str | None:
        return self._description

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, SqlWindowAliasExpressionFunctionOperator)
            and self.sql_function_alias == other.sql_function_alias
            and super().equals(other)
        )
