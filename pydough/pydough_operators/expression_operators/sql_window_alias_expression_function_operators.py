"""
TODO
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
    TODO
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
