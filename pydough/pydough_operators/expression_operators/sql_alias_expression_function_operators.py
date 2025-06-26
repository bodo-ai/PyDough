"""
TODO
"""

__all__ = ["SqlAliasExpressionFunctionOperator"]


from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
    build_deducer_from_json,
    build_verifier_from_json,
)

from .expression_function_operators import ExpressionFunctionOperator


class SqlAliasExpressionFunctionOperator(ExpressionFunctionOperator):
    """
    TODO
    """

    def __init__(
        self,
        function_name: str,
        sql_function_alias: str,
        is_aggregation: bool,
        verifier_json: dict | None,
        deducer_json: dict | None,
        description: str | None,
    ):
        verifier: TypeVerifier = build_verifier_from_json(verifier_json)
        deducer: ExpressionTypeDeducer = build_deducer_from_json(deducer_json)
        self._sql_function_alias: str = sql_function_alias
        self._description: str | None = description
        super().__init__(function_name, is_aggregation, verifier, deducer, True)

    @property
    def key(self) -> str:
        return f"FUNCTION-{self.function_name}"

    @property
    def is_aggregation(self) -> bool:
        return self._is_aggregation

    @property
    def function_name(self) -> str:
        return self._function_name

    @property
    def sql_function_alias(self) -> str:
        return self._sql_function_alias

    @property
    def description(self) -> str | None:
        return self._description

    @property
    def standalone_string(self) -> str:
        return f"Function[{self.function_name}]"

    def requires_enclosing_parens(self, parent) -> bool:
        return False

    def to_string(self, arg_strings: list[str]) -> str:
        # Stringify as "function_name(arg0, arg1, ...)
        return f"{self.function_name}({', '.join(arg_strings)})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, ExpressionFunctionOperator)
            and self.function_name == other.function_name
            and self.is_aggregation == other.is_aggregation
        )
