"""
Definition of the function operator class for scalar/aggregation operators
that are defined by a user by providing SQL text that acts as a macro that the
arguments are injected into.
"""

__all__ = ["SqlMacroExpressionFunctionOperator"]


from pydough.pydough_operators.type_inference import (
    ExpressionTypeDeducer,
    TypeVerifier,
    build_deducer_from_json,
    build_verifier_from_json,
)

from .expression_function_operators import ExpressionFunctionOperator


class SqlMacroExpressionFunctionOperator(ExpressionFunctionOperator):
    """
    Implementation class for PyDough operators that return an expression
    and represent a function call but are defined by a user-provided SQL
    text macro that specifies how the arguments should be injected into
    the SQL function call. The text should use Python format string style,
    e.g. "CASE WHEN {0} THEN {1} ELSE {2}".
    """

    def __init__(
        self,
        function_name: str,
        macro_text: str,
        is_aggregation: bool,
        verifier_json: dict | None,
        deducer_json: dict | None,
        description: str | None,
    ):
        verifier: TypeVerifier = build_verifier_from_json(verifier_json)
        deducer: ExpressionTypeDeducer = build_deducer_from_json(deducer_json)
        self._macro_text: str = macro_text
        self._description: str | None = description
        super().__init__(function_name, is_aggregation, verifier, deducer, True)

    @property
    def macro_text(self) -> str:
        return self._macro_text

    @property
    def description(self) -> str | None:
        return self._description

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, SqlMacroExpressionFunctionOperator)
            and self.macro_text == other.macro_text
            and super().equals(other)
        )
