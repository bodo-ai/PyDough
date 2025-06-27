"""
TODO
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
    TODO
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
