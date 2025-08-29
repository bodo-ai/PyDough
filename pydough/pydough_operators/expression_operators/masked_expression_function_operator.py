"""
TODO
"""

__all__ = ["MaskedExpressionFunctionOperator"]


from pydough.metadata.properties import MaskedTableColumnMetadata
from pydough.pydough_operators.type_inference import (
    ConstantType,
    ExpressionTypeDeducer,
    RequireNumArgs,
    TypeVerifier,
)

from .expression_function_operators import ExpressionFunctionOperator


class MaskedExpressionFunctionOperator(ExpressionFunctionOperator):
    """
    TODO
    """

    def __init__(
        self,
        masking_metadata: MaskedTableColumnMetadata,
        is_unprotect: bool,
    ):
        verifier: TypeVerifier = RequireNumArgs(1)
        deducer: ExpressionTypeDeducer = ConstantType(
            masking_metadata.unprotected_data_type
            if is_unprotect
            else masking_metadata.data_type
        )
        super().__init__(
            "UNMASK" if is_unprotect else "MASK", False, verifier, deducer, False
        )
        self._masking_metadata: MaskedTableColumnMetadata = masking_metadata
        self._is_unprotect: bool = is_unprotect

    @property
    def masking_metadata(self) -> MaskedTableColumnMetadata:
        """
        The metadata for the masked column.
        """
        return self._masking_metadata

    @property
    def is_unprotect(self) -> bool:
        """
        Whether this operator is unprotecting (True) or protecting (False).
        """
        return self._is_unprotect

    @property
    def format_string(self) -> str:
        """
        The format string to use for this operator to either mask or unmask the
        operand.
        """
        return (
            self.masking_metadata.unprotect_protocol
            if self.is_unprotect
            else self.masking_metadata.protect_protocol
        )

    def to_string(self, arg_strings: list[str]) -> str:
        name: str = "UNMASK" if self.is_unprotect else "MASK"
        arg_strings = [f"[{s}]" for s in arg_strings]
        return f"{name}::({self.format_string.format(*arg_strings)})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, MaskedExpressionFunctionOperator)
            and self.masking_metadata == other.masking_metadata
            and self.is_unprotect == other.is_unprotect
            and super().equals(other)
        )
