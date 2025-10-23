"""
Special operators containing logic to mask or unmask data based on a masked
table column's metadata.
"""

__all__ = ["MaskedExpressionFunctionOperator"]


from pydough.metadata.properties import MaskedTableColumnMetadata
from pydough.pydough_operators.type_inference import (
    ConstantType,
    ExpressionTypeDeducer,
    RequireNumArgs,
    TypeVerifier,
)
from pydough.types import PyDoughType

from .expression_function_operators import ExpressionFunctionOperator


class MaskedExpressionFunctionOperator(ExpressionFunctionOperator):
    """
    A special expression function operator that masks or unmasks data based on
    a masked table column's metadata. The operator contains the metadata for
    the column, but can represent either a masking or unmasking operation
    depending on the `is_unmask` flag.
    """

    def __init__(
        self,
        masking_metadata: MaskedTableColumnMetadata,
        is_unmask: bool,
    ):
        # Create a dummy verifier that requires exactly one argument, since all
        # masking/unmasking operations are unary.
        verifier: TypeVerifier = RequireNumArgs(1)

        # Create a dummy deducer that always returns the appropriate data type
        # from the metadata based on whether this is a masking or unmasking
        # operation.
        target_type: PyDoughType = (
            masking_metadata.unprotected_data_type
            if is_unmask
            else masking_metadata.data_type
        )
        deducer: ExpressionTypeDeducer = ConstantType(target_type)

        super().__init__(
            "UNMASK" if is_unmask else "MASK", False, verifier, deducer, False
        )
        self._masking_metadata: MaskedTableColumnMetadata = masking_metadata
        self._is_unmask: bool = is_unmask

    @property
    def masking_metadata(self) -> MaskedTableColumnMetadata:
        """
        The metadata for the masked column.
        """
        return self._masking_metadata

    @property
    def is_unmask(self) -> bool:
        """
        Whether this operator is unprotecting (True) or protecting (False).
        """
        return self._is_unmask

    @property
    def format_string(self) -> str:
        """
        The format string to use for this operator to either mask or unmask the
        operand.
        """
        return (
            self.masking_metadata.unprotect_protocol
            if self.is_unmask
            else self.masking_metadata.protect_protocol
        )

    def to_string(self, arg_strings: list[str]) -> str:
        name: str = "UNMASK" if self.is_unmask else "MASK"
        arg_strings = [f"[{s}]" for s in arg_strings]
        return f"{name}::({self.format_string.format(*arg_strings)})"

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, MaskedExpressionFunctionOperator)
            and self.masking_metadata == other.masking_metadata
            and self.is_unmask == other.is_unmask
            and super().equals(other)
        )
