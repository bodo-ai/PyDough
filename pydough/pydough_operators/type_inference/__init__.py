"""
Submodule of PyDough operators module dealing with type checking and return
type inference.
"""

__all__ = [
    "AllowAny",
    "ConstantType",
    "ExpressionTypeDeducer",
    "RequireArgRange",
    "RequireMinArgs",
    "RequireNumArgs",
    "SelectArgumentType",
    "TypeVerifier",
]

from .expression_type_deducer import (
    ConstantType,
    ExpressionTypeDeducer,
    SelectArgumentType,
)
from .type_verifier import (
    AllowAny,
    RequireArgRange,
    RequireMinArgs,
    RequireNumArgs,
    TypeVerifier,
)
