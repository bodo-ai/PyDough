"""
Predicate Server Client and Models.

This package provides:
- PredicateExpression: representation and validation of predicate expressions.
- EvaluateRequest / BatchEvaluateRequest: request objects for the API.
- EvaluateResponse / BatchEvaluateResponse: response objects for the API.
- PredicateAPIClient: client for interacting with the Predicate Server.
"""

__all__ = [
    "BatchEvaluateItemResponse",
    "BatchEvaluateRequest",
    "BatchEvaluateResponse",
    "EvaluateRequest",
    "EvaluateResponse",
    "Materialization",
    "MaterializationLiteral",
    "MaterializationRowIds",
    "PredicateAPIClient",
    "PredicateExpression",
    "PredicateParseError",
]

from .api_client import PredicateAPIClient
from .batch_evaluate_response import BatchEvaluateItemResponse, BatchEvaluateResponse
from .evaluate_request import BatchEvaluateRequest, EvaluateRequest
from .evaluate_response import (
    EvaluateResponse,
    Materialization,
    MaterializationLiteral,
    MaterializationRowIds,
)
from .predicate_expression import PredicateExpression, PredicateParseError
