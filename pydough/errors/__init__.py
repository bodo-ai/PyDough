"""
Module for error handling in PyDough.
"""

__all__ = [
    "PyDoughErrorBuilder",
    "PyDoughException",
    "PyDoughMetadataException",
    "PyDoughQDAGException",
    "PyDoughSQLException",
    "PyDoughSessionException",
    "PyDoughTestingException",
    "PyDoughTypeException",
    "PyDoughUnqualifiedException",
]

from .error_types import (
    PyDoughException,
    PyDoughMetadataException,
    PyDoughQDAGException,
    PyDoughSessionException,
    PyDoughSQLException,
    PyDoughTestingException,
    PyDoughTypeException,
    PyDoughUnqualifiedException,
)
from .pydough_error_builder import PyDoughErrorBuilder
