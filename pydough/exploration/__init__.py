"""
Module of PyDough dealing with APIs used for user explanation & exploration
of PyDough metadata and code.
"""

__all__ = ["explain", "explain_structure", "explain_term"]

from .explain import explain
from .structure import explain_structure
from .term import explain_term
