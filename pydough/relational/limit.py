"""
This file contains the relational implementation for a "limit" operation.
This is the relational representation of top-n selection and typically depends
on explicit ordering of the input relation.
"""

from .abstract import Relational


class Limit(Relational):
    """
    The Limit node in the relational tree. This node represents any TOP-N
    operations in the relational algebra. This operation is dependent on the
    orderings of the input relation.

    TODO: Should this also allow top-n per group?
    """
