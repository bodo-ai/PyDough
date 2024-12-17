"""
Definitions of the exception type used in the PyDough QDAG module.
"""

__all__ = ["PyDoughQDAGException"]


class PyDoughQDAGException(Exception):
    """Exception raised when there is an error relating to a PyDough QDAG, such
    as malformed arguments/structure.
    """
