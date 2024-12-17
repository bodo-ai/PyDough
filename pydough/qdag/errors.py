"""
Definitions of the exception type used in the PyDough QDAG module.
"""

__all__ = ["PyDoughASTException"]


class PyDoughASTException(Exception):
    """Exception raised when there is an error relating to a PyDough QDAG, such
    as malformed arguments/structure.
    """
