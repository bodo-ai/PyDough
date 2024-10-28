"""
TODO: add file-level docstring
"""

__all__ = ["PyDoughASTException"]


class PyDoughASTException(Exception):
    """Exception raised when there is an error relating to a PyDough AST, such
    as malformed arguments/structure.
    """
