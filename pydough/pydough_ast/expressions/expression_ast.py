"""
TODO: add file-level docstring
"""

from abc import abstractmethod


from pydough.pydough_ast.abstract_pydough_ast import PyDoughAST
from pydough.types import PyDoughType


class PyDoughExpressionAST(PyDoughAST):
    """
    The base class for AST nodes that represent expressions.
    """

    def __repr__(self):
        return self.to_string()

    @property
    @abstractmethod
    def pydough_type(self) -> PyDoughType:
        """
        The PyDough type of the expression.
        """

    @abstractmethod
    def to_string(self) -> str:
        """
        Returns a PyDough expression AST converted into a single-line string
        structured so it can be placed in the tree-like string representation
        of a collection AST.

        Returns:
            The single-line string representation of `self`.
        """

    @abstractmethod
    def requires_enclosing_parens(self, parent: "PyDoughExpressionAST") -> bool:
        """
        Identifies whether an expression converted to a string must be wrapped
        in parenthesis before being inserted into it's parent's string
        representation. This depends on what exactly the parent is.

        Args:
            `parent`: the parent expression AST that contains this expression
            AST as a child.

        Returns:
            True if the string representation of `parent` should enclose
            parenthesis around the string representation of `self`.
        """
