"""
Base definition of PyDough expression QDAG nodes.
"""

__all__ = ["PyDoughExpressionQDAG"]

from abc import abstractmethod

from pydough.qdag.abstract_pydough_qdag import PyDoughQDAG
from pydough.types import PyDoughType


class PyDoughExpressionQDAG(PyDoughQDAG):
    """
    The base class for QDAG nodes that represent expressions.
    """

    def __repr__(self):
        return self.to_string()

    @property
    @abstractmethod
    def pydough_type(self) -> PyDoughType:
        """
        The PyDough type of the expression.
        """

    @property
    @abstractmethod
    def is_aggregation(self) -> bool:
        """
        Whether the expression corresponds to an aggregation that
        can collapse multiple records into a scalar value.
        """

    @abstractmethod
    def is_singular(self, context: PyDoughQDAG) -> bool:
        """
        Returns whether the expression is singular with regards to a
        context collection.

        Args:
            `context`: the collection that the singular/plural status of the
            current expression is being checked against. Note: despite the
            annotation, this must be a collection.

        Returns:
            True if there is at most a single record of the current expression
            for each record of the context, and False otherwise.
        """

    @property
    def key(self) -> str:
        return self.to_string()

    @abstractmethod
    def to_string(self, tree_form: bool = False) -> str:
        """
        Returns a PyDough expression QDAG converted into a single-line string
        structured so it can be placed in the tree-like string representation
        of a collection QDAG.

        Args:
            `tree_form`: indicates that the string conversion is happening
            inside of a tree string (default False).

        Returns:
            The single-line string representation of `self`.
        """

    @abstractmethod
    def requires_enclosing_parens(self, parent: "PyDoughExpressionQDAG") -> bool:
        """
        Identifies whether an expression converted to a string must be wrapped
        in parenthesis before being inserted into it's parent's string
        representation. This depends on what exactly the parent is.

        Args:
            `parent`: the parent expression QDAG that contains this expression
            QDAG as a child.

        Returns:
            True if the string representation of `parent` should enclose
            parenthesis around the string representation of `self`.
        """
