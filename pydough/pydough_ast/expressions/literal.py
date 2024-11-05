"""
TODO: add file-level docstring
"""

__all__ = ["Literal"]

from .expression_ast import PyDoughExpressionAST
from pydough.types import PyDoughType


class Literal(PyDoughExpressionAST):
    """
    The AST node implementation class representing a literal term which is of
    a certain PyDough type, stored as a Python object.
    """

    def __init__(self, value: object, data_type: PyDoughType):
        # TODO: add verification that value properly ascribes to the type
        # described by `data_type`
        self._value: object = value
        self._data_type: PyDoughType = data_type

    @property
    def value(self) -> object:
        """
        The object stored by the literal.
        """
        return self._value

    @property
    def pydough_type(self) -> PyDoughType:
        return self._data_type

    @property
    def is_aggregation(self) -> bool:
        return False

    def requires_enclosing_parens(self, parent: PyDoughExpressionAST) -> bool:
        return False

    def to_string(self, tree_form: bool = False) -> str:
        return repr(self.value)

    def equals(self, other: object) -> bool:
        return (
            isinstance(other, Literal)
            and (self.pydough_type == other.pydough_type)
            and (self.value == other.value)
        )
