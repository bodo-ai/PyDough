"""
TODO: add file-level docstring
"""

from . import PyDoughExpressionAST
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

    def requires_enclosing_parens(self, parent: "PyDoughExpressionAST") -> bool:
        return False

    def to_string(self) -> str:
        return f"Literal[{self.value!r}]"

    def equals(self, other: "Literal") -> bool:
        return (
            super().equals(other)
            and (self.pydough_type == other.pydough_type)
            and (self.literal == other.literal)
        )
