"""
Utilities used for PyDough return type inference.
"""

__all__ = ["ConstantType", "ExpressionTypeDeducer", "SelectArgumentType"]

from abc import ABC, abstractmethod
from typing import Any

from pydough.types import PyDoughType


class ExpressionTypeDeducer(ABC):
    """
    Abstract base class for type-inferring classes that take in a list of
    PyDough expression QDAGs and returns a PyDough type. Each implementation
    class must implement the `infer_return_type` API.
    """

    @abstractmethod
    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        """
        Returns the inferred expression type based on the input arguments.

        Raises:
            `PyDoughQDAGException` if the arguments are invalid.
        """


class SelectArgumentType(ExpressionTypeDeducer):
    """
    Type deduction implementation class that always selects the type of a
    specifc argument from the inputs based on an ordinal position.
    """

    def __init__(self, index: int):
        self._index: int = index

    @property
    def index(self) -> int:
        """
        The ordinal position of the argument that is always selected.
        """
        return self._index

    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        from pydough.qdag import PyDoughExpressionQDAG, PyDoughQDAGException

        msg: str = f"Cannot select type of argument {self.index!r} out of {args!r}"
        if self.index not in range(len(args)):
            raise PyDoughQDAGException(msg)
        arg = args[self.index]
        if isinstance(arg, PyDoughExpressionQDAG):
            return arg.pydough_type
        else:
            raise PyDoughQDAGException(msg)


class ConstantType(ExpressionTypeDeducer):
    """
    Type deduction implementation class that always returns a specific
    PyDough type.
    """

    def __init__(self, data_type: PyDoughType):
        self._data_type: PyDoughType = data_type

    @property
    def data_type(self) -> PyDoughType:
        """
        The type always inferred by this deducer.
        """
        return self._data_type

    def infer_return_type(self, args: list[Any]) -> PyDoughType:
        return self.data_type
