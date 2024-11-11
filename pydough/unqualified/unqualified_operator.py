"""
TODO: add file-level docstring
"""

__all__ = [
    "UnqualifiedOperator",
    "SUM",
    "COUNT",
    "LOWER",
    "UPPER",
    "STARTSWITH",
    "ENDSWITH",
    "CONTAINS",
]

from collections.abc import MutableSequence

from .unqualified_node import UnqualifiedNode, UnqualifiedOperation


class UnqualifiedOperator:
    """
    Class for
    """

    def __init__(self, operation_name: str):
        self._operation_name: str = operation_name
        self.__name__ = self._operation_name

    @property
    def operation_name(self) -> str:
        """
        The name of the operation being done.
        """
        return self._operation_name

    def __call__(self, *args):
        unqualified_args: MutableSequence[UnqualifiedNode] = [
            UnqualifiedNode.coerce_to_unqualified(arg) for arg in args
        ]
        return UnqualifiedOperation(self.operation_name, unqualified_args)

    def __repr__(self):
        return f"{self.operation_name}"


SUM = UnqualifiedOperator("SUM")
COUNT = UnqualifiedOperator("COUNT")
LOWER = UnqualifiedOperator("LOWER")
UPPER = UnqualifiedOperator("UPPER")
STARTSWITH = UnqualifiedOperator("STARTSWITH")
ENDSWITH = UnqualifiedOperator("ENDSWITH")
CONTAINS = UnqualifiedOperator("CONTAINS")
