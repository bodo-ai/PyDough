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
    "LIKE",
    "MIN",
    "MAX",
    "YEAR",
]

from collections.abc import MutableSequence

from .unqualified_node import UnqualifiedNode, UnqualifiedOperation


class UnqualifiedOperator:
    """
    Class for a PyDough function call. Instances of this class can be invoked
    at the top level by PyDough code when imported.
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
AVG = UnqualifiedOperator("AVG")
MIN = UnqualifiedOperator("MIN")
MAX = UnqualifiedOperator("MAX")
COUNT = UnqualifiedOperator("COUNT")
IFF = UnqualifiedOperator("IFF")
LOWER = UnqualifiedOperator("LOWER")
UPPER = UnqualifiedOperator("UPPER")
YEAR = UnqualifiedOperator("YEAR")
LIKE = UnqualifiedOperator("LIKE")
STARTSWITH = UnqualifiedOperator("STARTSWITH")
ENDSWITH = UnqualifiedOperator("ENDSWITH")
CONTAINS = UnqualifiedOperator("CONTAINS")
