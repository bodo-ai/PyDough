"""A user-defined collection of integers in a specified range.
Usage:
`pydough.range_collection(name, column, *args)`
    args: start, end, step

This module defines a collection that generates integers from `start` to `end`
with a specified `step`. The user must specify the name of the collection and the
name of the column that will hold the integer values.
"""

from pydough.types import NumericType
from pydough.types.pydough_type import PyDoughType

from .user_collections import PyDoughUserGeneratedCollection

all = ["RangeGeneratedCollection"]


class RangeGeneratedCollection(PyDoughUserGeneratedCollection):
    """Integer range-based collection."""

    # HA_Q: should start/end/step be int or PyDoughQDAG? Why?
    def __init__(
        self,
        name: str,
        column_name: str,
        start: int,
        end: int,
        step: int,
    ) -> None:
        super().__init__(name=name, columns=[column_name])
        self.start = start
        self.end = end
        self.step = step

    @property
    def column_names_and_types(self) -> list[tuple[str, PyDoughType]]:
        return [(self.columns[0], NumericType())]

    def __len__(self) -> int:
        if self.start >= self.end:
            return 0
        return (self.end - self.start + self.step - 1) // self.step

    def is_empty(self) -> bool:
        """Check if the range collection is empty."""
        return len(self) == 0

    def to_string(self) -> str:
        return f"RangeCollection({self.name}: {self.columns[0]} from {self.start} to {self.end} step {self.step})"
