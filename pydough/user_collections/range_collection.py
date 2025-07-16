"""A user-defined collection of integers in a specified range.
Usage:
`pydough.range_collection(name, column, *args)`
    args: start, end, step

This module defines a collection that generates integers from `start` to `end`
with a specified `step`. The user must specify the name of the collection and the
name of the column that will hold the integer values.
"""

from typing import Any

from pydough.types import NumericType
from pydough.types.pydough_type import PyDoughType
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

all = ["RangeGeneratedCollection"]


class RangeGeneratedCollection(PyDoughUserGeneratedCollection):
    """Integer range-based collection."""

    def __init__(
        self,
        name: str,
        column_name: str,
        start: int | None,
        end: int | None,
        step: int | None,
    ) -> None:
        super().__init__(
            name=name,
            columns=[
                column_name,
            ],
            types=[NumericType()],
        )
        self._start = start if start is not None else 0
        self._end = end if end is not None else 0
        self._step = step if step is not None else 1
        self._range = range(self._start, self._end, self._step)

    @property
    def start(self) -> int | None:
        """Return the start of the range."""
        return self._start

    @property
    def end(self) -> int | None:
        """Return the end of the range."""
        return self._end

    @property
    def step(self) -> int | None:
        """Return the step of the range."""
        return self._step

    @property
    def range(self) -> range:
        """Return the range object representing the collection."""
        return self._range

    @property
    def column_names_and_types(self) -> list[tuple[str, PyDoughType]]:
        return [(self.columns[0], NumericType())]

    @property
    def data(self) -> Any:
        """Return the range as the data of the collection."""
        return self.range

    def __len__(self) -> int:
        if self.start is None or self.end is None or self.step is None:
            return 0
        elif self.start >= self.end:
            return 0
        else:
            return (self.end - self.start + self.step - 1) // self.step

    def always_non_empty(self) -> bool:
        """Check if the range collection is always non-empty."""
        return len(self) > 0

    def to_string(self) -> str:
        """Return a string representation of the range collection."""
        return f"RangeCollection({self.name}!r, {self.columns[0]}={self.range})"

    def equals(self, other) -> bool:
        if not isinstance(other, RangeGeneratedCollection):
            return False
        return (
            self.name == other.name
            and self.columns == other.columns
            and self.start == other.start
            and self.end == other.end
            and self.step == other.step
        )
