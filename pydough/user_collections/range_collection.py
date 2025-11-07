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
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

all = ["RangeGeneratedCollection"]


class RangeGeneratedCollection(PyDoughUserGeneratedCollection):
    """Integer range-based collection."""

    def __init__(
        self,
        name: str,
        column_name: str,
        range: range,
    ) -> None:
        super().__init__(
            name=name,
            columns=[
                column_name,
            ],
            types=[NumericType()],
        )
        self._range = range
        self._start = self._range.start
        self._end = self._range.stop
        self._step = self._range.step

    @property
    def start(self) -> int:
        """Return the start of the range."""
        return self._start

    @property
    def end(self) -> int:
        """Return the end of the range."""
        return self._end

    @property
    def step(self) -> int:
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
    def column_name(self) -> str:
        return self.columns[0]

    @property
    def unique_column_names(self) -> list[str]:
        return [self.columns[0]]

    def __len__(self) -> int:
        return len(self._range)

    def is_singular(self) -> bool:
        """Returns True if the collection is guaranteed to contain at most one row."""
        return len(self) <= 1

    def always_exists(self) -> bool:
        """Check if the range collection is always non-empty."""
        return len(self) > 0

    def to_string(self) -> str:
        """Return a string representation of the range collection."""
        return f"RangeCollection({self.name!r}, {self.columns[0]}={self.range})"

    def equals(self, other) -> bool:
        return (
            isinstance(other, RangeGeneratedCollection)
            and self.name == other.name
            and self.columns == other.columns
            and self.start == other.start
            and self.end == other.end
            and self.step == other.step
        )
