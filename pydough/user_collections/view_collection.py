"""
A user-defined collection representing a database [temporary] view/table.
Usage:
`pydough.to_table(query, name='view_name', is_view=True, is_temp=False)`

This module defines a collection that represents a materialized
[temporary] view/table created in the database from a PyDough query result.
The view/table can be used in subsequent PyDough queries within the same session.
"""

from pydough.types.pydough_type import PyDoughType
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection

__all__ = ["ViewGeneratedCollection"]


class ViewGeneratedCollection(PyDoughUserGeneratedCollection):
    """
    Represents a database [temporary] view/table created from a PyDough query.

    This collection is used to reference views/tables that have been materialized
    in the database and can be used in subsequent PyDough queries.
    """

    def __init__(
        self,
        name: str,
        columns: list[str],
        types: list[PyDoughType],
        is_view: bool = True,
        is_temp: bool = False,
    ) -> None:
        """
        Initialize a ViewGeneratedCollection.

        Args:
            name: The name of the view/table in the database (can be 'db.schema.name')
            columns: List of column names (from the query result used to create the view/table)
            types: List of PyDoughType for each column
            is_view: True if this is a VIEW, False if it's a TABLE. Default is True (VIEW).
            is_temp: True if this is a temporary view/table. Default is False (permanent).
        """
        super().__init__(name=name, columns=columns, types=types)
        self._is_view = is_view
        self._is_temp = is_temp

    @property
    def types(self) -> list[PyDoughType]:
        """Return a list containing the types of the columns."""
        return self._types

    @property
    def is_view(self) -> bool:
        """Return True if this is a view, False if it's a table."""
        return self._is_view

    @property
    def is_temp(self) -> bool:
        """Return True if this is a temporary view/table."""
        return self._is_temp

    @property
    def column_names_and_types(self) -> list[tuple[str, PyDoughType]]:
        """Return column names and their types."""
        assert len(self.columns) == len(self.types)
        return list(zip(self.columns, self.types))

    @property
    def unique_column_names(self) -> list[str | list[str]]:
        """
        Return the list of unique column names.

        For views/tables, we cannot determine uniqueness constraints
        from the query alone, so we return an empty list.
        """
        return []

    def is_singular(self) -> bool:
        """
        Returns True if the collection is guaranteed to contain at most one row.

        For views/tables, we cannot determine this statically, so we return False.
        """
        return False

    def always_exists(self) -> bool:
        """
        Check if the view/table collection is always non-empty.

        For views/tables, we cannot determine this statically, so we return False.
        """
        return False

    def to_string(self) -> str:
        """Return a string representation of the view collection."""
        kind = "View" if self._is_view else "Table"
        temp_str = "Temp" if self._is_temp else ""
        return (
            f"{temp_str}{kind}GeneratedCollection("
            f"name={self.name!r}, "
            f"columns={self.columns!r}"
            f")"
        )

    def equals(self, other) -> bool:
        """Check if this collection is equal to another collection."""
        return (
            isinstance(other, ViewGeneratedCollection)
            and self.name == other.name
            and self.columns == other.columns
            and self.types == other.types
            and self._is_view == other._is_view
            and self._is_temp == other._is_temp
        )
