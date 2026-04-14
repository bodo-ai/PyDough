"""
A user-defined collection representing a database [temporary] view/table.
Usage:
`pydough.to_table(query, name='view_name', as_view=True, temp=False, replace=True)`

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
        is_view: bool = False,
        is_replace: bool = False,
        is_temp: bool = False,
        unique_columns: list[str | list[str]] | None = None,
        write_path: str | None = None,
    ) -> None:
        """
        Initialize a ViewGeneratedCollection.

        Args:
            name: The logical name of the collection used in PyDough queries
                (e.g., in ``per=`` strings). Must be a simple identifier with
                no dots.
            columns: List of column names (from the query result used to create
                the view/table)
            types: List of PyDoughType for each column
            is_view: True if this is a VIEW, False if it's a TABLE. Default
                is False (TABLE).
            is_temp: True if this is a temporary view/table. Default is False
                (permanent).
            unique_columns: List of unique column names or column combinations.
                If None, defaults to treating all columns together as a unique
                key.
            write_path: The fully-qualified SQL path used for DDL and FROM
                clauses (e.g., ``'db.schema.table'``). When provided, this is
                used instead of ``name`` in generated SQL, which lets PyDough
                continue to reference the collection by its short ``name``
                while writing to a qualified path. If ``None``, ``name`` is
                used in SQL as well.
        """
        super().__init__(name=name, columns=columns, types=types)
        self._is_view = is_view
        self._is_replace = is_replace
        self._is_temp = is_temp
        # Default to all columns as the unique key if not specified
        self._unique_columns: list[str | list[str]] = (
            unique_columns if unique_columns is not None else [columns]
        )
        self._write_path = write_path

    @property
    def sql_name(self) -> str:
        """
        The SQL path used in DDL statements and FROM clauses.

        Returns ``write_path`` when set, otherwise falls back to ``name``.
        """
        return self._write_path if self._write_path is not None else self.name

    @property
    def types(self) -> list[PyDoughType]:
        """Return a list containing the types of the columns."""
        return self._types

    @property
    def is_view(self) -> bool:
        """Return True if this is a view, False if it's a table."""
        return self._is_view

    @property
    def is_replace(self) -> bool:
        """Return True if this view/table was created with replace=True."""
        return self._is_replace

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
        Return the list of unique column names or column combinations.

        If not explicitly set, defaults to treating all columns together
        as a unique key (conservative assumption).
        """
        return self._unique_columns

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
        write_path_str = (
            f", write_path={self._write_path!r}" if self._write_path is not None else ""
        )
        return (
            f"{temp_str}{kind}GeneratedCollection("
            f"name={self.name!r}, "
            f"columns={self.columns!r}"
            f"{write_path_str}"
            f")"
        )

    def to_explanation(self, verbose: bool) -> list[str]:
        """Return explanation lines for the view/table collection."""
        kind = "view" if self._is_view else "table"
        temp_prefix = "temporary " if self._is_temp else ""
        lines = super().to_explanation(verbose)
        lines.insert(1, f"This collection is a materialized {temp_prefix}{kind}.")
        if verbose:
            lines.append(f"Created with replace={self._is_replace}")
        return lines

    def equals(self, other) -> bool:
        """Check if this collection is equal to another collection."""
        return (
            isinstance(other, ViewGeneratedCollection)
            and self.name == other.name
            and self.columns == other.columns
            and self.types == other.types
            and self._is_view == other._is_view
            and self._is_temp == other._is_temp
            and self._unique_columns == other._unique_columns
            and self._write_path == other._write_path
        )
