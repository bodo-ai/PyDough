"""
This file contains the relational implementation for a variation of a scan node
that, instead of reading a base table, it reads the result of a SQL query.
"""

from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .abstract_node import RelationalNode
from .scan import Scan


class SqlQueryScan(Scan):
    """
    A variation of the Scan node in the relational tree that reads the result of
    a SQL query instead of a base table. This is treated as identical to a
    regular scan, except that the table path contains a query. Other operations
    throughout the various optimization pipelines for relational nodes are
    unaffected because they use the node/node_copy APIs, rather than directly
    creating a new SCAN instance. When a SqlQueryScan is copied, the resulting
    scan will also be a SqlQueryScan.
    """

    def __init__(
        self,
        sql_query: str,
        columns: dict[str, RelationalExpression],
        unique_sets: set[frozenset[str]] | None = None,
    ) -> None:
        super().__init__(sql_query, columns, unique_sets)

    @property
    def sql_query(self) -> str:
        return self.table_name

    def to_string(self, compact=False) -> str:
        return f"SQLQUERYSCAN(table={self.table_name}, columns={self.make_column_string(self.columns, compact)})"

    def node_copy(
        self,
        columns: dict[str, RelationalExpression],
        inputs: list[RelationalNode],
    ) -> RelationalNode:
        assert not inputs, "SqlQueryScan node should have 0 inputs"
        return SqlQueryScan(self.table_name, columns, self._unique_sets)
