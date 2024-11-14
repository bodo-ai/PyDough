"""
This file contains the relational implementation for a "scan" node, which generally
represents any "base table" in relational algebra. As we expand to more types of
"tables" (e.g. constant table, in memory table) this Scan node may serve as a parent
class for more specific implementations.
"""

from collections.abc import MutableMapping

from .abstract import Relational
from .relational_expressions import RelationalExpression


class Scan(Relational):
    """
    The Scan node in the relational tree. Right now these refer to tables
    stored within a provided database connection with is assumed to be singular
    and always available.
    """

    def __init__(
        self, table_name: str, columns: MutableMapping[str, RelationalExpression]
    ) -> None:
        self.table_name: str = table_name
        self._columns: MutableMapping[str, RelationalExpression] = columns

    @property
    def inputs(self):
        # A scan is required to be the leaf node of the relational tree.
        return []

    @property
    def columns(self) -> MutableMapping[str, RelationalExpression]:
        return self._columns

    def equals(self, other: "Relational") -> bool:
        return (
            isinstance(other, Scan)
            and self.table_name == other.table_name
            and self.columns == other.columns
        )

    def to_string(self) -> str:
        return f"SCAN(table={self.table_name}, columns={self.columns})"
