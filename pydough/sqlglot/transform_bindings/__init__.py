"""
Submodule of the sqlglot module dedicated to the logic that transforms
invocations of PyDough function operators into SQLGlot function calls.
"""

__all__ = ["BaseTransformBindings", "SQLiteTransformBindings", "bindings_from_dialect"]

from typing import TYPE_CHECKING

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseDialect

from .base_transform_bindings import BaseTransformBindings
from .sqlite_transform_bindings import SQLiteTransformBindings

if TYPE_CHECKING:
    from pydough.sqlglot.sqlglot_relational_visitor import SQLGlotRelationalVisitor


def bindings_from_dialect(
    dialect: DatabaseDialect,
    configs: PyDoughConfigs,
    visitor: "SQLGlotRelationalVisitor",
) -> BaseTransformBindings:
    """
    Returns a binding instance corresponding to a specific database
    dialect.

    Args:
        `dialect`: the database dialect that the bindings should be
        created for.
        `configs`: the settings being used during hte conversion.

    Returns:
        A binding instance for the specified dialect.
    """
    match dialect:
        case DatabaseDialect.ANSI:
            return BaseTransformBindings(configs, visitor)
        case DatabaseDialect.SQLITE:
            return SQLiteTransformBindings(configs, visitor)
        case _:
            raise NotImplementedError(f"Unsupported dialect: {dialect}")
