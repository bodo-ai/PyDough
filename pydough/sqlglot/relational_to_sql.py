"""
Class that converts the relational tree to a SQL implementation
via SQLGlot. This class acts as the main connector between the
relational tree and the SQLGlot library.
"""

from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.relational.relational_nodes import RelationalRoot

from .sqlglot_relational_visitor import SQLGlotRelationalVisitor

__all__ = ["convert_relation_to_sql"]


def convert_relation_to_sql(relational: RelationalRoot, dialect: SQLGlotDialect) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        relational (RelationalRoot): The relational tree to convert.
        dialect (SQLGlotDialect): The dialect to use for the conversion.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor().relational_to_sqlglot(
        relational
    )
    return glot_expr.sql(dialect)
