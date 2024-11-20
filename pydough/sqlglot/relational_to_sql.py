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

# Cache a visitor for the module
_visitor: SQLGlotRelationalVisitor = SQLGlotRelationalVisitor()


def convert_relation_to_sql(relational: RelationalRoot, dialect: SQLGlotDialect) -> str:
    glot_expr: SQLGlotExpression = _visitor.relational_to_sqlglot(relational)
    return glot_expr.sql(dialect)
