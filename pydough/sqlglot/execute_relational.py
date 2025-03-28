"""
Class that converts the relational tree to the "executed" forms
of PyDough, which is either returns the SQL text or executes
the query on the database.
"""

from collections.abc import MutableSequence

import pandas as pd
from sqlglot import parse_one
from sqlglot.dialects import Dialect as SQLGlotDialect
from sqlglot.dialects import SQLite as SQLiteDialect
from sqlglot.expressions import Alias, Column
from sqlglot.expressions import Expression as SQLGlotExpression

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.logger import get_logger
from pydough.relational import JoinType, RelationalRoot
from pydough.relational.relational_expressions import (
    RelationalExpression,
)

from .join_type_relational_visitor import JoinTypeRelationalVisitor
from .sqlglot_relational_visitor import SQLGlotRelationalVisitor
from .transform_bindings import SqlGlotTransformBindings

__all__ = ["convert_relation_to_sql", "execute_df"]


def convert_relation_to_sql(
    relational: RelationalRoot,
    dialect: SQLGlotDialect,
    bindings: SqlGlotTransformBindings,
    config: PyDoughConfigs,
) -> str:
    """
    Convert the given relational tree to a SQL string using the given dialect.

    Args:
        `relational`: The relational tree to convert.
        `dialect`: The dialect to use for the conversion.
        `bindings`: The function bindings used for conversion.

    Returns:
        str: The SQL string representing the relational tree.
    """
    glot_expr: SQLGlotExpression = SQLGlotRelationalVisitor(
        dialect, bindings, config
    ).relational_to_sqlglot(relational)

    glot_expr = parse_one(glot_expr.sql(dialect))
    glot_expr = apply_sqlglot_optimizer(glot_expr, relational, dialect)

    return glot_expr.sql(dialect, pretty=True)


def apply_sqlglot_optimizer(
    glot_expr: SQLGlotExpression, relational: RelationalRoot, dialect: SQLGlotDialect
) -> SQLGlotExpression:
    """
    Apply the SQLGlot optimizer to the given SQLGlot expression.

    Args:
        glot_expr: The SQLGlot expression to optimize.
        relational: The relational tree to optimize the expression for.
        dialect: The dialect to use for the optimization.

    Returns:
        The optimized SQLGlot expression.
    """
    from sqlglot.optimizer.annotate_types import annotate_types
    from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
    from sqlglot.optimizer.eliminate_joins import eliminate_joins
    from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
    from sqlglot.optimizer.merge_subqueries import merge_subqueries
    from sqlglot.optimizer.normalize import normalize
    from sqlglot.optimizer.optimize_joins import optimize_joins
    from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
    from sqlglot.optimizer.qualify import qualify
    from sqlglot.optimizer.simplify import simplify

    # Apply each rule explicitly with appropriate kwargs
    glot_expr = qualify(
        glot_expr, dialect=dialect, quote_identifiers=False, isolate_tables=True
    )
    glot_expr = normalize(glot_expr)
    glot_expr = pushdown_predicates(glot_expr, dialect=dialect)
    glot_expr = optimize_joins(glot_expr)
    glot_expr = eliminate_subqueries(glot_expr)

    join_types = set(JoinTypeRelationalVisitor().get_join_types(relational))
    if JoinType.ANTI not in join_types and JoinType.SEMI not in join_types:
        glot_expr = merge_subqueries(glot_expr)

    glot_expr = eliminate_joins(glot_expr)
    glot_expr = eliminate_ctes(glot_expr)
    glot_expr = annotate_types(glot_expr, dialect=dialect)
    glot_expr = simplify(glot_expr, dialect=dialect)
    fix_column_case(glot_expr, relational.ordered_columns)
    return glot_expr


def fix_column_case(
    glot_expr: SQLGlotExpression,
    ordered_columns: MutableSequence[tuple[str, RelationalExpression]],
) -> None:
    """
    Fixes the column names in the SQLGlot expression to match the case in the original RelationalRoot.

    Args:
        glot_expr: The SQLGlot expression to fix
        ordered_columns: The ordered columns from the RelationalRoot
    """
    # Create a mapping of lowercase column names to their original case
    column_case_map = {col_name.lower(): col_name for col_name, _ in ordered_columns}
    # Fix column names in the top-level SELECT expressions
    if hasattr(glot_expr, "expressions"):
        for expr in glot_expr.expressions:
            # Handle expressions with aliases
            if isinstance(expr, Alias):
                identifier = expr.args.get("alias")
                alias_lower = identifier.this.lower()
                if alias_lower in column_case_map:
                    identifier.set("this", column_case_map[alias_lower])
            elif isinstance(expr, Column):
                col_lower = expr.this.this.lower()
                if col_lower in column_case_map:
                    expr.set("this", column_case_map[col_lower])


def convert_dialect_to_sqlglot(dialect: DatabaseDialect) -> SQLGlotDialect:
    """
    Convert the given DatabaseDialect to the corresponding SQLGlotDialect.

    Args:
        dialect (DatabaseDialect): The dialect to convert.

    Returns:
        SQLGlotDialect: The corresponding SQLGlot dialect.
    """
    if dialect == DatabaseDialect.ANSI:
        # Note: ANSI is the base dialect for SQLGlot.
        return SQLGlotDialect()
    elif dialect == DatabaseDialect.SQLITE:
        return SQLiteDialect()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


def execute_df(
    relational: RelationalRoot,
    ctx: DatabaseContext,
    bindings: SqlGlotTransformBindings,
    config: PyDoughConfigs,
    display_sql: bool = False,
) -> pd.DataFrame:
    """
    Execute the given relational tree on the given database access
    context and return the result.

    Args:
        `relational`: The relational tree to execute.
        `ctx`: The database context to execute the query in.
        `bindings`: The function transformation bindings used to convert
        PyDough operators into SQLGlot expressions.
        `display_sql`: if True, prints out the SQL that will be run before
        it is executed.

    Returns:
        The result of the query as a Pandas DataFrame
    """
    sqlglot_dialect: SQLGlotDialect = convert_dialect_to_sqlglot(ctx.dialect)
    sql: str = convert_relation_to_sql(relational, sqlglot_dialect, bindings, config)
    if display_sql:
        pyd_logger = get_logger(__name__)
        pyd_logger.info(f"SQL query:\n {sql}")
    return ctx.connection.execute_query_df(sql)
