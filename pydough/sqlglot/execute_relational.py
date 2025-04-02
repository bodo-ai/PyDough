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
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.eliminate_ctes import eliminate_ctes
from sqlglot.optimizer.eliminate_joins import eliminate_joins
from sqlglot.optimizer.eliminate_subqueries import eliminate_subqueries
from sqlglot.optimizer.merge_subqueries import merge_subqueries
from sqlglot.optimizer.normalize import normalize
from sqlglot.optimizer.optimize_joins import optimize_joins
from sqlglot.optimizer.pushdown_predicates import pushdown_predicates
from sqlglot.optimizer.pushdown_projections import pushdown_projections
from sqlglot.optimizer.qualify import qualify
from sqlglot.optimizer.qualify_columns import quote_identifiers
from sqlglot.optimizer.simplify import simplify

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
)
from pydough.logger import get_logger
from pydough.relational import JoinType, JoinTypeRelationalVisitor, RelationalRoot
from pydough.relational.relational_expressions import (
    RelationalExpression,
)

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
    glot_expr = parse_one(glot_expr.sql(dialect), dialect=dialect)
    glot_expr = apply_sqlglot_optimizer(glot_expr, relational, dialect)
    # print(glot_expr.sql(dialect, pretty=True))
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
    # Apply each rule explicitly with appropriate kwargs
    # TODO: (gh #313) - Two rules that sqlglot optimizer provides are skipped
    # (unnest_subqueries and canonicalize). Additionally, the rule `merge_subqueries`
    # is skipped if the AST has a `semi` or `anti` join.

    # Rewrite sqlglot AST to have normalized and qualified tables and columns.
    glot_expr = qualify(
        glot_expr, dialect=dialect, quote_identifiers=False, isolate_tables=True
    )

    # Rewrite sqlglot AST to remove unused columns projections.
    glot_expr = pushdown_projections(glot_expr)

    # Rewrite sqlglot AST into conjunctive normal form
    glot_expr = normalize(glot_expr)

    # TODO: (gh #313) RULE SKIPPED
    # Rewrite sqlglot AST to convert some predicates with subqueries into joins.
    # Convert scalar subqueries into cross joins.
    # Convert correlated or vectorized subqueries into a group by so it is not
    # a many to many left join.
    # glot_expr = unnest_subqueries(glot_expr)

    # Rewrite sqlglot AST to pushdown predicates in FROMS and JOINS.
    glot_expr = pushdown_predicates(glot_expr, dialect=dialect)

    # Removes cross joins if possible and reorder joins based on predicate
    # dependencies.
    glot_expr = optimize_joins(glot_expr)

    # Rewrite derived tables as CTES, deduplicating if possible.
    glot_expr = eliminate_subqueries(glot_expr)

    # TODO: (gh #313) RULE SKIPPED for `semi` and `anti` joins.
    join_types = JoinTypeRelationalVisitor().get_join_types(relational)
    if JoinType.ANTI not in join_types and JoinType.SEMI not in join_types:
        glot_expr = merge_subqueries(glot_expr)

    # Remove unused joins from an expression.
    # This only removes joins when we know that the join condition doesn't
    # produce duplicate rows.
    glot_expr = eliminate_joins(glot_expr)

    # Remove unused CTEs from an expression.
    glot_expr = eliminate_ctes(glot_expr)

    # NEW: Makes sure all identifiers that need to be quoted are quoted.
    glot_expr = quote_identifiers(glot_expr, dialect=dialect)

    # Infers the types of an expression, annotating its AST accordingly.
    # depends on the schema.
    glot_expr = annotate_types(glot_expr, dialect=dialect)

    # TODO: (gh #313) RULE SKIPPED
    # Converts a sql expression into a standard form.
    # This method relies on annotate_types because many of the
    # conversions rely on type inference.
    # glot_expr = canonicalize(glot_expr, dialect=dialect)

    # Rewrite sqlglot AST to simplify expressions.
    glot_expr = simplify(glot_expr, dialect=dialect)

    # Fix column names in the top-level SELECT expressions.
    # The optimizer changes the cases of column names, so we need to
    # match the alias in the relational tree.
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
    # Fix column names in the top-level SELECT expressions
    if hasattr(glot_expr, "expressions"):
        for idx, (col_name, _) in enumerate(ordered_columns):
            expr = glot_expr.expressions[idx]
            # Handle expressions with aliases
            if isinstance(expr, Alias):
                identifier = expr.args.get("alias")
                identifier.set("this", col_name)
            elif isinstance(expr, Column):
                expr.this.this.set("this", col_name)


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
