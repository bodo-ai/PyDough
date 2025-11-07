"""
Overridden version of the qualify.py and qualify_tables.py file from sqlglot.
"""

from __future__ import annotations

import itertools
import typing as t

from sqlglot import alias, exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.helper import csv_reader, name_sequence
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import (
    pushdown_cte_alias_columns as pushdown_cte_alias_columns_func,
)
from sqlglot.optimizer.qualify_columns import (
    qualify_columns as qualify_columns_func,
)
from sqlglot.optimizer.qualify_columns import (
    quote_identifiers as quote_identifiers_func,
)
from sqlglot.optimizer.qualify_columns import (
    validate_qualify_columns as validate_qualify_columns_func,
)
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import Schema, ensure_schema

if t.TYPE_CHECKING:
    from sqlglot._typing import E

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def qualify(
    expression: exp.Expression,
    dialect: DialectType = None,
    db: str | None = None,
    catalog: str | None = None,
    schema: dict | Schema | None = None,
    expand_alias_refs: bool = True,
    expand_stars: bool = True,
    infer_schema: bool | None = None,
    isolate_tables: bool = False,
    qualify_columns: bool = True,
    allow_partial_qualification: bool = False,
    validate_qualify_columns: bool = True,
    quote_identifiers: bool = True,
    identify: bool = True,
    infer_csv_schemas: bool = False,
) -> exp.Expression:
    """
    Rewrite sqlglot AST to have normalized and qualified tables and columns.

    This step is necessary for all further SQLGlot optimizations.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify(expression, schema=schema).sql()
        'SELECT "tbl"."col" AS "col" FROM "tbl" AS "tbl"'

    Args:
        expression: Expression to qualify.
        db: Default database name for tables.
        catalog: Default catalog name for tables.
        schema: Schema to infer column names and types.
        expand_alias_refs: Whether to expand references to aliases.
        expand_stars: Whether to expand star queries. This is a necessary step
            for most of the optimizer's rules to work; do not set to False unless you
            know what you're doing!
        infer_schema: Whether to infer the schema if missing.
        isolate_tables: Whether to isolate table selects.
        qualify_columns: Whether to qualify columns.
        allow_partial_qualification: Whether to allow partial qualification.
        validate_qualify_columns: Whether to validate columns.
        quote_identifiers: Whether to run the quote_identifiers step.
            This step is necessary to ensure correctness for case sensitive queries.
            But this flag is provided in case this step is performed at a later time.
        identify: If True, quote all identifiers, else only necessary ones.
        infer_csv_schemas: Whether to scan READ_CSV calls in order to infer the CSVs' schemas.

    Returns:
        The qualified expression.
    """
    schema = ensure_schema(schema, dialect=dialect)
    expression = qualify_tables(
        expression,
        db=db,
        catalog=catalog,
        schema=schema,
        dialect=dialect,
        infer_csv_schemas=infer_csv_schemas,
    )
    expression = normalize_identifiers(expression, dialect=dialect)

    if isolate_tables:
        expression = isolate_table_selects(expression, schema=schema)

    if Dialect.get_or_raise(dialect).PREFER_CTE_ALIAS_COLUMN:
        expression = pushdown_cte_alias_columns_func(expression)

    if qualify_columns:
        expression = qualify_columns_func(
            expression,
            schema,
            expand_alias_refs=expand_alias_refs,
            expand_stars=expand_stars,
            infer_schema=infer_schema,
            allow_partial_qualification=allow_partial_qualification,
        )

    if quote_identifiers:
        expression = quote_identifiers_func(
            expression, dialect=dialect, identify=identify
        )

    if validate_qualify_columns:
        validate_qualify_columns_func(expression)

    return expression


def qualify_tables(
    expression: E,
    db: str | exp.Identifier | None = None,
    catalog: str | exp.Identifier | None = None,
    schema: Schema | None = None,
    infer_csv_schemas: bool = False,
    dialect: DialectType = None,
) -> E:
    """
    Rewrite sqlglot AST to have fully qualified tables. Join constructs such as
    (t1 JOIN t2) AS t will be expanded into (SELECT * FROM t1 AS t1, t2 AS t2) AS t.

    Examples:
        >>> import sqlglot
        >>> expression = sqlglot.parse_one("SELECT 1 FROM tbl")
        >>> qualify_tables(expression, db="db").sql()
        'SELECT 1 FROM db.tbl AS tbl'
        >>>
        >>> expression = sqlglot.parse_one("SELECT 1 FROM (t1 JOIN t2) AS t")
        >>> qualify_tables(expression).sql()
        'SELECT 1 FROM (SELECT * FROM t1 AS t1, t2 AS t2) AS t'

    Args:
        expression: Expression to qualify
        db: Database name
        catalog: Catalog name
        schema: A schema to populate
        infer_csv_schemas: Whether to scan READ_CSV calls in order to infer the CSVs' schemas.
        dialect: The dialect to parse catalog and schema into.

    Returns:
        The qualified expression.
    """
    next_alias_name = name_sequence("_q_")
    db = exp.parse_identifier(db, dialect=dialect) if db else None
    catalog = exp.parse_identifier(catalog, dialect=dialect) if catalog else None

    def _qualify(table: exp.Table) -> None:
        if isinstance(table.this, exp.Identifier):
            if not table.args.get("db"):
                table.set("db", db)
            if not table.args.get("catalog") and table.args.get("db"):
                table.set("catalog", catalog)

    if (db or catalog) and not isinstance(expression, exp.Query):
        for node in expression.walk(prune=lambda n: isinstance(n, exp.Query)):
            if isinstance(node, exp.Table):
                _qualify(node)

    for scope in traverse_scope(expression):
        for derived_table in itertools.chain(scope.ctes, scope.derived_tables):
            if isinstance(derived_table, exp.Subquery):
                unnested = derived_table.unnest()
                if isinstance(unnested, exp.Table):
                    joins = unnested.args.pop("joins", None)
                    derived_table.this.replace(
                        exp.select("*").from_(unnested.copy(), copy=False)
                    )
                    derived_table.this.set("joins", joins)

            if not derived_table.args.get("alias"):
                alias_ = next_alias_name()
                derived_table.set(
                    "alias", exp.TableAlias(this=exp.to_identifier(alias_))
                )
                scope.rename_source(None, alias_)

            pivots = derived_table.args.get("pivots")
            if pivots and not pivots[0].alias:
                pivots[0].set(
                    "alias", exp.TableAlias(this=exp.to_identifier(next_alias_name()))
                )

        table_aliases = {}

        for name, source in scope.sources.items():
            if isinstance(source, exp.Table):
                pivots = source.args.get("pivots")
                quoted = None
                if not source.alias:
                    # Don't add the pivot's alias to the pivoted table, use the table's name instead
                    if pivots and pivots[0].alias == name:
                        name = source.name

                    # PYDOUGH CHANGE: preserve quoting from the original table name
                    # Example: keywords."CAST" should become keywords."CAST" AS "CAST"
                    # Only do this if the source is not an Anonymous expression
                    # e.g. TABLE(GENERATOR(...)) is not a named table
                    quoted = (
                        source.this.quoted
                        if not isinstance(source.this, exp.Anonymous)
                        else quoted
                    )

                    # Mutates the source by attaching an alias to it
                    # PYDOUGH CHANGE: pass along quoting information
                    alias(
                        source,
                        name or source.name or next_alias_name(),
                        copy=False,
                        table=True,
                        quoted=quoted,
                    )

                table_aliases[".".join(p.name for p in source.parts)] = (
                    exp.to_identifier(source.alias)
                )

                if pivots:
                    if not pivots[0].alias:
                        pivot_alias = next_alias_name()
                        pivots[0].set(
                            "alias", exp.TableAlias(this=exp.to_identifier(pivot_alias))
                        )

                    # This case corresponds to a pivoted CTE, we don't want to qualify that
                    if isinstance(scope.sources.get(source.alias_or_name), Scope):
                        continue

                _qualify(source)

                if (
                    infer_csv_schemas
                    and schema
                    and isinstance(source.this, exp.ReadCSV)
                ):
                    with csv_reader(source.this) as reader:
                        header = next(reader)
                        columns = next(reader)
                        schema.add_table(
                            source,
                            {k: type(v).__name__ for k, v in zip(header, columns)},
                            match_depth=False,
                        )
            elif isinstance(source, Scope) and source.is_udtf:
                udtf = source.expression
                table_alias = udtf.args.get("alias") or exp.TableAlias(
                    this=exp.to_identifier(next_alias_name())
                )
                udtf.set("alias", table_alias)

                if not table_alias.name:
                    table_alias.set("this", exp.to_identifier(next_alias_name()))
                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    for i, e in enumerate(udtf.expressions[0].expressions):
                        table_alias.append("columns", exp.to_identifier(f"_col_{i}"))
            else:
                for node in scope.walk():
                    if (
                        isinstance(node, exp.Table)
                        and not node.alias
                        and isinstance(node.parent, (exp.From, exp.Join))
                    ):
                        # Mutates the table by attaching an alias to it
                        alias(node, node.name, copy=False, table=True)

        for column in scope.columns:
            if column.db:
                table_alias = table_aliases.get(
                    ".".join(p.name for p in column.parts[0:-1])
                )

                if table_alias:
                    for p in exp.COLUMN_PARTS[1:]:
                        column.set(p, None)
                    column.set("table", table_alias)

    return expression
