"""
Overridden version of the qualify.py, qualify_tables.py, and qualify_columns.py
files from sqlglot.
"""

from __future__ import annotations

import itertools
import typing as t

from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, DialectType
from sqlglot.dialects.sqlite import SQLite
from sqlglot.errors import OptimizeError
from sqlglot.helper import name_sequence, seq_get
from sqlglot.optimizer.isolate_table_selects import isolate_table_selects
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import (
    pushdown_cte_alias_columns,
)
from sqlglot.optimizer.qualify_columns import (
    quote_identifiers as quote_identifiers_func,
)
from sqlglot.optimizer.qualify_columns import (
    validate_qualify_columns as validate_qualify_columns_func,
)
from sqlglot.optimizer.qualify_columns import (
    _pop_table_column_aliases,
    _separate_pseudocolumns,
    _expand_using,
    _expand_alias_refs,
    _convert_columns_to_dots,
    _expand_stars,
    qualify_outputs,
    _expand_group_by,
    _expand_order_by_and_distinct_on,
)
from sqlglot.optimizer.resolver import Resolver
from sqlglot.optimizer.scope import Scope, traverse_scope
from sqlglot.schema import Schema, ensure_schema
from sqlglot.optimizer.annotate_types import TypeAnnotator

if t.TYPE_CHECKING:
    from sqlglot._typing import E
    from collections.abc import Sequence

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

    Returns:
        The qualified expression.
    """
    schema = ensure_schema(schema, dialect=dialect)
    dialect = Dialect.get_or_raise(dialect)

    expression = normalize_identifiers(
        expression, dialect=dialect, store_original_column_identifiers=True
    )
    expression = qualify_tables(
        expression,
        db=db,
        catalog=catalog,
        dialect=dialect,
    )

    if isolate_tables:
        expression = isolate_table_selects(expression, schema=schema)

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
        expression: Expr to qualify
        db: Database name
        catalog: Catalog name
        dialect: The dialect to parse catalog and schema into.

    Returns:
        The qualified expression.
    """
    dialect = Dialect.get_or_raise(dialect)
    next_alias_name = name_sequence("_")

    if db := db or None:
        db = exp.parse_identifier(db, dialect=dialect)
        db.meta["is_table"] = True
        db = normalize_identifiers(db, dialect=dialect)
    if catalog := catalog or None:
        catalog = exp.parse_identifier(catalog, dialect=dialect)
        catalog.meta["is_table"] = True
        catalog = normalize_identifiers(catalog, dialect=dialect)

    def _qualify(table: exp.Table) -> None:
        if isinstance(table.this, exp.Identifier):
            if db and not table.args.get("db"):
                table.set("db", db.copy())
            if catalog and not table.args.get("catalog") and table.args.get("db"):
                table.set("catalog", catalog.copy())

    if (db or catalog) and not isinstance(expression, exp.Query):
        with_ = expression.args.get("with_") or exp.With()
        cte_names = {cte.alias_or_name for cte in with_.expressions}

        for node in expression.walk(prune=lambda n: isinstance(n, exp.Query)):
            if isinstance(node, exp.Table) and node.name not in cte_names:
                _qualify(node)

    def _set_alias(
        expr: exp.Expr,
        target_alias: str | None = None,
        scope: Scope | None = None,
        normalize: bool = False,
        columns: Sequence[str | exp.Identifier | exp.ColumnDef] | None = None,
        quoted: bool | None = None,
    ) -> None:
        table_alias = expr.args.get("alias") or exp.TableAlias()

        if table_alias.name:
            return

        new_alias_name = target_alias or next_alias_name()
        if normalize and target_alias:
            new_alias_name = normalize_identifiers(new_alias_name, dialect=dialect).name

        alias_identifier = exp.to_identifier(new_alias_name)
        # PYDOUGH CHANGE: preserve quoting from the original table name
        # Example: keywords."CAST" should become keywords."CAST" AS "CAST"
        if quoted is not None:
            alias_identifier.set("quoted", quoted)
        table_alias.set("this", alias_identifier)

        if columns:
            table_alias.set(
                "columns",
                [
                    exp.to_identifier(c) if isinstance(c, str) else c.copy()
                    for c in columns
                ],
            )

        expr.set("alias", table_alias)

        if scope:
            scope.rename_source(None, new_alias_name)

    for scope in traverse_scope(expression):
        parent = scope.parent

        queries: list[exp.Expr] = list(scope.subqueries)

        # Subquery wrappers around a DML / DDL query fragment, e.g., a CREATE FUNCTION body or
        # an UPDATE's SET subquery, don't belong to any scope, so they aren't collected above
        if scope.is_root and isinstance(scope.expression, exp.Subquery):
            queries.append(scope.expression.unnest())
        elif scope.is_subquery:
            queries.append(scope.expression)

        for query in queries:
            subquery = query.parent
            if isinstance(subquery, exp.Subquery):
                unwrapped = subquery.unwrap()
                if isinstance(unwrapped.parent, (exp.From, exp.Join)):
                    # We can reach this from a wrapped derived table, which must keep its alias
                    continue

                if (
                    isinstance(unwrapped.parent, exp.Create)
                    and unwrapped is not subquery
                ):
                    # Function bodies may require wrapping parentheses, e.g. in BigQuery
                    # `... AS ((SELECT 1))` the outer parens delimit the body itself
                    unwrapped.set("this", subquery)
                else:
                    unwrapped.replace(subquery)

        for derived_table in scope.derived_tables:
            unnested = derived_table.unnest()
            if isinstance(unnested, exp.Table):
                joins = unnested.args.get("joins")
                unnested.set("joins", None)
                derived_table.this.replace(
                    exp.select("*").from_(unnested.copy(), copy=False)
                )
                derived_table.this.set("joins", joins)

            _set_alias(derived_table, scope=scope)
            if pivot := seq_get(derived_table.args.get("pivots") or [], -1):
                _set_alias(pivot)

        table_aliases = {}

        for name, source in scope.sources.items():
            # A source can appear in many scopes, e.g. as a lateral source of a UDTF scope or as a
            # CTE propagated to inner scopes. Deferring to the parent scope when it contains the
            # same source ensures each source is processed once, in the outermost scope that
            # contains it.
            if parent and parent.sources.get(name) is source:
                continue

            if isinstance(source, exp.Table):
                # When the name is empty, it means that we have a non-table source, e.g. a
                # pivoted cte
                is_real_table_source = bool(name)

                if pivot := seq_get(source.args.get("pivots") or [], -1):
                    name = source.name

                table_this = source.this
                table_alias = source.args.get("alias")

                # PYDOUGH CHANGE: preserve quoting from the original table name
                # Example: keywords."CAST" should become keywords."CAST" AS "CAST"
                # Only do this if the source is not aliased already and is not an
                # Anonymous expression, e.g. TABLE(GENERATOR(...)) is not a named
                # table.
                quoted = None
                if not table_alias and not isinstance(
                    table_this, (exp.Anonymous, exp.ExplodingGenerateSeries)
                ):
                    quoted = table_this.quoted

                _set_alias(
                    source,
                    target_alias=name or source.name or None,
                    normalize=True,
                    quoted=quoted,
                )

                source_fqn = ".".join(p.name for p in source.parts)
                had_explicit_alias = table_alias and table_alias.name
                if not had_explicit_alias or source_fqn not in table_aliases:
                    table_aliases[source_fqn] = source.args["alias"].this.copy()

                if pivot:
                    target_alias = source.alias if pivot.unpivot else None
                    _set_alias(pivot, target_alias=target_alias, normalize=True)

                    # This case corresponds to a pivoted CTE, we don't want to qualify that
                    if isinstance(scope.sources.get(source.alias_or_name), Scope):
                        continue

                if is_real_table_source:
                    _qualify(source)
            elif isinstance(source, Scope) and source.is_udtf:
                udtf = source.expression
                _set_alias(udtf)

                table_alias = udtf.args["alias"]

                if isinstance(udtf, exp.Values) and not table_alias.columns:
                    # PYDOUGH CHANGE: use the dialect's own default column
                    # naming convention for VALUES clauses instead of
                    # sqlglot's generic "_col_N" (e.g. SQLite uses
                    # "column1", "column2", ...).
                    if isinstance(dialect, SQLite):
                        raw_column_names = [
                            f"column{i + 1}"
                            for i in range(len(udtf.expressions[0].expressions))
                        ]
                    else:
                        raw_column_names = dialect.generate_values_aliases(udtf)
                    column_aliases = [
                        normalize_identifiers(
                            exp.to_identifier(name) if isinstance(name, str) else name,
                            dialect=dialect,
                        )
                        for name in raw_column_names
                    ]
                    table_alias.set("columns", column_aliases)
                elif isinstance(udtf, exp.TableFromRows) and not table_alias.columns:
                    default_columns = dialect.DEFAULT_FUNCTIONS_COLUMN_NAMES.get(
                        type(udtf.this)
                    )
                    if default_columns:
                        table_alias.set(
                            "columns",
                            [
                                normalize_identifiers(
                                    exp.to_identifier(c), dialect=dialect
                                )
                                for c in (
                                    default_columns
                                    if isinstance(default_columns, tuple)
                                    else [default_columns]
                                )
                            ],
                        )

        for table in scope.tables:
            if not table.alias and isinstance(table.parent, (exp.From, exp.Join)):
                _set_alias(table, target_alias=table.name)

        for column in scope.local_columns:
            if column.db:
                table_alias = table_aliases.get(
                    ".".join(p.name for p in column.parts[0:-1])
                )

                if table_alias:
                    for p in exp.COLUMN_PARTS[1:]:
                        column.set(p, None)

                    column.set("table", table_alias.copy())

    return expression


def qualify_columns_func(
    expression: exp.Expression,
    schema: t.Dict | Schema,
    expand_alias_refs: bool = True,
    expand_stars: bool = True,
    infer_schema: t.Optional[bool] = None,
    allow_partial_qualification: bool = False,
) -> exp.Expression:
    """
    Rewrite sqlglot AST to have fully qualified columns.

    Example:
        >>> import sqlglot
        >>> schema = {"tbl": {"col": "INT"}}
        >>> expression = sqlglot.parse_one("SELECT col FROM tbl")
        >>> qualify_columns(expression, schema).sql()
        'SELECT tbl.col AS col FROM tbl'

    Args:
        expression: Expression to qualify.
        schema: Database schema.
        expand_alias_refs: Whether to expand references to aliases.
        expand_stars: Whether to expand star queries. This is a necessary step
            for most of the optimizer's rules to work; do not set to False unless you
            know what you're doing!
        infer_schema: Whether to infer the schema if missing.
        allow_partial_qualification: Whether to allow partial qualification.

    Returns:
        The qualified expression.

    Notes:
        - Currently only handles a single PIVOT or UNPIVOT operator
    """
    schema = ensure_schema(schema)
    annotator = TypeAnnotator(schema)
    infer_schema = schema.empty if infer_schema is None else infer_schema
    dialect = schema.dialect or Dialect()
    pseudocolumns = dialect.PSEUDOCOLUMNS

    for scope in traverse_scope(expression):
        if dialect.PREFER_CTE_ALIAS_COLUMN:
            pushdown_cte_alias_columns(scope)

        scope_expression = scope.expression
        is_select = isinstance(scope_expression, exp.Select)

        _separate_pseudocolumns(scope, pseudocolumns)

        resolver = Resolver(scope, schema, infer_schema=infer_schema)
        _pop_table_column_aliases(scope.ctes)
        _pop_table_column_aliases(scope.derived_tables)
        using_column_tables = _expand_using(scope, resolver)

        if (
            schema.empty or dialect.FORCE_EARLY_ALIAS_REF_EXPANSION
        ) and expand_alias_refs:
            _expand_alias_refs(
                scope,
                resolver,
                dialect,
                expand_only_groupby=dialect.EXPAND_ONLY_GROUP_ALIAS_REF,
            )

        _convert_columns_to_dots(scope, resolver)
        _qualify_columns(
            scope,
            resolver,
            allow_partial_qualification=allow_partial_qualification,
        )

        # Refresh classification caches: a column just qualified in place may have been cached
        # as external
        scope.clear_column_cache()

        if not schema.empty and expand_alias_refs:
            _expand_alias_refs(scope, resolver, dialect)

        if is_select:
            if expand_stars:
                _expand_stars(
                    scope,
                    resolver,
                    using_column_tables,
                    pseudocolumns,
                    annotator,
                )
            qualify_outputs(scope, dialect=dialect)

        _expand_group_by(scope, dialect)

        # DISTINCT ON and ORDER BY follow the same rules (tested in DuckDB, Postgres, ClickHouse)
        # https://www.postgresql.org/docs/current/sql-select.html#SQL-DISTINCT
        _expand_order_by_and_distinct_on(scope, resolver)

        if dialect.ANNOTATE_ALL_SCOPES:
            annotator.annotate_scope(scope)

    return expression


def _qualify_columns(
    scope: Scope, resolver: Resolver, allow_partial_qualification: bool
) -> None:
    """Disambiguate columns, ensuring each column specifies a source"""
    # PYDOUGH CHANGE: using our custom get_scope_columns function instead of
    # scope.columns
    for column in get_scope_columns(scope):
        column_table = column.table
        column_name = column.name

        if column_table and column_table in scope.sources:
            source_columns = resolver.get_source_columns(column_table)
            if (
                not allow_partial_qualification
                and source_columns
                and column_name not in source_columns
                and "*" not in source_columns
            ):
                raise OptimizeError(f"Unknown column: {column_name}")

        if not column_table:
            if scope.pivots and not column.find_ancestor(exp.Pivot):
                # If the column is under the Pivot expression, we need to qualify it
                # using the name of the pivoted source instead of the pivot's alias
                column.set("table", exp.to_identifier(scope.pivots[0].alias))
                continue

            # column_table can be a '' because bigquery unnest has no table alias
            column_table = resolver.get_table(column_name)
            if column_table:
                column.set("table", column_table)

    for pivot in scope.pivots:
        for column in pivot.find_all(exp.Column):
            if not column.table and column.name in resolver.all_columns:
                column_table = resolver.get_table(column.name)
                if column_table:
                    column.set("table", column_table)


def get_scope_columns(scope: Scope) -> list[exp.Column]:
    """
    Pydouhh custom version of `scope.columns`. This function extract the columns
    from the given Scope.

    Args:
        `scope`: Sqlglot scope from which the function exacts the columns.

    Returns:
        A list of columns.
    """
    scope._ensure_collected()
    columns = scope._raw_columns

    external_columns = [
        column
        for scope in itertools.chain(
            scope.subquery_scopes,
            scope.udtf_scopes,
            (dts for dts in scope.derived_table_scopes if dts.can_be_correlated),
        )
        for column in scope.external_columns
    ]

    _columns = []
    for column in columns + external_columns:
        ancestor = column.find_ancestor(
            exp.Select,
            exp.Qualify,
            exp.Order,
            exp.Having,
            exp.Hint,
            exp.Table,
            exp.Star,
        )
        if (
            not ancestor
            or column.table
            or isinstance(ancestor, exp.Select)
            or (
                isinstance(ancestor, exp.Table)
                and not isinstance(ancestor.this, exp.Func)
            )
            or (
                isinstance(ancestor, exp.Order) or isinstance(ancestor, exp.Qualify)
                # PYDOUGH CHANGE: not checking for instance of Window or WithinGroup
                # or not column.name in named_selects. Allowing qualification
                # for columns in Qualify
            )
            or (isinstance(ancestor, exp.Star) and not column.arg_key == "except")
        ):
            _columns.append(column)

    return _columns
