import warnings

import pydough
from pydough.configs import PyDoughSession
from pydough.conversion import convert_ast_to_relational
from pydough.database_connectors import CreateCapabilities, DatabaseDialect
from pydough.errors import PyDoughSessionException
from pydough.errors.error_utils import is_valid_sql_name
from pydough.logger import get_logger
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.qdag.collections.calculate import Calculate
from pydough.qdag.expressions.reference import Reference
from pydough.relational import RelationalRoot
from pydough.sqlglot import convert_relation_to_sql
from pydough.sqlglot.sqlglot_helpers import normalize_column_name
from pydough.types import PyDoughType
from pydough.unqualified import UnqualifiedNode, qualify_node
from pydough.unqualified.unqualified_node import UnqualifiedGeneratedCollection
from pydough.user_collections.view_collection import ViewGeneratedCollection

from .evaluate_unqualified import _load_session_info

__all__ = ["to_table"]


def _infer_schema_from_relational(
    relational: RelationalRoot,
) -> tuple[list[str], list[PyDoughType]]:
    """
    Infer the schema (column names and types) from a RelationalRoot node.

    Args:
        relational: The root of the relational tree.

    Returns:
        A tuple of (column_names, column_types) where:
        - column_names is a list of column name strings
        - column_types is a list of PyDoughType objects
    """

    column_names: list[str] = []
    column_types: list[PyDoughType] = []

    for col_name, col_expr in relational.ordered_columns:
        column_names.append(col_name)
        column_types.append(col_expr.data_type)

    return column_names, column_types


def _generate_create_ddl(
    name: str,
    sql: str,
    as_view: bool,
    replace: bool,
    temp: bool,
    db_dialect: DatabaseDialect,
) -> tuple[list[str], bool]:
    """
    Generate the CREATE DDL statement(s) for a view or table.

    Args:
        name: The name of the view/table (can be 'db.schema.name')
        sql: The SQL query to use as the view/table definition
        as_view: True to create a VIEW, False to create a TABLE
        replace: True to use CREATE OR REPLACE
        temp: True to create a TEMPORARY view/table
        db_dialect: The database dialect to generate the DDL for

    Returns:
        A tuple of (ddl_statements, actual_temp) where:
        - ddl_statements is a list of DDL strings to execute in order
        - actual_temp is the final temp value (may differ from input due to dialect limitations)
    """
    # Handle differences in CREATE syntax for different databases.
    create_caps: CreateCapabilities = db_dialect.create_capabilities
    object_type = "VIEW" if as_view else "TABLE"
    ddl_statements: list[str] = []

    # Oracle uses PRIVATE TEMPORARY TABLE (Oracle 18c+), not standard TEMPORARY TABLE.
    # The ORA$PTT_ prefix is required by Oracle for private temporary tables.
    # ON COMMIT PRESERVE DEFINITION keeps the table structure for the session.
    if temp and not as_view and db_dialect == DatabaseDialect.ORACLE:
        if replace:
            ddl_statements.append(f"DROP TABLE IF EXISTS {name}")
        ddl_statements.append(
            f"CREATE PRIVATE TEMPORARY TABLE {name} ON COMMIT PRESERVE DEFINITION AS {sql}"
        )
        return ddl_statements, temp

    if temp:
        allowed = create_caps.temp_view if as_view else create_caps.temp_table
        if not allowed:
            raise PyDoughSessionException(
                f"TEMPORARY {object_type} is not supported for {db_dialect.name}"
            )

    if as_view and not temp and db_dialect == DatabaseDialect.SQLITE:
        # Sqlite does not support creating persistent views that reference attached databases.
        # like tpch.order Only temporary views are supported.
        # Override to be temporary and raise a warning.
        temp = True
        warnings.warn(
            "SQLite does not support creating persistent views that reference attached databases. "
            "Only temporary views are supported. The view will be created as TEMPORARY."
        )

    # Check if we can use CREATE OR REPLACE
    can_replace: bool = (
        create_caps.replace_view if as_view else create_caps.replace_table
    )

    # For databases that don't support CREATE OR REPLACE TABLE/VIEW,
    # use DROP TABLE/VIEW IF EXISTS + CREATE TABLE/VIEW pattern
    if replace and not can_replace:
        drop_stmt = f"DROP {object_type} IF EXISTS {name}"
        ddl_statements.append(drop_stmt)
        # Don't add OR REPLACE since we're using DROP first
        replace = False

    create = "CREATE"
    if replace and can_replace:
        create += " OR REPLACE"
    if temp:
        create += " TEMPORARY"

    create += f" {object_type}"

    ddl_statements.append(f"{create} {name} AS {sql}")

    return ddl_statements, temp


def _compute_unique_columns(
    qualified: PyDoughCollectionQDAG,
    output_columns: list[str],
) -> list[str | list[str]]:
    """
    Compute unique column combinations for a ViewGeneratedCollection by
    traversing the QDAG ancestor hierarchy.

    The algorithm:
    1. Build a rename mapping from the final Calculate's term values
       (e.g., if the user wrote `rkey=key`, maps 'key' -> 'rkey').
    2. Traverse ancestor_context upward, collecting unique_terms from each
       level, skipping levels where that level is singular w.r.t. the child
       (meaning the child's uniqueness already implies the parent's).
    3. Concatenate all collected unique_terms and map each term to its
       output column name via the rename mapping.
    4. Fall back to all output columns if any term cannot be mapped.

    Args:
        qualified: The qualified QDAG collection node.
        output_columns: The normalized output column names.

    Returns:
        A list where each element is a unique key combination (a list of
        column names).
    """
    # Step 1: Build rename mapping: original_name -> output_name
    # by walking the preceding_context chain to find the final Calculate.
    rename_map: dict[str, str] = {}
    node: PyDoughCollectionQDAG | None = qualified
    while node is not None:
        if isinstance(node, Calculate):
            for output_name, expr in node.calc_term_values.items():
                if isinstance(expr, Reference):
                    rename_map[expr.term_name] = output_name
            break
        node = getattr(node, "preceding_context", None)

    # Step 2: Traverse ancestor_context upward, collecting unique_terms
    # from non-skipped levels.
    output_set = set(output_columns)
    unique_term_groups: list[list[str]] = []
    child: PyDoughCollectionQDAG | None = None
    current: PyDoughCollectionQDAG | None = qualified

    while current is not None:
        terms = current.unique_terms
        # Skip this level if it is singular w.r.t. the child —
        # meaning each child record maps back to at most 1 parent record,
        # so the parent's uniqueness is already implied by the child's.
        should_skip = child is not None and current.is_singular(child)
        if not should_skip and terms:
            unique_term_groups.append(list(terms))
        child = current
        current = current.ancestor_context

    if not unique_term_groups:
        return [output_columns]

    # Step 3: Concatenate all collected unique_terms (dedup, preserve order).
    seen: set[str] = set()
    all_unique_terms: list[str] = []
    for group in unique_term_groups:
        for term in group:
            if term not in seen:
                seen.add(term)
                all_unique_terms.append(term)

    # Step 4: Map each unique term to an output column name.
    mapped_terms: list[str] = []
    for term in all_unique_terms:
        if term in output_set:
            mapped_terms.append(term)
        elif term in rename_map and rename_map[term] in output_set:
            mapped_terms.append(rename_map[term])
        else:
            # Cannot map this term — fall back to all output columns.
            return [output_columns]

    return [mapped_terms]


def to_table(
    node: UnqualifiedNode,
    name: str,
    as_view: bool = False,
    replace: bool = False,
    temp: bool = False,
    **kwargs,
) -> UnqualifiedGeneratedCollection:
    """
    Materialize the given PyDough query as a database temporary view/table,
    and return a collection reference that can be used
    in subsequent PyDough queries.

    Args:
        node: The PyDough query node to materialize.
        name: The name of the view/table to create. Can optionally include
            database and schema as 'db.schema.name'.
        as_view: If True, create a VIEW. If False, create a TABLE.
            Default is False.
        replace: If True, use CREATE OR REPLACE to allow replacing an
            existing view/table. Default is False.
        temp: If True, create a TEMPORARY view/table that will be deleted
            when the database session closes. Default is False.

    Returns:
        An UnqualifiedGeneratedCollection that can be used in subsequent
        PyDough queries (e.g., with .CALCULATE(), .WHERE()) to reference
        the created view/table.

    """
    is_valid_sql_name.verify(name, "table/view name")

    display_sql: bool = bool(kwargs.pop("display_sql", False))

    # Load session and convert to relational tree (same as to_sql)
    session: PyDoughSession = _load_session_info(**kwargs)
    if session.database is None:
        raise PyDoughSessionException(
            "Cannot create view/table without a database connection.\n"
            "Please configure a database connection in the session."
        )
    if session.database.dialect == DatabaseDialect.BODOSQL:
        raise PyDoughSessionException("to_table() is not yet implemented for BodoSQL.")
    qualified: PyDoughQDAG = qualify_node(node, session)
    if not isinstance(qualified, PyDoughCollectionQDAG):
        raise pydough.active_session.error_builder.expected_collection(qualified)
    relational: RelationalRoot = convert_ast_to_relational(qualified, None, session)

    # Step 1: Generate SQL for the query
    sql: str = convert_relation_to_sql(relational, session)

    # Step 2: Infer schema from relational tree and normalize column names.
    column_names, column_types = _infer_schema_from_relational(relational)
    column_names = [normalize_column_name(col)[1] for col in column_names]

    # Step 2b: Compute unique columns.
    unique_columns: list[str | list[str]] = _compute_unique_columns(
        qualified, column_names
    )

    # Step 3: Generate and execute DDL to create view/table.
    # Oracle private temporary tables require the ORA$PTT_ prefix on the table name.
    # Only the last segment of a dotted name (schema.table or db.schema.table) is prefixed.
    if temp and not as_view and session.database.dialect == DatabaseDialect.ORACLE:
        prefix, _, base = name.rpartition(".")
        table_name = f"{prefix}.ORA$PTT_{base}" if prefix else f"ORA$PTT_{base}"
    else:
        table_name = name
    ddl_statements, actual_temp = _generate_create_ddl(
        table_name, sql, as_view, replace, temp, session.database.dialect
    )
    pyd_logger = None
    if display_sql:
        pyd_logger = get_logger(__name__)

    # Execute the DDL statement(s) via the session's database connection
    # (may include DROP IF EXISTS before CREATE for some dialects)
    for ddl_stmt in ddl_statements:
        if pyd_logger is not None:
            pyd_logger.info(f"SQL query:\n {ddl_stmt}")
        session.database.connection.execute_ddl(ddl_stmt)

    # Step 4: Create ViewGeneratedCollection with the inferred schema
    # Use actual_temp which may differ from the input temp due to dialect limitations
    view_collection = ViewGeneratedCollection(
        name=table_name,
        columns=column_names,
        types=column_types,
        is_view=as_view,
        is_replace=replace,
        is_temp=actual_temp,
        unique_columns=unique_columns,
    )

    # Step 5: Wrap in UnqualifiedGeneratedCollection so it can be used in
    # PyDough queries (e.g., with .CALCULATE(), .WHERE())
    return UnqualifiedGeneratedCollection(view_collection)
