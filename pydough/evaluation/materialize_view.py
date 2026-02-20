import warnings
from dataclasses import dataclass

import pydough
from pydough.configs import PyDoughSession
from pydough.conversion import convert_ast_to_relational
from pydough.database_connectors import DatabaseDialect
from pydough.errors import PyDoughSessionException
from pydough.logger import get_logger
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.sqlglot import convert_relation_to_sql
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


@dataclass(frozen=True)
class CreateCapabilities:
    """
    Class to define the capabilities of the different database dialects
    for CREATE statements.
    This is used to determine which syntax options are available
    for creating views/tables in different databases.
    """

    replace_table: bool = True
    temp_table: bool = True
    replace_view: bool = True
    temp_view: bool = True


CREATE_CAPABILITIES: dict[DatabaseDialect, CreateCapabilities] = {
    DatabaseDialect.SNOWFLAKE: CreateCapabilities(
        temp_view=False,
    ),
    DatabaseDialect.POSTGRES: CreateCapabilities(
        replace_table=False,
        temp_view=False,
    ),
    DatabaseDialect.MYSQL: CreateCapabilities(
        replace_table=False,
        temp_view=False,
    ),
    DatabaseDialect.SQLITE: CreateCapabilities(
        replace_table=False,
        replace_view=False,
    ),
    DatabaseDialect.ANSI: CreateCapabilities(),
}
"""
Mapping of database dialects to their CREATE statement capabilities.
This is used to determine which options are available when generating 
the DDL for creating views/tables in different databases.
"""


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
    create_caps = CREATE_CAPABILITIES[db_dialect]
    object_type = "VIEW" if as_view else "TABLE"
    ddl_statements: list[str] = []

    if temp:
        allowed = create_caps.temp_view if as_view else create_caps.temp_table
        if not allowed:
            raise PyDoughSessionException(
                f"TEMP {object_type} not supported for {db_dialect.name}"
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
    can_replace = create_caps.replace_view if as_view else create_caps.replace_table

    # For databases that don't support CREATE OR REPLACE TABLE,
    # use DROP TABLE IF EXISTS + CREATE TABLE pattern
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

    display_sql: bool = bool(kwargs.pop("display_sql", False))

    # Load session and convert to relational tree (same as to_sql)
    session: PyDoughSession = _load_session_info(**kwargs)
    if session.database is None:
        raise PyDoughSessionException(
            "Cannot create view/table without a database connection.\n"
            "Please configure a database connection in the session."
        )
    # session.metadata = graph
    qualified: PyDoughQDAG = qualify_node(node, session)
    if not isinstance(qualified, PyDoughCollectionQDAG):
        raise pydough.active_session.error_builder.expected_collection(qualified)
    relational: RelationalRoot = convert_ast_to_relational(qualified, None, session)

    # Step 1: Generate SQL for the query
    sql: str = convert_relation_to_sql(relational, session)

    # Step 2: Infer schema from relational tree
    column_names, column_types = _infer_schema_from_relational(relational)

    # Step 3: Generate and execute DDL to create view/table
    ddl_statements, actual_temp = _generate_create_ddl(
        name, sql, as_view, replace, temp, session.database.dialect
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
        name=name,
        columns=column_names,
        types=column_types,
        is_view=as_view,
        is_temp=actual_temp,
    )

    # Step 5: Wrap in UnqualifiedGeneratedCollection so it can be used in
    # PyDough queries (e.g., with .CALCULATE(), .WHERE())
    return UnqualifiedGeneratedCollection(view_collection)
