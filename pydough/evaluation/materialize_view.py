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
from pydough.user_collections.user_collections import PyDoughUserGeneratedCollection
from pydough.user_collections.view_collection import ViewGeneratedCollection

from .evaluate_unqualified import _load_session_info

all = ["to_table"]


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
) -> str:
    """
    Generate the CREATE DDL statement for a view or table.

    Args:
        name: The name of the view/table (can be 'db.schema.name')
        sql: The SQL query to use as the view/table definition
        as_view: True to create a VIEW, False to create a TABLE
        replace: True to use CREATE OR REPLACE
        temp: True to create a TEMPORARY view/table
        database_context: The database context to determine the database type for syntax differences.

    Returns:
        The DDL string to execute.
    """
    # Handle differences in CREATE syntax for different databases.
    # Make sure it works with all supported databases (MySQL, Postgres, Snowflake, SQLite).

    create_keyword = "CREATE"
    if replace:
        if db_dialect in (
            DatabaseDialect.ANSI,
            DatabaseDialect.SNOWFLAKE,
            DatabaseDialect.POSTGRES,
        ):
            create_keyword += " OR REPLACE"
        # MySQL does not support CREATE OR REPLACE for tables, only for views
        elif db_dialect == DatabaseDialect.MYSQL and as_view:
            create_keyword += " OR REPLACE"
        else:
            raise PyDoughSessionException(
                f"CREATE OR REPLACE is not supported for {db_dialect.name} when creating a table."
            )
    temp_keyword = "TEMPORARY " if temp else ""
    object_type = "VIEW" if as_view else "TABLE"

    # SQLite requires parentheses around the SELECT for views
    sql_text = ""
    if db_dialect == DatabaseDialect.SQLITE and as_view:
        sql_text = f"{create_keyword} {temp_keyword}{object_type} {name} AS ({sql})"
    else:
        sql_text = f"{create_keyword} {temp_keyword}{object_type} {name} AS {sql}"
    return sql_text


def to_table(
    node: UnqualifiedNode,
    name: str,
    as_view: bool = False,
    replace: bool = False,
    temp: bool = False,
    **kwargs,
) -> PyDoughUserGeneratedCollection:
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
        A PyDoughUserGeneratedCollection that can be used in subsequent
        PyDough queries to reference the created view/table.

    """

    # Load session and convert to relational tree (same as to_sql)
    session: PyDoughSession = _load_session_info(**kwargs)
    if session.database is None:
        raise PyDoughSessionException(
            "Cannot create view/table without a database connection."
            "Please configure a database connection in the session."
        )
    qualified: PyDoughQDAG = qualify_node(node, session)
    if not isinstance(qualified, PyDoughCollectionQDAG):
        raise pydough.active_session.error_builder.expected_collection(qualified)
    relational: RelationalRoot = convert_ast_to_relational(qualified, None, session)

    # Step 1: Generate SQL for the query
    sql: str = convert_relation_to_sql(relational, session)

    # Step 2: Infer schema from relational tree
    column_names, column_types = _infer_schema_from_relational(relational)

    # Step 3: Generate and execute DDL to create view/table
    ddl: str = _generate_create_ddl(
        name, sql, as_view, replace, temp, session.database.dialect
    )
    display_sql: bool = bool(kwargs.pop("display_sql", False))
    if display_sql:
        pyd_logger = get_logger(__name__)
        pyd_logger.info(f"SQL query:\n {sql}")

    # Execute the DDL via the session's database connection
    session.database.connection.execute_ddl(ddl)

    # Step 4: Create ViewGeneratedCollection with the inferred schema
    view_collection = ViewGeneratedCollection(
        name=name,
        columns=column_names,
        types=column_types,
        is_view=as_view,
        is_temp=temp,
    )

    # Step 5: return the collection reference
    return view_collection
