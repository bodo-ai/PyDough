"""
Definitions of various fixtures used in PyDough tests that are automatically
available.
"""

import os
import sqlite3
import subprocess
from collections.abc import Callable
from functools import cache

import pytest

import pydough
import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import (
    DatabaseConnection,
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
    load_database_context,
)
from pydough.metadata.graphs import GraphMetadata
from pydough.qdag import AstNodeBuilder
from tests.testing_utilities import graph_fetcher

from .gen_data.gen_technograph import gen_technograph_records


@pytest.fixture
def default_config() -> PyDoughConfigs:
    """
    The de-facto configuration of PyDoughConfigs used in testing. This is
    re-created with each request since a test function can mutate this.
    """
    config: PyDoughConfigs = PyDoughConfigs()
    # Set the defaults manually, in case they ever change.
    config.sum_default_zero = True
    config.avg_default_zero = False
    config.start_of_week = DayOfWeek.SUNDAY
    config.start_week_as_zero = True
    return config


@pytest.fixture
def defog_config() -> PyDoughConfigs:
    """
    The configuration of PyDoughConfigs used in testing defog.ai standard
    queries. This is re-created with each request since a test function can
    mutate this.
    """
    config: PyDoughConfigs = PyDoughConfigs()
    # Set the config values to match the defog.ai queries.
    config.sum_default_zero = True
    config.avg_default_zero = False
    config.start_of_week = DayOfWeek.MONDAY
    config.start_week_as_zero = True
    return config


@pytest.fixture(
    params=[
        pytest.param((sow, swaz), id=f"{sow.name.lower()}-{'zero' if swaz else 'one'}")
        for sow in list(DayOfWeek)
        for swaz in (True, False)
    ]
)
def week_handling_config(request):
    """
    Fixture which sets the start of week and start week as zero configuration.
    """
    config: PyDoughConfigs = PyDoughConfigs()
    start_of_week_config, start_week_as_zero_config = request.param
    config.start_of_week = start_of_week_config
    config.start_week_as_zero = start_week_as_zero_config
    return config


@pytest.fixture(scope="session")
def sample_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the sample graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/sample_graphs.json"


@pytest.fixture(scope="session")
def mysql_sample_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the MySQL sample graphs.
    It is the same as the sample graph path
    """
    return f"{os.path.dirname(__file__)}/test_metadata/sample_graphs.json"


@pytest.fixture(scope="session")
def udf_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the UDF graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/udf_sample_graphs.json"


@pytest.fixture(scope="session")
def invalid_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the invalid graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/invalid_graphs.json"


@pytest.fixture(scope="session")
def valid_sample_graph_names() -> set[str]:
    """
    Set of valid names to use to access a sample graph.
    """
    return {"TPCH", "Empty", "Epoch", "TechnoGraph"}


@pytest.fixture(scope="session")
def valid_udf_graph_names() -> set[str]:
    """
    Set of valid names to use to access a UDF graph.
    """
    return {"TPCH_SQLITE_UDFS"}


@pytest.fixture(params=["TPCH", "Empty", "Epoch"])
def sample_graph_names(request) -> str:
    """
    Fixture for the names that each of the sample graphs can be accessed.
    """
    return request.param


@pytest.fixture(scope="session")
def get_sample_graph(
    sample_graph_path: str,
    valid_sample_graph_names: set[str],
) -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported sample
    graph names and returns the metadata for that PyDough graph.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=sample_graph_path, graph_name=name
        )

    return impl


@pytest.fixture(scope="session")
def get_mysql_sample_graph(
    mysql_sample_graph_path: str,
    valid_sample_graph_names: set[str],
) -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported sample
    MySQL graph names and returns the metadata for that PyDough graph.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=mysql_sample_graph_path, graph_name=name
        )

    return impl


@pytest.fixture(scope="session")
def get_udf_graph(
    udf_graph_path: str, valid_udf_graph_names: set[str]
) -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported UDF
    graph names and returns the metadata for that PyDough graph.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        if name not in valid_udf_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=udf_graph_path, graph_name=name
        )

    return impl


@pytest.fixture
def sample_graphs(
    sample_graph_names: str, get_sample_graph: graph_fetcher
) -> GraphMetadata:
    """
    Retrieves the PyDough metadata for each graph in the `sample_graphs` JSON
    file.
    """
    return get_sample_graph(sample_graph_names)


@pytest.fixture(scope="session")
def tpch_node_builder(get_sample_graph) -> AstNodeBuilder:
    """
    Builds a QDAG node builder using the TPCH graph.
    """
    return AstNodeBuilder(get_sample_graph("TPCH"))


@pytest.fixture(scope="session")
def get_plan_test_filename() -> Callable[[str], str]:
    """
    A function that takes in a file name and returns the path to that file
    from within the directory of plan testing refsol files.
    """

    def impl(file_name: str) -> str:
        return f"{os.path.dirname(__file__)}/test_plan_refsols/{file_name}.txt"

    return impl


@pytest.fixture(scope="session")
def get_sql_test_filename() -> Callable[[str, DatabaseDialect], str]:
    """
    A function that takes in a file name and returns the path to that file
    from within the directory of SQL text testing refsol files.
    """

    def impl(file_name: str, dialect: DatabaseDialect) -> str:
        return f"{os.path.dirname(__file__)}/test_sql_refsols/{file_name}_{dialect.value.lower()}.sql"

    return impl


@pytest.fixture
def update_tests() -> bool:
    """
    If True, planner/sql tests should update the refsol file instead of
    verifying that the test matches the file. If False, the refsol file is used
    to check the answer.

    This is controlled by an environment variable `PYDOUGH_UPDATE_TESTS`.
    """
    return os.getenv("PYDOUGH_UPDATE_TESTS", "0") == "1"


@pytest.fixture(
    params=[
        pytest.param(operator, id=operator.binop.name)
        for operator in pydop.builtin_registered_operators().values()
        if isinstance(operator, pydop.BinaryOperator)
    ]
)
def binary_operators(request) -> pydop.BinaryOperator:
    """
    Returns every PyDough expression operator for a BinOp.
    """
    return request.param


@pytest.fixture(
    params=[
        pytest.param(DatabaseDialect.ANSI, id="ansi"),
        pytest.param(DatabaseDialect.SQLITE, id="sqlite"),
        pytest.param(DatabaseDialect.MYSQL, id="mysql"),
    ]
)
def sqlite_dialects(request) -> DatabaseDialect:
    """
    Returns the SQLite dialect.
    """
    return request.param


@pytest.fixture(
    params=[
        pytest.param(DatabaseDialect.ANSI, id="ansi"),
        pytest.param(DatabaseDialect.SQLITE, id="sqlite"),
        pytest.param(DatabaseDialect.MYSQL, id="mysql"),
    ]
)
def empty_context_database(request) -> DatabaseContext:
    """
    Returns a database context with an empty connection for each supported
    PyDough SQL dialect.
    """
    return DatabaseContext(empty_connection, request.param)


@pytest.fixture(scope="session")
def sqlite_people_jobs() -> DatabaseConnection:
    """
    Return a SQLite database connection a new in memory database that
    is pre-loaded with the PEOPLE and JOBS tables with the following properties:
     - People:
        - person_id: BIGINT PRIMARY KEY
        - name: TEXT
    - Jobs:
        - job_id: BIGINT PRIMARY KEY
        - person_id: BIGINT (Foreign key to PEOPLE.person_id)
        - title: TEXT
        - salary: FLOAT

    Returns:
        sqlite3.Connection: A connection to an in-memory SQLite database.
    """
    create_table_1: str = """
        CREATE TABLE PEOPLE (
            person_id BIGINT PRIMARY KEY,
            name TEXT
        )
    """
    create_table_2: str = """
        CREATE TABLE JOBS (
            job_id BIGINT PRIMARY KEY,
            person_id BIGINT,
            title TEXT,
            salary FLOAT
        )
    """
    sqlite3_empty_connection: DatabaseConnection = DatabaseConnection(
        sqlite3.connect(":memory:")
    )
    cursor: sqlite3.Cursor = sqlite3_empty_connection.connection.cursor()
    cursor.execute(create_table_1)
    cursor.execute(create_table_2)
    for i in range(10):
        cursor.execute(f"""
            INSERT INTO PEOPLE (person_id, name)
            VALUES ({i}, 'Person {i}')
        """)
        for j in range(2):
            cursor.execute(f"""
                INSERT INTO JOBS (job_id, person_id, title, salary)
                VALUES ({(2 * i) + j}, {i}, 'Job {i}', {(i + j + 5.7) * 1000})
            """)
    sqlite3_empty_connection.connection.commit()
    cursor.close()
    return sqlite3_empty_connection


@pytest.fixture
def sqlite_people_jobs_context(
    sqlite_people_jobs: DatabaseConnection, sqlite_dialects: DatabaseDialect
) -> DatabaseContext:
    """
    Returns a DatabaseContext for the SQLite PEOPLE and JOBS tables
    with the given dialect.
    """
    return DatabaseContext(sqlite_people_jobs, sqlite_dialects)


@pytest.fixture(scope="module")
def sqlite_tpch_db_path() -> str:
    """
    Return the path to the TPCH database. We setup testing
    to always be in the base module at the same location with
    the name tpch.db.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    return os.path.join(base_dir, "tpch.db")


@pytest.fixture(scope="module")
def sqlite_tpch_db(sqlite_tpch_db_path: str) -> sqlite3.Connection:
    """
    Download the TPCH data and return a connection to the SQLite database.
    """
    # Ensure that the database is attached as 'tpch` instead of `main`
    connection: sqlite3.Connection = sqlite3.connect(":memory:")
    connection.execute(f"attach database '{sqlite_tpch_db_path}' as tpch")
    return connection


@pytest.fixture
def sqlite_tpch_db_context(sqlite_tpch_db_path: str, sqlite_tpch_db) -> DatabaseContext:
    """
    Return a DatabaseContext for the SQLite TPCH database.
    """
    return DatabaseContext(DatabaseConnection(sqlite_tpch_db), DatabaseDialect.SQLITE)


@pytest.fixture(scope="session")
def defog_graphs() -> graph_fetcher:
    """
    Returns the graphs for the defog database.
    """
    # Setup the directory to be the main PyDough directory.

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = f"{os.path.dirname(__file__)}/test_metadata/defog_graphs.json"
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

    return impl


@pytest.fixture(scope="session")
def sqlite_defog_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection for the defog database.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    # Setup the defog database.
    subprocess.run("cd tests/gen_data; bash setup_defog.sh", shell=True)
    path: str = os.path.join(base_dir, "tests/gen_data/defog.db")
    connection: sqlite3.Connection = sqlite3.connect(path)
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


@pytest.fixture(scope="session")
def sqlite_epoch_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection for the epoch database.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    # Setup the epoch database.
    subprocess.run(
        "cd tests; rm -fv gen_data/epoch.db; sqlite3 gen_data/epoch.db < gen_data/init_epoch.sql",
        shell=True,
    )
    path: str = os.path.join(base_dir, "tests/gen_data/epoch.db")
    connection: sqlite3.Connection = sqlite3.connect(path)
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


@pytest.fixture(scope="session")
def sqlite_technograph_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection for the technograph database.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    path: str = os.path.join(base_dir, "tests/gen_data/technograph.db")

    # Delete the existing database.
    subprocess.run(
        "cd tests; rm -fv gen_data/technograph.db; sqlite3 gen_data/technograph.db < gen_data/init_technograph.sql",
        shell=True,
    )

    # Setup the database
    connection: sqlite3.Connection = sqlite3.connect(path)
    cursor: sqlite3.Cursor = connection.cursor()
    gen_technograph_records(cursor)

    # Return the database context.
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


MYSQL_ENVS = ["MYSQL_USERNAME", "MYSQL_PASSWORD", "MYSQL_DB", "MYSQL_HOST"]

"""
    MySQL environment variables required for connection.
    MYSQL_USERNAME: The username for MySQL.
    MYSQL_PASSWORD: The password for MySQL.
    MYSQL_DB: The database name for MySQL.
    MYSQL_HOST: The host ip for MySQL.
"""


def is_mysql_env_set() -> bool:
    """
    Check if the MySQL environment variables are set. Allowing empty strings.
    Returns:
        bool: True if all required MySQL environment variables are set, False otherwise.
    """
    return all(os.getenv(env) is not None for env in MYSQL_ENVS)


@pytest.fixture
def mysql_conn_tpch_db_context() -> DatabaseContext:
    """
    This fixture is used to connect to the MySQL TPCH database using
    a connection object.
    Returns a DatabaseContext for the MySQL TPCH database.
    """
    if not is_mysql_env_set():
        pytest.skip("Skipping MySQL tests: environment variables not set.")
    import mysql.connector as mysql_connector

    mysql_username = os.getenv("MYSQL_USERNAME")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_tpch_db = os.getenv("MYSQL_DB")
    mysql_host = os.getenv("MYSQL_HOST")

    connection: mysql_connector.connection.MySQLConnection = mysql_connector.connect(
        user=mysql_username,
        password=mysql_password,
        host=mysql_host,
        database=mysql_tpch_db,
    )
    return load_database_context(
        "mysql",
        connection=connection,
    )


@pytest.fixture
def mysql_params_tpch_db_context() -> DatabaseContext:
    """
    This fixture is used to connect to the MySQL TPCH database using
    parameters instead of a connection object.
    Returns a DatabaseContext for the MySQL TPCH database.
    """
    if not is_mysql_env_set():
        pytest.skip("Skipping MySQL tests: environment variables not set.")

    mysql_tpch_db = os.getenv("MYSQL_DB")
    mysql_host = os.getenv("MYSQL_HOST")
    mysql_username = os.getenv("MYSQL_USERNAME")
    mysql_password = os.getenv("MYSQL_PASSWORD")

    return load_database_context(
        "mysql",
        user=mysql_username,
        password=mysql_password,
        host=mysql_host,
        database=mysql_tpch_db,
    )
