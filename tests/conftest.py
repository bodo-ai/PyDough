"""
Definitions of various fixtures used in PyDough tests that are automatically
available.
"""

import os
import sqlite3
import subprocess
import time
from collections.abc import Callable
from functools import cache

import pandas as pd
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
from pydough.errors import PyDoughTestingException
from pydough.metadata.graphs import GraphMetadata
from pydough.qdag import AstNodeBuilder
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q1_output,
    tpch_q2_output,
    tpch_q3_output,
    tpch_q4_output,
    tpch_q5_output,
    tpch_q6_output,
    tpch_q7_output,
    tpch_q8_output,
    tpch_q9_output,
    tpch_q10_output,
    tpch_q11_output,
    tpch_q12_output,
    tpch_q13_output,
    tpch_q14_output,
    tpch_q15_output,
    tpch_q16_output,
    tpch_q17_output,
    tpch_q18_output,
    tpch_q19_output,
    tpch_q20_output,
    tpch_q21_output,
    tpch_q22_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q1,
    impl_tpch_q2,
    impl_tpch_q3,
    impl_tpch_q4,
    impl_tpch_q5,
    impl_tpch_q6,
    impl_tpch_q7,
    impl_tpch_q8,
    impl_tpch_q9,
    impl_tpch_q10,
    impl_tpch_q11,
    impl_tpch_q12,
    impl_tpch_q13,
    impl_tpch_q14,
    impl_tpch_q15,
    impl_tpch_q16,
    impl_tpch_q17,
    impl_tpch_q18,
    impl_tpch_q19,
    impl_tpch_q20,
    impl_tpch_q21,
    impl_tpch_q22,
)
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher

from .gen_data.gen_pagerank import gen_pagerank_records, pagerank_configs
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
def sf_sample_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the Snowflake sample graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/snowflake_sample_graphs.json"


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
def get_test_graph_by_name() -> graph_fetcher:
    """
    Returns a known test graph requested if the graph location was included in test_graph_location.
    """
    test_graph_location: dict[str, str] = {
        "synthea": "synthea_graph.json",
        "world_development_indicators": "world_development_indicators_graph.json",
        "keywords": "reserved_words_graph.json",
    }

    @cache
    def impl(name: str) -> GraphMetadata:
        file_name: str = test_graph_location[name]
        path: str = f"{os.path.dirname(__file__)}/test_metadata/{file_name}"
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

    return impl


@pytest.fixture(scope="session")
def get_mysql_defog_graphs() -> graph_fetcher:
    """
    Returns the graphs for the defog database in MySQL.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = f"{os.path.dirname(__file__)}/test_metadata/mysql_defog_graphs.json"
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

    return impl


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
            raise PyDoughTestingException(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=sample_graph_path, graph_name=name
        )

    return impl


@pytest.fixture(scope="session")
def get_sf_sample_graph(
    sf_sample_graph_path: str,
    valid_sample_graph_names: set[str],
) -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported sample
    Snowflake graph names and returns the metadata for that PyDough graph.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=sf_sample_graph_path, graph_name=name
        )

    return impl


@pytest.fixture(scope="session")
def get_sf_defog_graphs() -> graph_fetcher:
    """
    Returns the graphs for the defog database in Snowflake.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = (
            f"{os.path.dirname(__file__)}/test_metadata/snowflake_defog_graphs.json"
        )
        return pydough.parse_json_metadata_from_file(file_path=path, graph_name=name)

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
        pytest.param(DatabaseDialect.SNOWFLAKE, id="snowflake"),
        pytest.param(DatabaseDialect.MYSQL, id="mysql"),
        pytest.param(DatabaseDialect.POSTGRES, id="postgres"),
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
def masked_graphs() -> graph_fetcher:
    """
    Returns the graphs for the masked databases.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        path: str = f"{os.path.dirname(__file__)}/test_metadata/masked_graphs.json"
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


@pytest.fixture(
    params=[
        pytest.param(("0", "raw"), id="raw"),
        pytest.param(("1", "rewrite"), id="rewrite"),
    ]
)
def enable_mask_rewrites(request):
    """
    Temporarily enable the mask rewrites by setting the environment variable to
    the specified value, returning the name that should be used to identify
    the situation ("raw" for disabled, "rewrite" for enabled).
    """
    old_value: str = os.environ.get("PYDOUGH_ENABLE_MASK_REWRITES", "0")
    os.environ["PYDOUGH_ENABLE_MASK_REWRITES"] = request.param[0]
    yield request.param[1]
    os.environ["PYDOUGH_ENABLE_MASK_REWRITES"] = old_value


@pytest.fixture(scope="session")
def sqlite_cryptbank_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection for the CRYPTBANK database.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    # Setup the cryptbank database.
    subprocess.run(
        "cd tests; rm -fv gen_data/cryptbank.db; sqlite3 gen_data/cryptbank.db < gen_data/init_cryptbank.sql",
        shell=True,
    )
    path: str = os.path.join(base_dir, "tests/gen_data/cryptbank.db")
    connection: sqlite3.Connection = sqlite3.connect(":memory:")
    connection.execute(f"attach database '{path}' as CRBNK")
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


@pytest.fixture(scope="session")
def sqlite_custom_datasets_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection with all the custom datasets attached.
    """
    gen_data_path: str = "tests/gen_data"
    # Dataset tuple format: (schema_name, db_file_name, init_sql_file_name)
    SQLite_datasets: list[tuple[str, str, str]] = [
        ("synthea", "synthea.db", "init_synthea_sqlite.sql"),
        ("wdi", "world_development_indicators.db", "init_world_indicators_sqlite.sql"),
        ("keywords", "reserved_words.db", "init_reserved_words_sqlite.sql"),
    ]

    # List of shell commands required to re-create all the db files
    commands: list[str] = [f"cd {gen_data_path}"]
    # Collect all db_file_names into the rm command
    rm_command: str = "rm -fv " + " ".join(
        db_file for (_, db_file, _) in SQLite_datasets
    )
    commands.append(rm_command)
    # Add one sqlite3 command per dataset
    for _, db_file, init_sql in SQLite_datasets:
        commands.append(f"sqlite3 {db_file} < {init_sql}")
    # Get the shell commands required to re-create all the db files
    shell_cmd: str = "; ".join(commands)

    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    # Setup the world development indicators database.
    subprocess.run(shell_cmd, shell=True, check=True)
    # Central in-memory connection
    connection: sqlite3.Connection = sqlite3.connect(":memory:")

    # Use (schema_name, db_file_name info) on SQLite_datasets to ATTACH DBs
    for schema, db_file, _ in SQLite_datasets:
        path: str = os.path.join(base_dir, gen_data_path, db_file)
        connection.execute(f"ATTACH DATABASE '{path}' AS {schema}")

    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


SF_ENVS = ["SF_USERNAME", "SF_PASSWORD", "SF_ACCOUNT"]
"""
    Snowflake environment variables required for connection.
    SF_USERNAME: The username for the Snowflake account.
    SF_PASSWORD: The password for the Snowflake account.
    SF_ACCOUNT: The account identifier for the Snowflake account.
"""


def is_snowflake_env_set() -> bool:
    """
    Check if the Snowflake environment variables are set.

    Returns:
        bool: True if all required Snowflake environment variables are set, False otherwise.
    """
    return all(os.getenv(env) for env in SF_ENVS)


@pytest.fixture
def sf_conn_db_context() -> Callable[[str, str], DatabaseContext]:
    """
    This fixture is used to connect to the Snowflake TPCH database using
    a connection object.
    Return a DatabaseContext for the Snowflake TPCH database.
    """

    def _impl(database_name: str, schema_name: str) -> DatabaseContext:
        if not is_snowflake_env_set():
            pytest.skip("Skipping Snowflake tests: environment variables not set.")
        import snowflake.connector as sf_connector

        warehouse = "DEMO_WH"
        password = os.getenv("SF_PASSWORD")
        username = os.getenv("SF_USERNAME")
        account = os.getenv("SF_ACCOUNT")
        connection: sf_connector.connection.SnowflakeConnection = sf_connector.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database=database_name,
            schema=schema_name,
        )
        # Run DEFOG_DAILY_UPDATE() only if data is older than 1 day
        with connection.cursor() as cur:
            cur.execute("""
                DECLARE last_mod DATE;

            BEGIN
                -- Get table last modified date
                SELECT DATE(LAST_ALTERED) INTO last_mod
                FROM INFORMATION_SCHEMA.TABLES
                WHERE table_catalog='DEFOG' 
                    AND table_schema = 'BROKER'
                    AND table_name = 'SBDAILYPRICE';

                -- If last modified is before today, call the procedure
                IF (last_mod < CURRENT_DATE()) THEN
                    CALL DEFOG.BROKER.DEFOG_DAILY_UPDATE();
                END IF;
            END;
            """)

        return load_database_context("snowflake", connection=connection)

    return _impl


@pytest.fixture
def sf_params_tpch_db_context() -> DatabaseContext:
    """
    This fixture is used to connect to the Snowflake TPCH database using
    parameters instead of a connection object.
    Return a DatabaseContext for the Snowflake TPCH database.
    """
    if not is_snowflake_env_set():
        pytest.skip("Skipping Snowflake tests: environment variables not set.")
    sf_tpch_db = "SNOWFLAKE_SAMPLE_DATA"
    sf_tpch_schema = "TPCH_SF1"
    warehouse = "DEMO_WH"
    password = os.getenv("SF_PASSWORD")
    username = os.getenv("SF_USERNAME")
    account = os.getenv("SF_ACCOUNT")
    return load_database_context(
        "snowflake",
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=sf_tpch_db,
        schema=sf_tpch_schema,
    )


def is_ci():
    """
    Detect if running inside CI (GitHub Actions sets this env var).
    """
    return os.getenv("GITHUB_ACTIONS", "false").lower() == "true"


def container_exists(name: str) -> bool:
    """
    Check if a Docker container with the given name exists.
    """
    result = subprocess.run(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        stdout=subprocess.PIPE,
        text=True,
    )
    return name in result.stdout.splitlines()


def container_is_running(name: str) -> bool:
    """
    Check if a Docker container with the given name is currently running.
    """
    result = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], stdout=subprocess.PIPE, text=True
    )
    return name in result.stdout.splitlines()


MYSQL_ENVS = ["MYSQL_USERNAME", "MYSQL_PASSWORD"]
"""
The MySQL environment variables required for connection.
- `MYSQL_USERNAME`: The username for MySQL.
- `MYSQL_PASSWORD`: The password for MySQL.
"""


@pytest.fixture(scope="session")
def require_mysql_env() -> None:
    """
    Checks whether all required MySQL environment variables are set.
    """
    if not all(os.getenv(var) is not None for var in MYSQL_ENVS):
        pytest.skip("Skipping MySQL tests: environment variables not set.")


MYSQL_DOCKER_CONTAINER = "mysql_tpch_test"
MYSQL_DOCKER_IMAGE = "bodoai1/pydough-mysql-tpch:latest"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = "3306"
MYSQL_DB = "tpch"
"""
    CONSTANTS for the MySQL Docker container setup.
    - DOCKER_CONTAINER: The name of the Docker container.
    - DOCKER_IMAGE: The Docker image to use for the MySQL container.
    - MYSQL_HOST: The host address for MySQL.
    - MYSQL_PORT: The port on which MySQL is exposed.
    - MYSQL_DB: The name of the TPCH database in MySQL.
"""


@pytest.fixture(scope="session")
def mysql_docker_setup() -> None:
    """Set up the MySQL Docker container for testing."""
    try:
        if not is_ci():
            if container_exists(MYSQL_DOCKER_CONTAINER):
                if not container_is_running(MYSQL_DOCKER_CONTAINER):
                    subprocess.run(
                        ["docker", "start", MYSQL_DOCKER_CONTAINER], check=True
                    )
            else:
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        MYSQL_DOCKER_CONTAINER,
                        "-e",
                        f"MYSQL_ROOT_PASSWORD={os.getenv('MYSQL_PASSWORD')}",
                        "-p",
                        f"{MYSQL_PORT}:3306",
                        MYSQL_DOCKER_IMAGE,
                    ],
                    check=True,
                )
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to set up MySQL Docker container: {e}")

    # Check import is successful
    try:
        import mysql.connector as mysql_connector
    except ImportError as e:
        raise RuntimeError("mysql-connector-python is not installed") from e

    # Wait for MySQL to be ready
    for _ in range(30):
        try:
            conn = mysql_connector.connect(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=os.getenv("MYSQL_USERNAME"),
                password=os.getenv("MYSQL_PASSWORD"),
                database=MYSQL_DB,
            )
            conn.close()
            break
        except mysql_connector.Error as e:
            print("Error occurred while connecting to MySQL:", e)
            time.sleep(1)
    else:
        subprocess.run(["docker", "rm", "-f", MYSQL_DOCKER_CONTAINER])
        pytest.fail("MySQL container did not become ready in time.")


@pytest.fixture(scope="session")
def mysql_conn_db_context(
    require_mysql_env, mysql_docker_setup
) -> Callable[[str], DatabaseContext]:
    """
    This fixture is used to connect to the MySQL TPCH database using
    a connection object.
    Returns a DatabaseContext for the MySQL TPCH database.
    """
    # The first time, set up the defog data
    import mysql.connector as mysql_connector

    mysql_username = os.getenv("MYSQL_USERNAME")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_host = MYSQL_HOST

    connection: mysql_connector.connection.MySQLConnection = mysql_connector.connect(
        user=mysql_username,
        password=mysql_password,
        host=mysql_host,
        use_pure=True,
    )

    # Loads the defog data into the MySQL engine.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    path: str = os.path.join(base_dir, "tests/gen_data/init_defog_mysql.sql")
    with open(path) as f:
        init_defog_script: str = f.read()
    cursor: mysql_connector.connection.MySQLCursor = connection.cursor()
    for statement in init_defog_script.split(";\n"):
        if statement.strip():
            cursor.execute(statement.strip())
    connection.commit()
    cursor.close()

    @cache
    def _impl(database_name: str) -> DatabaseContext:
        mysql_username = os.getenv("MYSQL_USERNAME")
        mysql_password = os.getenv("MYSQL_PASSWORD")
        mysql_db = database_name
        mysql_host = MYSQL_HOST

        connection: mysql_connector.connection.MySQLConnection = (
            mysql_connector.connect(
                user=mysql_username,
                password=mysql_password,
                host=mysql_host,
                database=mysql_db,
            )
        )
        return load_database_context(
            "mysql",
            connection=connection,
        )

    return _impl


@pytest.fixture(scope="session")
def mysql_params_tpch_db_context(
    require_mysql_env, mysql_docker_setup
) -> DatabaseContext:
    """
    This fixture is used to connect to the MySQL TPCH database using
    parameters instead of a connection object.
    Returns a DatabaseContext for the MySQL TPCH database.
    """

    mysql_username = os.getenv("MYSQL_USERNAME")
    mysql_password = os.getenv("MYSQL_PASSWORD")
    mysql_db = MYSQL_DB
    mysql_host = MYSQL_HOST

    return load_database_context(
        "mysql",
        user=mysql_username,
        password=mysql_password,
        host=mysql_host,
        database=mysql_db,
    )


POSTGRES_ENVS = ["POSTGRES_USER", "POSTGRES_PASSWORD"]
"""
    Postgres environment variables required for connection.
    `POSTGRES_USER`: The username for Postgres.
    `POSTGRES_PASSWORD`: The password for Postgres.
"""


@pytest.fixture(scope="session")
def require_postgres_env() -> None:
    """
    Check if the Postgres environment variables are set. Allowing empty strings.
    Returns:
        bool: True if all required Postgres environment variables are set, False otherwise.
    """
    if not all(os.getenv(var) is not None for var in POSTGRES_ENVS):
        pytest.skip("Skipping Postgres tests: environment variables not set.")


POSTGRES_DOCKER_CONTAINER = "postgres_tpch_test"
POSTGRES_DOCKER_IMAGE = "bodoai1/pydough-postgres-tpch:latest"
POSTGRES_HOST = "127.0.0.1"
POSTGRES_PORT = 5432
POSTGRES_DB = "pydough_test"
"""
    CONSTANTS for the Postgres Docker container setup.
    - DOCKER_CONTAINER: The name of the Docker container.
    - DOCKER_IMAGE: The Docker image to use for the Postgres container.
    - POSTGRES_HOST: The host address for Postgres.
    - POSTGRES_PORT: The port on which Postgres is exposed.
    - POSTGRES_DB: The name of the TPCH database in Postgres.
"""


@pytest.fixture(scope="session")
def postgres_docker_setup() -> None:
    """Set up the Postgres Docker container for testing."""
    try:
        if not is_ci():
            if container_exists(POSTGRES_DOCKER_CONTAINER):
                if not container_is_running(POSTGRES_DOCKER_CONTAINER):
                    subprocess.run(
                        ["docker", "start", POSTGRES_DOCKER_CONTAINER], check=True
                    )
            else:
                subprocess.run(
                    [
                        "docker",
                        "run",
                        "-d",
                        "--name",
                        POSTGRES_DOCKER_CONTAINER,
                        "-e",
                        f"POSTGRES_PASSWORD={os.getenv('POSTGRES_PASSWORD')}",
                        "-p",
                        f"{POSTGRES_PORT}:5432",
                        POSTGRES_DOCKER_IMAGE,
                    ],
                    check=True,
                )
    except subprocess.CalledProcessError as e:
        pytest.fail(f"Failed to set up Postgres Docker container: {e}")

    # Check import is successful
    try:
        import psycopg2
    except ImportError as e:
        raise RuntimeError("psycopg2 is not installed") from e

    # Wait for Postgres to be ready
    for _ in range(30):
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                database=POSTGRES_DB,
            )
            conn.close()
            break
        except psycopg2.Error as e:
            print("Error occurred while connecting to Postgres:", e)
            time.sleep(1)
    else:
        subprocess.run(["docker", "rm", "-f", POSTGRES_DOCKER_CONTAINER])
        pytest.fail("Postgres container did not become ready in time.")


@pytest.fixture
def postgres_conn_db_context(
    require_postgres_env,
    postgres_docker_setup,
) -> DatabaseContext:
    """
    This fixture is used to connect to the Postgres TPCH database using
    a connection object.
    Returns a DatabaseContext for the Postgres TPCH database.
    """
    import psycopg2

    postgres_user: str | None = os.getenv("POSTGRES_USER")
    postgres_password: str | None = os.getenv("POSTGRES_PASSWORD")
    postgres_db: str = POSTGRES_DB
    postgres_host: str = POSTGRES_HOST
    postgres_port: int = POSTGRES_PORT
    connection: psycopg2.extensions.connection = psycopg2.connect(
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port,
    )
    connection.autocommit = True  # It avoids getting stuck when DROP/CREATE
    # Loads the defog data into the Postgres engine.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    path: str = os.path.join(base_dir, "tests/gen_data/init_defog_postgres.sql")
    with open(path) as f:
        init_defog_script: str = f.read()
    cursor: psycopg2.connection.PostgresCursor = connection.cursor()
    for statement in init_defog_script.split(";\n"):
        if statement.strip():
            cursor.execute(statement.strip())

    connection.commit()
    cursor.close()

    return load_database_context(
        "postgres",
        connection=connection,
    )


@pytest.fixture
def postgres_params_tpch_db_context(
    require_postgres_env, postgres_docker_setup
) -> DatabaseContext:
    """
    This fixture is used to connect to the Postgres TPCH database using
    parameters instead of a connection object.
    Returns a DatabaseContext for the Postgres TPCH database.
    """

    postgres_user: str | None = os.getenv("POSTGRES_USER")
    postgres_password: str | None = os.getenv("POSTGRES_PASSWORD")
    postgres_tpch_db: str = POSTGRES_DB
    postgres_host: str = POSTGRES_HOST
    postgres_port: int = POSTGRES_PORT

    return load_database_context(
        "postgres",
        dbname=postgres_tpch_db,
        user=postgres_user,
        password=postgres_password,
        host=postgres_host,
        port=postgres_port,
    )


@pytest.fixture(scope="session")
def get_pagerank_graph() -> graph_fetcher:
    """
    A function that returns the graph used for PageRank calculations. The same
    graph is used for all PageRank tests, but different databases are used that
    adhere to the same table schema setup that the graph invokes.
    """

    @cache
    def impl(name: str) -> GraphMetadata:
        return pydough.parse_json_metadata_from_file(
            file_path=f"{os.path.dirname(__file__)}/test_metadata/pagerank_graphs.json",
            graph_name="PAGERANK",
        )

    return impl


@pytest.fixture(scope="session")
def sqlite_pagerank_db_contexts() -> dict[str, DatabaseContext]:
    """
    Returns the SQLITE database contexts for the various pagerank database.
    This is returned as a dictionary mapping the name of the database to the
    DatabaseContext for that database, all of which adhere to the same
    schema structure assumed by the PAGERANK graph.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))

    # Setup each of the the pagerank databases using the configurations.
    result: dict[str, DatabaseContext] = {}
    for name, nodes, edges in pagerank_configs():
        # Create the database and ensure it is empty.
        subprocess.run(
            f"cd tests; rm -fv gen_data/{name.lower()}.db; sqlite3 gen_data/{name.lower()}.db < gen_data/init_pagerank.sql",
            shell=True,
        )
        path: str = os.path.join(base_dir, f"tests/gen_data/{name.lower()}.db")
        connection: sqlite3.Connection = sqlite3.connect(path)

        # Fill the tables of the database using the nodes/edges, then store the
        # database context in the result.
        gen_pagerank_records(connection, nodes, edges)
        result[name] = DatabaseContext(
            DatabaseConnection(connection), DatabaseDialect.SQLITE
        )
    return result


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q1,
                "TPCH",
                tpch_q1_output,
                "tpch_q1",
            ),
            id="tpch_q1",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q2,
                "TPCH",
                tpch_q2_output,
                "tpch_q2",
            ),
            id="tpch_q2",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q3,
                "TPCH",
                tpch_q3_output,
                "tpch_q3",
            ),
            id="tpch_q3",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q4,
                "TPCH",
                tpch_q4_output,
                "tpch_q4",
            ),
            id="tpch_q4",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q5,
                "TPCH",
                tpch_q5_output,
                "tpch_q5",
            ),
            id="tpch_q5",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q6,
                "TPCH",
                tpch_q6_output,
                "tpch_q6",
            ),
            id="tpch_q6",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q7,
                "TPCH",
                tpch_q7_output,
                "tpch_q7",
            ),
            id="tpch_q7",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q8,
                "TPCH",
                tpch_q8_output,
                "tpch_q8",
            ),
            id="tpch_q8",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q9,
                "TPCH",
                tpch_q9_output,
                "tpch_q9",
            ),
            id="tpch_q9",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q10,
                "TPCH",
                tpch_q10_output,
                "tpch_q10",
            ),
            id="tpch_q10",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q11,
                "TPCH",
                tpch_q11_output,
                "tpch_q11",
            ),
            id="tpch_q11",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q12,
                "TPCH",
                tpch_q12_output,
                "tpch_q12",
            ),
            id="tpch_q12",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q13,
                "TPCH",
                tpch_q13_output,
                "tpch_q13",
            ),
            id="tpch_q13",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q14,
                "TPCH",
                tpch_q14_output,
                "tpch_q14",
            ),
            id="tpch_q14",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q15,
                "TPCH",
                tpch_q15_output,
                "tpch_q15",
            ),
            id="tpch_q15",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q16,
                "TPCH",
                tpch_q16_output,
                "tpch_q16",
            ),
            id="tpch_q16",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q17,
                "TPCH",
                tpch_q17_output,
                "tpch_q17",
            ),
            id="tpch_q17",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q18,
                "TPCH",
                tpch_q18_output,
                "tpch_q18",
            ),
            id="tpch_q18",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q19,
                "TPCH",
                tpch_q19_output,
                "tpch_q19",
            ),
            id="tpch_q19",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q20,
                "TPCH",
                tpch_q20_output,
                "tpch_q20",
            ),
            id="tpch_q20",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q21,
                "TPCH",
                tpch_q21_output,
                "tpch_q21",
            ),
            id="tpch_q21",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q22,
                "TPCH",
                tpch_q22_output,
                "tpch_q22",
            ),
            id="tpch_q22",
        ),
        pytest.param(
            # Smoke test covering slicing, INTEGER, GETPART, SMALLEST, FIND,
            # JOIN_STRINGS, LPAD, RPAD, STRING, REPLACE, LOWER, UPPER,
            # LARGEST, STRIP, SQRT and ROUND.
            PyDoughPandasTest(
                "result = parts.CALCULATE("
                " key,"
                " a=INTEGER(JOIN_STRINGS('', brand[-2:], brand[7:], brand[-2:-1])),"
                " b=UPPER(SMALLEST(GETPART(name, ' ', 2), GETPART(name, ' ', -1))),"
                " c=STRIP(name[:2], 'o'),"
                " d=LPAD(STRING(size), 3, '0'),"
                " e=RPAD(STRING(size), 3, '0'),"
                " f=REPLACE(manufacturer, 'Manufacturer#', 'm'),"
                " g=REPLACE(LOWER(container), ' '),"
                " h=STRCOUNT(name, 'o') + (FIND(name, 'o') / 100.0),"
                " i=ROUND(SQRT(LARGEST(size, 10)), 3),"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": range(1, 6),
                        "a": [1331, 1331, 4224, 3443, 3223],
                        "b": ["LACE", "SADDLE", "CORNSILK", "CHOCOLATE", "BROWN"],
                        "c": ["g", "bl", "sp", "c", "f"],
                        "d": ["007", "001", "021", "014", "015"],
                        "e": ["700", "100", "210", "140", "150"],
                        "f": ["m1", "m1", "m4", "m3", "m3"],
                        "g": ["jumbopkg", "lgcase", "wrapcase", "meddrum", "smpkg"],
                        "h": [4.01, 1.23, 2.17, 5.01, 3.01],
                        "i": [3.162, 3.162, 4.583, 3.742, 3.873],
                    }
                ),
                "smoke_a",
            ),
            id="smoke_a",
        ),
        pytest.param(
            # Smoke test covering YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE,
            # SECOND, start of year, start of quarter, start of month, start of
            # week, start of day, start of hour, start of minute, ±years,
            # ±quarters, ±months, ±weeks, ±days, ±hours, ±minutes, ±seconds,
            # DAYNAME, DATEDIFF (year, month, day, week, hour, minute, second),
            # DAYOFWEEK, JOIN_STRINGS.
            PyDoughPandasTest(
                "result = orders.WHERE("
                " STARTSWITH(order_priority, '3')"
                " & ENDSWITH(clerk, '5')"
                " & CONTAINS(comment, 'fo')"
                ").CALCULATE("
                " key,"
                " a=JOIN_STRINGS('_', YEAR(order_date), QUARTER(order_date), MONTH(order_date), DAY(order_date)),"
                " b=JOIN_STRINGS(':', DAYNAME(order_date), DAYOFWEEK(order_date)),"
                " c=DATETIME(order_date, 'start of year', '+6 months', '-13 days'),"
                " d=DATETIME(order_date, 'start of quarter', '+1 year', '+25 hours'),"
                " e=DATETIME('2025-01-01 12:35:13', 'start of minute'),"
                " f=DATETIME('2025-01-01 12:35:13', 'start of hour', '+2 quarters', '+3 weeks'),"
                " g=DATETIME('2025-01-01 12:35:13', 'start of day'),"
                " h=JOIN_STRINGS(';', HOUR('2025-01-01 12:35:13'), MINUTE(DATETIME('2025-01-01 12:35:13', '+45 minutes')), SECOND(DATETIME('2025-01-01 12:35:13', '-7 seconds'))),"
                " i=DATEDIFF('years', '1993-05-25 12:45:36', order_date),"
                " j=DATEDIFF('quarters', '1993-05-25 12:45:36', order_date),"
                " k=DATEDIFF('months', '1993-05-25 12:45:36', order_date),"
                " l=DATEDIFF('weeks', '1993-05-25 12:45:36', order_date),"
                " m=DATEDIFF('days', '1993-05-25 12:45:36', order_date),"
                " n=DATEDIFF('hours', '1993-05-25 12:45:36', order_date),"
                " o=DATEDIFF('minutes', '1993-05-25 12:45:36', order_date),"
                " p=DATEDIFF('seconds', '1993-05-25 12:45:36', order_date),"
                " q=DATETIME(order_date, 'start of week'),"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [131, 834, 4677, 7430, 8065],
                        "a": [
                            "1994_2_6_8",
                            "1994_2_5_23",
                            "1998_1_2_21",
                            "1993_2_6_9",
                            "1992_4_10_22",
                        ],
                        "b": [
                            "Wednesday:3",
                            "Monday:1",
                            "Saturday:6",
                            "Wednesday:3",
                            "Thursday:4",
                        ],
                        "c": [
                            "1994-06-18",
                            "1994-06-18",
                            "1998-06-18",
                            "1993-06-18",
                            "1992-06-18",
                        ],
                        "d": [
                            "1995-04-02 01:00:00",
                            "1995-04-02 01:00:00",
                            "1999-01-02 01:00:00",
                            "1994-04-02 01:00:00",
                            "1993-10-02 01:00:00",
                        ],
                        "e": ["2025-01-01 12:35:00"] * 5,
                        "f": ["2025-07-22 12:00:00"] * 5,
                        "g": ["2025-01-01"] * 5,
                        "h": ["12;20;6"] * 5,
                        "i": [1, 1, 5, 0, -1],
                        "j": [4, 4, 19, 0, -2],
                        "k": [13, 12, 57, 1, -7],
                        "l": [54, 52, 247, 2, -31],
                        "m": [379, 363, 1733, 15, -215],
                        "n": [9084, 8700, 41580, 348, -5172],
                        "o": [544995, 521955, 2494755, 20835, -310365],
                        "p": [32699664, 31317264, 149685264, 1250064, -18621936],
                        "q": [
                            "1994-06-05",
                            "1994-05-22",
                            "1998-02-15",
                            "1993-06-06",
                            "1992-10-18",
                        ],
                    }
                ),
                "smoke_b",
            ),
            id="smoke_b",
        ),
        pytest.param(
            # Smoke test covering SUM, COUNT, NDISTINCT, AVG, MIN, MAX,
            # ANYTHING, VAR, STD, ABS, FLOOR, CEIL, KEEP_IF, DEFAULT_TO,
            # PRESENT, ABSENT, ROUND, MEDIAN and QUANTILE.
            PyDoughPandasTest(
                "result = TPCH.CALCULATE("
                " a=COUNT(customers),"
                " b=SUM(FLOOR(customers.account_balance)),"
                " c=SUM(CEIL(customers.account_balance)),"
                " d=NDISTINCT(customers.market_segment),"
                " e=ROUND(AVG(ABS(customers.account_balance)), 4),"
                " f=MIN(customers.account_balance),"
                " g=MAX(customers.account_balance),"
                " h=ANYTHING(customers.name[:1]),"
                " i=COUNT(KEEP_IF(customers.account_balance, customers.account_balance > 0)),"
                " j=CEIL(VAR(KEEP_IF(customers.account_balance, customers.account_balance > 0), type='population')),"
                " k=ROUND(VAR(KEEP_IF(customers.account_balance, customers.account_balance < 0), type='sample'), 4),"
                " l=FLOOR(STD(KEEP_IF(customers.account_balance, customers.account_balance < 0), type='population')),"
                " m=ROUND(STD(KEEP_IF(customers.account_balance, customers.account_balance > 0), type='sample'), 4),"
                " n=ROUND(AVG(DEFAULT_TO(KEEP_IF(customers.account_balance, customers.account_balance > 0), 0)), 2),"
                " o=SUM(PRESENT(KEEP_IF(customers.account_balance, customers.account_balance > 1000))),"
                " p=SUM(ABSENT(KEEP_IF(customers.account_balance, customers.account_balance > 1000))),"
                " q=QUANTILE(customers.account_balance, 0.2),"
                " r=MEDIAN(customers.account_balance),"
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "a": [150000],
                        "b": [674252474],
                        "c": [674401000],
                        "d": [5],
                        "e": [4586.6834],
                        "f": [-999.99],
                        "g": [9999.99],
                        "h": ["C"],
                        "i": [136308],
                        "j": [8322120],
                        "k": [83910.7228],
                        "l": [289],
                        "m": [2884.819],
                        "n": [4541.1],
                        "o": [122881],
                        "p": [27119],
                        "q": [1215.91],
                        "r": [4477.3],
                    }
                ),
                "smoke_c",
            ),
            id="smoke_c",
        ),
        pytest.param(
            # Smoke test covering RANKING, RELSUM, RELAVG, RELCOUNT, RELSIZE,
            # PERCENTILE, PREV, NEXT.
            PyDoughPandasTest(
                "result = nations.WHERE(region.name == 'ASIA').customers.CALCULATE("
                " key,"
                " a=RANKING(by=(account_balance.ASC(), key.ASC())),"
                " b=RANKING(by=(account_balance.ASC(), key.ASC()), per='nations'),"
                " c=RANKING(by=market_segment.ASC(), allow_ties=True),"
                " d=RANKING(by=market_segment.ASC(), allow_ties=True, dense=True),"
                " e=PERCENTILE(by=(account_balance.ASC(), key.ASC())),"
                " f=PERCENTILE(by=(account_balance.ASC(), key.ASC()), n_buckets=12, per='nations'),"
                " g=PREV(key, by=key.ASC()),"
                " h=PREV(key, n=2, default=-1, by=key.ASC(), per='nations'),"
                " i=NEXT(key, by=key.ASC()),"
                " j=NEXT(key, n=6000, by=key.ASC(), per='nations'),"
                " k=RELSUM(account_balance, per='nations'),"
                " l=RELSUM(account_balance, by=key.ASC(), cumulative=True),"
                " m=ROUND(RELAVG(account_balance), 2),"
                " n=ROUND(RELAVG(account_balance, per='nations', by=key.ASC(), frame=(None, -1)), 2),"
                " o=RELCOUNT(KEEP_IF(account_balance, account_balance > 0)),"
                " p=RELSIZE(),"
                ")"
                ".TOP_K(10, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [7, 9, 19, 21, 25, 28, 36, 37, 38, 45],
                        "a": [
                            29000,
                            25596,
                            27275,
                            6657,
                            22305,
                            5509,
                            16404,
                            246,
                            20106,
                            30146,
                        ],
                        "b": [5803, 5081, 5465, 1367, 4416, 1119, 3265, 53, 3975, 6156],
                        "c": [
                            1,
                            12012,
                            18100,
                            24087,
                            12012,
                            12012,
                            5924,
                            12012,
                            18100,
                            1,
                        ],
                        "d": [1, 3, 4, 5, 3, 3, 2, 3, 4, 1],
                        "e": [97, 85, 91, 23, 74, 19, 55, 1, 67, 100],
                        "f": [12, 11, 11, 3, 9, 3, 7, 1, 9, 12],
                        "g": [None, 7, 9, 19, 21, 25, 28, 36, 37, 38],
                        "h": [-1, -1, -1, -1, -1, 9, -1, 21, -1, -1],
                        "i": [9, 19, 21, 25, 28, 36, 37, 38, 45, 51],
                        "j": [
                            149394,
                            149030,
                            149411,
                            149032,
                            None,
                            149036,
                            149751,
                            149062,
                            None,
                            146097,
                        ],
                        "k": [
                            26740212.13,
                            27293627.48,
                            26740212.13,
                            27293627.48,
                            26898468.71,
                            27293627.48,
                            27081997.67,
                            27293627.48,
                            26898468.71,
                            27930482.5,
                        ],
                        "l": [
                            9561.95,
                            17886.02,
                            26800.73,
                            28228.98,
                            35362.68,
                            36369.86,
                            41357.13,
                            40439.38,
                            46784.49,
                            56767.87,
                        ],
                        "m": [4504.02] * 10,
                        "n": [
                            None,
                            None,
                            9561.95,
                            8324.07,
                            None,
                            4876.16,
                            None,
                            3586.5,
                            7133.7,
                            None,
                        ],
                        "o": [27454] * 10,
                        "p": [30183] * 10,
                    }
                ),
                "smoke_d",
            ),
            id="smoke_d",
        ),
    ],
)
def tpch_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the 22 TPC-H queries, as well as some additional
    smoke tests to ensure various functions work as-intended. Returns an
    instance of PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                "result = customers.CALCULATE("
                "key,"
                "name[:],"
                "phone,"
                "next_digits=phone[3:6],"
                "country_code=phone[:3],"
                "name_without_first_char=name[1:],"
                "last_digit=phone[-1:],"
                "name_without_start_and_end_char=name[1:-1],"
                "phone_without_last_5_chars=phone[:-5],"
                "name_second_to_last_char=name[-2:-1],"
                "cust_number=name[-10:18]"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 6)),
                        "name": [
                            "Customer#000000001",
                            "Customer#000000002",
                            "Customer#000000003",
                            "Customer#000000004",
                            "Customer#000000005",
                        ],
                        "phone": [
                            "25-989-741-2988",
                            "23-768-687-3665",
                            "11-719-748-3364",
                            "14-128-190-5944",
                            "13-750-942-6364",
                        ],
                        "next_digits": [
                            "989",
                            "768",
                            "719",
                            "128",
                            "750",
                        ],
                        "country_code": ["25-", "23-", "11-", "14-", "13-"],
                        "name_without_first_char": [
                            "ustomer#000000001",
                            "ustomer#000000002",
                            "ustomer#000000003",
                            "ustomer#000000004",
                            "ustomer#000000005",
                        ],
                        "last_digit": ["8", "5", "4", "4", "4"],
                        "name_without_start_and_end_char": [
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                        ],
                        "phone_without_last_5_chars": [
                            "25-989-741",
                            "23-768-687",
                            "11-719-748",
                            "14-128-190",
                            "13-750-942",
                        ],
                        "name_second_to_last_char": ["0", "0", "0", "0", "0"],
                        "cust_number": [
                            "#000000001",
                            "#000000002",
                            "#000000003",
                            "#000000004",
                            "#000000005",
                        ],
                    }
                ),
                "slicing_test",
            ),
            id="slicing_test",
        ),
        pytest.param(
            PyDoughPandasTest(
                """result = customers.CALCULATE(
                    key,
                    p1=GETPART(name, "#", key),
                    p2=GETPART(name, "0", key),
                    p3=GETPART(address, ",", key),
                    p4=GETPART(address, ",", -key),
                    p5=GETPART(phone, "-", key),
                    p6=GETPART(phone, "-", -key),
                    p7=GETPART(comment, " ", key),
                    p8=GETPART(comment, " ", -key),
                    p9=GETPART(address, "!", key),
                    p10=GETPART(market_segment, "O", -key),
                    p11=GETPART(name, "00000000", key),
                    p12=GETPART("^%1$$@@@##2$#&@@@*^%3$#", "@@@", -key),
                    p13=GETPART(name, "", key),
                    p14=GETPART("", " ", key),
                    p15=GETPART(name, "#", 0),
                    p16=GETPART(nation.name, nation.name, key),
                    p17=GETPART(GETPART(phone, "-", key), "7", 2)
                ).TOP_K(4, by=key.ASC())""",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "k": [1, 2, 3, 4],
                        "p1": ["Customer", "000000002", None, None],
                        "p2": ["Customer#", "", "", ""],
                        "p3": ["IVhzIApeRb ot", "NCwDVaWNe6tEgvwfmRchLXak", None, None],
                        "p4": ["E", "XSTf4", None, None],
                        "p5": ["25", "768", "748", "5944"],
                        "p6": ["2988", "687", "719", "14"],
                        "p7": ["to", "accounts.", "eat", "regular"],
                        "p8": ["e", "boldly:", "even", "ideas"],
                        "p9": ["IVhzIApeRb ot,c,E", None, None, None],
                        "p10": ["BUILDING", "M", "AUT", None],
                        "p11": ["Customer#", "2", None, None],
                        "p12": ["*^%3$#", "##2$#&", "^%1$$", None],
                        "p13": ["Customer#000000001", None, None, None],
                        "p14": [None, None, None, None],
                        "p15": ["Customer", "Customer", "Customer", "Customer"],
                        "p16": ["", "", None, None],
                        "p17": [None, "68", "48", None],
                    }
                ),
                "get_part_test",
            ),
            id="get_part_test",
        ),
    ],
)
def custom_functions_test_data(request) -> PyDoughPandasTest:
    """
    Test data for testing different functions of PyDough using TPCH database.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param
