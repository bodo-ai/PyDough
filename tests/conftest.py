"""
Definitions of various fixtures used in PyDough tests that are automatically
available.
"""

import os
import sqlite3
import subprocess
from collections.abc import Callable
from functools import cache

import pandas as pd
import pytest
from gen_data.gen_technograph import gen_technograph_records
from test_utils import graph_fetcher

import pydough
import pydough.pydough_operators as pydop
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import (
    DatabaseConnection,
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
)
from pydough.metadata.graphs import GraphMetadata
from pydough.qdag import AstNodeBuilder


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
    subprocess.run("cd tests; bash setup_defog.sh", shell=True)
    path: str = os.path.join(base_dir, "tests/defog.db")
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
        "cd tests; rm -fv epoch.db; sqlite3 epoch.db < init_epoch.sql", shell=True
    )
    path: str = os.path.join(base_dir, "tests/epoch.db")
    connection: sqlite3.Connection = sqlite3.connect(path)
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)


@pytest.fixture(scope="session")
def sqlite_technograph_connection() -> DatabaseContext:
    """
    Returns the SQLITE database connection for the technograph database.
    """
    # Setup the directory to be the main PyDough directory.
    base_dir: str = os.path.dirname(os.path.dirname(__file__))
    path: str = os.path.join(base_dir, "tests/technograph.db")
    # Delete the existing database.
    subprocess.run(
        "cd tests; rm -fv technograph.db; sqlite3 technograph.db < init_technograph.sql",
        shell=True,
    )
    # Setup the database
    connection: sqlite3.Connection = sqlite3.connect(path)
    cursor: sqlite3.Cursor = connection.cursor()
    gen_technograph_records(cursor)
    print(pd.DataFrame(cursor.execute("select * from countries").fetchall()))
    print(pd.DataFrame(cursor.execute("select * from products").fetchall()))
    print(pd.DataFrame(cursor.execute("select * from errors").fetchall()))
    print(pd.DataFrame(cursor.execute("select * from devices").fetchall()))
    print(pd.DataFrame(cursor.execute("select * from incidents").fetchall()))
    # pd.DataFrame(cursor.execute("select * from products").fetchall()).sort_values(by=1)
    # pd.DataFrame(cursor.execute("select * from devices").fetchall())[1].value_counts()
    # pd.DataFrame(cursor.execute("select * from devices").fetchall())[5].apply(lambda x: pd.Timestamp(x).hour)
    # pd.DataFrame(cursor.execute("select * from incidents").fetchall())[6].value_counts()
    # pd.DataFrame(cursor.execute("select count(distinct in_device_id) from incidents").fetchall())
    # pd.DataFrame(cursor.execute("select in_device_id, count(*) from incidents group by 1 order by 2 asc").fetchall())
    # pd.DataFrame(cursor.execute("select pr_type, pr_brand, (100.0 * count(in_id)) / (1.0 * count(distinct de_id)) from incidents, devices, products WHERE in_device_id = de_id AND de_product_id = pr_id group by 1, 2").fetchall())
    # pd.DataFrame(cursor.execute("select de_production_country_id, pr_brand, (100.0 * count(in_id)) / (1.0 * count(distinct de_id)) from incidents, devices, products WHERE in_device_id = de_id AND de_product_id = pr_id group by 1, 2").fetchall())

    # pd.DataFrame(cursor.execute("select pr_name, (1.0 * count(in_id)) / (1.0 * count(distinct de_id)) from devices, countries, products left join incidents on in_device_id = de_id WHERE de_production_country_id = co_id AND de_product_id = pr_id group by 1").fetchall()).sort_values(by=1)
    # print(pd.DataFrame(cursor.execute("select pr_name, (1.0 * count(in_id)) / (1.0 * count(distinct de_id)) from devices, countries, products left join incidents on in_device_id = de_id WHERE de_production_country_id = co_id AND de_product_id = pr_id group by 1").fetchall()).sort_values(by=1).to_string())

    # pd.DataFrame(cursor.execute("select in_error_report_ts, in_service_review from incidents").fetchall()).sort_values(by=0)
    #
    # pd.DataFrame(cursor.execute("select co_name, pr_name, (1.0 * count(in_id)) / (1.0 * count(distinct de_id)) from devices, countries, products left join incidents on in_device_id = de_id WHERE de_production_country_id = co_id AND de_product_id = pr_id group by 1, 2").fetchall()).sort_values(by=2)
    # pd.DataFrame(cursor.execute("select co_name, pr_name, in_error_id, (1.0 * count(in_id)) / (1.0 * count(distinct de_id)) from devices, countries, products left join incidents on in_device_id = de_id WHERE de_production_country_id = co_id AND de_product_id = pr_id group by 1, 2, 3").fetchall()).sort_values(by=3)
    # pd.DataFrame(cursor.execute("select co_name, pr_name, in_error_id, COUNT(distinct de_id), COUNT(in_error_id) from devices, countries, products left join incidents on in_device_id = de_id WHERE de_production_country_id = co_id AND de_product_id = pr_id AND co_name = 'US' AND pr_name='RubyBolt-II' group by 1, 2, 3").fetchall()).sort_values(by=4)
    #
    # pd.DataFrame(cursor.execute("select pr_type, pr_brand, count(de_product_id) from products left join devices ON de_product_id = pr_id group by 1, 2").fetchall())
    breakpoint()
    # Return the database context.
    return DatabaseContext(DatabaseConnection(connection), DatabaseDialect.SQLITE)
