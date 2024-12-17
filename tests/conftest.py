"""
Definitions of various fixtures used in PyDough tests that are automatically
available.
"""

import json
import os
import sqlite3
from collections.abc import MutableMapping

import pytest
from test_utils import graph_fetcher, map_over_dict_values, noun_fetcher

import pydough
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import (
    DatabaseConnection,
    DatabaseContext,
    DatabaseDialect,
)
from pydough.metadata.graphs import GraphMetadata
from pydough.pydough_ast import AstNodeBuilder
from pydough.pydough_ast import pydough_operators as pydop


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
    return config


@pytest.fixture(scope="session")
def sample_graph_path() -> str:
    """
    Tuple of the path to the JSON file containing the sample graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/sample_graphs.json"


@pytest.fixture(scope="session")
def sample_graph_nouns_path() -> str:
    """
    Tuple of the path to the JSON file containing the nouns for each
    of the sample graphs.
    """
    return f"{os.path.dirname(__file__)}/test_metadata/sample_graphs_nouns.json"


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
    return {"Amazon", "TPCH", "Empty"}


@pytest.fixture(params=["Amazon", "TPCH", "Empty"])
def sample_graph_names(request) -> str:
    """
    Fixture for the names that each of the sample graphs can be accessed.
    """
    return request.param


@pytest.fixture
def get_sample_graph(
    sample_graph_path: str,
    valid_sample_graph_names: set[str],
) -> graph_fetcher:
    """
    A function that takes in the name of a graph from the supported sample
    graph names and returns the metadata for that PyDough graph.
    """

    def impl(name: str) -> GraphMetadata:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        return pydough.parse_json_metadata_from_file(
            file_path=sample_graph_path, graph_name=name
        )

    return impl


@pytest.fixture
def get_sample_graph_nouns(
    sample_graph_nouns_path: str, valid_sample_graph_names: set[str]
) -> noun_fetcher:
    """
    A function that takes in the name of a graph (currently only supports the
    values 'amazon', 'tpch', and 'empty') and returns the metadata for that
    PyDough graph.
    """

    def impl(name: str) -> MutableMapping[str, set[str]]:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        nouns: MutableMapping[str, set[str]]
        with open(sample_graph_nouns_path) as f:
            nouns = json.load(f)[name]
        # Convert the noun values for each name from a list to a set
        return map_over_dict_values(nouns, set)

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


@pytest.fixture
def tpch_node_builder(get_sample_graph) -> AstNodeBuilder:
    """
    Builds an AST node builder using the TPCH graoh.
    """
    return AstNodeBuilder(get_sample_graph("TPCH"))


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
