"""
TODO: add file-level docstring.
"""

import pydough
import pytest
import json
from pydough.metadata.graphs import GraphMetadata
from typing import MutableMapping, Set
from pydough.pydough_ast import AstNodeBuilder, pydough_operators as pydop

from test_utils import graph_fetcher, noun_fetcher, map_over_dict_values

import os
import sqlite3
from pydough.database_connectors.database_connector import DatabaseConnection


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
def valid_sample_graph_names() -> Set[str]:
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
    valid_sample_graph_names: Set[str],
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
    sample_graph_nouns_path: str, valid_sample_graph_names: Set[str]
) -> noun_fetcher:
    """
    A function that takes in the name of a graph (currently only supports the
    values 'amazon', 'tpch', and 'empty') and returns the metadata for that
    PyDough graph.
    """

    def impl(name: str) -> MutableMapping[str, Set[str]]:
        if name not in valid_sample_graph_names:
            raise Exception(f"Unrecognized graph name '{name}'")
        nouns: MutableMapping[str, Set[str]]
        with open(sample_graph_nouns_path, "r") as f:
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


@pytest.fixture(scope="session")
def sqlite3_people_jobs() -> DatabaseConnection:
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
