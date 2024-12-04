"""
Tests for the session module. This doesn't directly test the
active session but instead unit tests the core functionality
of any session.

For each of these tests we create a new session so that we can
manipulate the session without affecting other tests.
"""

import pandas as pd

from pydough.configs import PyDoughConfigs, PyDoughSession
from pydough.database_connectors import (
    DatabaseConnection,
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
)
from pydough.metadata import GraphMetadata


def test_defaults() -> None:
    """
    Tests that a sessions defaults are set correctly.
    """
    session: PyDoughSession = PyDoughSession()
    assert session.metadata is None
    assert session.config is not None
    default_config: PyDoughConfigs = PyDoughConfigs()
    # TODO: Add an API to iterate and check all of the properties
    # match the defaults.
    assert session.config.sum_default_zero is default_config.sum_default_zero
    assert session.config.avg_default_zero is default_config.avg_default_zero
    assert session.database is not None
    assert session.database.connection is empty_connection
    assert session.database.dialect is DatabaseDialect.ANSI


def test_load_metadata_graph(sample_graph_path: str, sample_graph_names: str) -> None:
    """
    Tests that we can load the metadata graph on the session.
    Also checks a couple features to ensure we have loaded the correct graph.
    """
    session: PyDoughSession = PyDoughSession()
    graph: GraphMetadata = session.load_metadata_graph(
        sample_graph_path, sample_graph_names
    )
    assert graph is session.metadata
    assert graph.name == sample_graph_names
    assert graph.path == sample_graph_names
    assert graph.components == [sample_graph_names]


# TODO: Add a test that we can generate SQL using a session's default.
# This is not possible until we have the to_sql() APIs working.


def test_connect_sqlite_database() -> None:
    """
    Tests that we can connect to a SQLite database,
    it can execute SQL, and that the session is updated.
    """
    session: PyDoughSession = PyDoughSession()
    database: DatabaseContext = session.connect_database("sqlite", database=":memory:")
    assert database is session.database
    assert database.connection is not empty_connection
    assert database.dialect is DatabaseDialect.SQLITE
    connection_paths: list[DatabaseConnection] = [
        session.database.connection,
        database.connection,
    ]
    for connection in connection_paths:
        result: pd.DataFrame = connection.execute_query_df("Select 1 as A")
        pd.testing.assert_frame_equal(result, pd.DataFrame({"A": [1]}))
