"""
Tests for the session module. This doesn't directly test the
active session but instead unit tests the core functionality
of any session.

For each of these tests we create a new session so that we can
manipulate the session without affecting other tests.
"""

import pandas as pd

from pydough.configs import ConfigProperty, PyDoughConfigs, PyDoughSession
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
    load_database_context,
)
from pydough.metadata import GraphMetadata, parse_json_metadata_from_file


def test_defaults() -> None:
    """
    Tests that a sessions defaults are set correctly.
    """
    session: PyDoughSession = PyDoughSession()
    assert session.metadata is None
    assert session.config is not None
    default_config: PyDoughConfigs = PyDoughConfigs()
    for key, value in PyDoughConfigs.__dict__.items():
        if isinstance(value, ConfigProperty):
            assert getattr(session.config, key) == getattr(
                default_config, key
            ), f"Configuration value {key} doesn't match the default value."
    assert session.database is not None
    assert session.database.connection is empty_connection
    assert session.database.dialect is DatabaseDialect.ANSI


def test_setting_config() -> None:
    """
    Test that the config property can be set directly
    through a setter for a session.
    """
    session: PyDoughSession = PyDoughSession()
    old_config: PyDoughConfigs = session.config
    new_config: PyDoughConfigs = PyDoughConfigs()
    session.config = new_config
    assert session.config is new_config
    assert session.config is not old_config


def test_setting_metadata(sample_graph_path: str, sample_graph_names: str) -> None:
    """
    Test that the metadata property can be set directly
    through a setter for a session.
    """
    session: PyDoughSession = PyDoughSession()
    graph: GraphMetadata = parse_json_metadata_from_file(
        sample_graph_path, sample_graph_names
    )
    old_graph: GraphMetadata | None = session.metadata
    session.metadata = graph
    assert session.metadata is graph and graph is not None
    assert old_graph is None


def test_setting_database() -> None:
    """
    Test that the database property can be set directly.
    """
    session: PyDoughSession = PyDoughSession()
    database: DatabaseContext = load_database_context("sqlite", database=":memory:")
    old_database: DatabaseContext = session.database
    session.database = database
    assert session.database is database
    assert session.database is not old_database


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
    result: pd.DataFrame = session.database.connection.execute_query_df("Select 1 as A")
    pd.testing.assert_frame_equal(result, pd.DataFrame({"A": [1]}))
