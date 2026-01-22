"""
Tests for the session module. This unit tests the core functionality
of any session.

For each of these tests we create a new session so that we can
manipulate the session without affecting other tests.

In addition, we do some testing on the defaults/supported behavior
for the active session to verify that it works. Importantly, in all
tests anything that impacts the active session must be READ-ONLY to
ensure that we don't affect other tests, or that in the teardown step
we replace the active session with a new default session.
"""

from collections.abc import Callable

import pandas as pd
import pytest

import pydough
from pydough.configs import (
    ConfigProperty,
    DivisionByZeroBehavior,
    PyDoughConfigs,
    PyDoughSession,
)
from pydough.database_connectors import (
    DatabaseContext,
    DatabaseDialect,
    empty_connection,
    load_database_context,
)
from pydough.errors import PyDoughSQLException
from pydough.metadata import GraphMetadata, parse_json_metadata_from_file
from pydough.unqualified import UnqualifiedNode
from tests.test_pydough_functions.simple_pydough_functions import (
    simple_division_by_zero,
    simple_scan,
)
from tests.testing_utilities import (
    PyDoughPandasTest,
    graph_fetcher,
)


@pytest.mark.parametrize(
    "session",
    [
        pytest.param(PyDoughSession(), id="newSession"),
        pytest.param(pydough.active_session, id="activeSession"),
    ],
)
def test_defaults(session: PyDoughSession) -> None:
    """
    Tests that a sessions defaults are set correctly.
    """
    assert session.metadata is None
    assert session.config is not None
    default_config: PyDoughConfigs = PyDoughConfigs()
    for key, value in PyDoughConfigs.__dict__.items():
        if isinstance(value, ConfigProperty):
            assert getattr(session.config, key) == getattr(default_config, key), (
                f"Configuration value {key} doesn't match the default value."
            )
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


def test_active_session_to_sql(sample_graph_path: str) -> None:
    """
    Verify that the active session can generate SQL by default
    without any configuration.
    """
    output_query: str = """
SELECT
  o_orderkey AS key
FROM tpch.orders
"""
    try:
        # Load metadata for the session
        old_metadata: GraphMetadata | None = pydough.active_session.metadata
        graph: GraphMetadata = pydough.active_session.load_metadata_graph(
            sample_graph_path, "TPCH"
        )
        root: UnqualifiedNode = pydough.init_pydough_context(graph)(simple_scan)()
        output = pydough.to_sql(root)
        assert output == output_query.strip()
    finally:
        pydough.active_session.metadata = old_metadata


@pytest.fixture(
    params=[
        pytest.param(
            (
                DivisionByZeroBehavior.DATABASE,
                PyDoughPandasTest(
                    simple_division_by_zero,
                    "TPCH",
                    lambda: pd.DataFrame({"computed_value": [None]}),
                    "division_by_zero_database",
                ),
            ),
            id="database",
        ),
        pytest.param(
            (
                DivisionByZeroBehavior.NULL,
                PyDoughPandasTest(
                    simple_division_by_zero,
                    "TPCH",
                    lambda: pd.DataFrame({"computed_value": [None]}),
                    "division_by_zero_null",
                ),
            ),
            id="null",
        ),
        pytest.param(
            (
                DivisionByZeroBehavior.ZERO,
                PyDoughPandasTest(
                    simple_division_by_zero,
                    "TPCH",
                    lambda: pd.DataFrame({"computed_value": [0]}),
                    "division_by_zero_zero",
                ),
            ),
            id="zero",
        ),
    ],
)
def division_by_zero_test_data(
    request,
) -> tuple[DivisionByZeroBehavior, PyDoughPandasTest]:
    """
    Test data for division by zero SQL generation tests.
    """
    return request.param


def test_division_by_zero_to_sql(
    division_by_zero_test_data: tuple[DivisionByZeroBehavior, PyDoughPandasTest],
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Tests that division by zero SQL is correctly generated for all dialects.
    """
    division_by_zero_config, test_data = division_by_zero_test_data

    # Create config with the specified division_by_zero behavior
    config = PyDoughConfigs()
    config.division_by_zero = division_by_zero_config

    file_path: str = get_sql_test_filename(
        test_data.test_name, empty_context_database.dialect
    )
    test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database, config=config
    )


@pytest.fixture(
    params=[
        pytest.param("sqlite", id="sqlite"),
        pytest.param(
            "snowflake",
            id="snowflake",
            marks=[pytest.mark.snowflake, pytest.mark.execute],
        ),
        pytest.param(
            "mysql",
            id="mysql",
            marks=[pytest.mark.mysql, pytest.mark.execute],
        ),
        pytest.param(
            "postgres",
            id="postgres",
            marks=[pytest.mark.postgres, pytest.mark.execute],
        ),
    ],
)
def division_by_zero_db_context(
    request,
    sqlite_tpch_db_context: DatabaseContext,
    sf_conn_db_context: Callable,
    mysql_conn_db_context: Callable,
    postgres_conn_db_context: DatabaseContext,
    get_sample_graph: graph_fetcher,
    get_sf_sample_graph: graph_fetcher,
) -> tuple[DatabaseContext, GraphMetadata]:
    """
    Returns database context and graph metadata for each supported database.
    """
    match request.param:
        case "sqlite":
            return sqlite_tpch_db_context, get_sample_graph("TPCH")
        case "snowflake":
            return (
                sf_conn_db_context("SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"),
                get_sf_sample_graph("TPCH"),
            )
        case "mysql":
            return mysql_conn_db_context("tpch"), get_sample_graph("TPCH")
        case "postgres":
            return postgres_conn_db_context, get_sample_graph("TPCH")
    return sqlite_tpch_db_context, get_sample_graph("TPCH")


@pytest.mark.execute
@pytest.mark.parametrize(
    "division_by_zero_config",
    [
        pytest.param(DivisionByZeroBehavior.DATABASE, id="database"),
        pytest.param(DivisionByZeroBehavior.NULL, id="null"),
        pytest.param(DivisionByZeroBehavior.ZERO, id="zero"),
    ],
)
def test_division_by_zero_e2e(
    division_by_zero_config: DivisionByZeroBehavior,
    division_by_zero_db_context: tuple[DatabaseContext, GraphMetadata],
) -> None:
    """
    Tests division by zero behavior across all supported databases.
    """
    db_context, graph = division_by_zero_db_context

    new_configs = PyDoughConfigs()
    new_configs.division_by_zero = division_by_zero_config

    root: UnqualifiedNode = pydough.init_pydough_context(graph)(
        simple_division_by_zero
    )()

    # DATABASE mode: Snowflake/Postgres throw errors, SQLite/MySQL return NULL
    if division_by_zero_config == DivisionByZeroBehavior.DATABASE:
        if db_context.dialect in (DatabaseDialect.SNOWFLAKE, DatabaseDialect.POSTGRES):
            with pytest.raises(PyDoughSQLException, match="[Dd]ivision by zero"):
                pydough.to_df(
                    root,
                    metadata=graph,
                    database=db_context,
                    config=new_configs,
                )
            return

    output = pydough.to_df(
        root,
        metadata=graph,
        database=db_context,
        config=new_configs,
    )

    # Snowflake returns uppercase column names
    col_name = (
        "COMPUTED_VALUE"
        if db_context.dialect == DatabaseDialect.SNOWFLAKE
        else "computed_value"
    )

    # Determine expected result based on config
    if division_by_zero_config == DivisionByZeroBehavior.ZERO:
        expected_df = pd.DataFrame({col_name: [0.0]})
    else:
        # DATABASE (for sqlite/mysql) and NULL both return NULL
        expected_df = pd.DataFrame({col_name: [None]})

    pd.testing.assert_frame_equal(output, expected_df, check_dtype=False)
