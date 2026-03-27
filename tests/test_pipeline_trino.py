"""
Integration tests for the PyDough workflow on various queries using Trino.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pytest
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)

from tests.test_pydough_functions.simple_pydough_functions import week_offset

from tests.testing_utilities import (
    graph_fetcher,
    PyDoughSQLComparisonTest,
)

from .conftest import tpch_custom_test_data_dialect_replacements

from .test_pipeline_defog_custom import defog_custom_pipeline_test_data
from .test_pipeline_defog import defog_pipeline_test_data
from .test_pipeline_custom_datasets import custom_datasets_test_data  # noqa
from .test_pipeline_tpch_custom import tpch_custom_pipeline_test_data  # noqa

from .testing_utilities import PyDoughPandasTest


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_tpch_trino_params(
    get_trino_graphs: graph_fetcher,
    trino_params_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with Trino as the executing database. Using `host`, `port`,
    `user`, `catalog`, `schema`, and `warehouse` as keyword arguments to the
    DatabaseContext. Only tests using TPC-H query 16, since the rest of the
    tests are already covered with the trino connection test.
    """
    test_data: PyDoughPandasTest = PyDoughPandasTest(
        impl_tpch_q16,
        "TPCH",
        tpch_q16_output,
        "tpch_q16_params",
    )
    test_data.run_e2e_test(
        get_trino_graphs,
        trino_params_tpch_db_context,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_trino_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
    get_trino_graphs: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H custom queries from the original code generation on
    Trino.
    """
    tpch_custom_pipeline_test_data = tpch_custom_test_data_dialect_replacements(
        trino_conn_db_context.dialect,
        tpch_custom_pipeline_test_data,
    )

    tpch_custom_pipeline_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.trino
@pytest.mark.execute
def test_defog_e2e(
    defog_pipeline_test_data: PyDoughSQLComparisonTest,
    get_trino_graphs: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
    defog_config: PyDoughConfigs,
    sqlite_defog_connection: DatabaseContext,
) -> None:
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on the defog.ai queries.
    NOTE: passing SQLite connection as reference database so that refsol
    is executed using SQLite.
    This is needed because refsol uses SQLite SQL syntax to obtain
    the correct results.
    """
    defog_pipeline_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        defog_config,
        reference_database=sqlite_defog_connection,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_trino_e2e_defog_custom(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
    get_trino_graphs: graph_fetcher,
    defog_config: PyDoughConfigs,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the defog analytical queries with Trino database.
    """
    defog_custom_pipeline_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        config=defog_config,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.custom_datasets
@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_trino_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,  # noqa: F811
    get_custom_datasets_graph: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_custom_datasets_graph,
        trino_conn_db_context,
        coerce_types=True,
        atol=5e-3,
    )
