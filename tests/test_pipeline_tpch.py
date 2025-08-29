"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import (
    graph_fetcher,
)

from .testing_utilities import PyDoughPandasTest


def test_pipeline_until_relational_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on the
    22 TPC-H queries.
    """
    file_path: str = get_plan_test_filename(tpch_pipeline_test_data.test_name)
    tpch_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Same as test_pipeline_until_relational_tpch, but for the generated SQL text.
    """
    file_path: str = get_sql_test_filename(
        tpch_pipeline_test_data.test_name, empty_context_database.dialect
    )
    tpch_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        sqlite_tpch_db_context,
    )


@pytest.mark.pgsql
@pytest.mark.execute
def test_pipeline_e2e_pgsql_conn(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    postgres_conn_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation on Postgres.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        postgres_conn_tpch_db_context,
        coerce_types=True,
    )
