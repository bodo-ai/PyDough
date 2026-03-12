"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.metadata.graphs.graph_metadata import GraphMetadata
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
    if (
        tpch_pipeline_test_data.test_name == "dataframe_collection_inf"
        and empty_context_database.dialect == DatabaseDialect.MYSQL
    ):
        pytest.skip("Skipping test as MySQL does not support Infinity values.")

    file_path: str = get_sql_test_filename(
        tpch_pipeline_test_data.test_name, empty_context_database.dialect
    )
    tpch_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    all_dialects_tpch_db_context: tuple[DatabaseContext, GraphMetadata],
):
    """
    Test executing the TPC-H queries from the original code generation.
    """

    db_context, graph = all_dialects_tpch_db_context

    if db_context.dialect == DatabaseDialect.BODOSQL:
        # Skip any of these tests due to various errors/gaps.
        tests_skipped: dict[str, str] = {
            "tpch_q16": "BodoSQL Iceberg STARTSWITH read bug",
            "tpch_q20": "BodoSQL Iceberg STARTSWITH read bug",
            "smoke_b": "BodoSQL Iceberg STARTSWITH read bug",
            "smoke_c": "Unknown Bodo/BodoSQL typing bug with array concatenation",
        }
        if tpch_pipeline_test_data.test_name in tests_skipped:
            pytest.skip(tests_skipped[tpch_pipeline_test_data.test_name])

    tpch_pipeline_test_data.run_e2e_test(lambda _: graph, db_context, coerce_types=True)
