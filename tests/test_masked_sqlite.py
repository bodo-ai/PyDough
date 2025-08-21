"""
Integration tests for the PyDough workflow with custom questions on the custom
CRYPTBANK sqlite database.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                "TODO",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "era_name": [
                            "WWI",
                            "Interwar",
                            "WWII",
                            "Cold War",
                            "Modern Era",
                        ],
                        "event_name": [
                            "Assassination of Archduke Ferdinand",
                            "Founding of the League of Nations",
                            "Invasion of Poland",
                            "First Meeting of the United Nations General Assembly",
                            "Dissolution of the Soviet Union",
                        ],
                    }
                ),
                "TODO",
            ),
            id="TODO",
        )
    ],
)
def cryptbank_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using cryptbank test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(cryptbank_pipeline_test_data.test_name)
    cryptbank_pipeline_test_data.run_relational_test(
        masked_graphs, file_path, update_tests
    )


def test_pipeline_until_sql_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom epoch dataset
    into SQL text.
    """
    file_path: str = get_sql_test_filename(
        cryptbank_pipeline_test_data.test_name, sqlite_tpch_db_context.dialect
    )
    cryptbank_pipeline_test_data.run_sql_test(
        masked_graphs,
        file_path,
        update_tests,
        sqlite_tpch_db_context,
    )


@pytest.mark.execute
def test_pipeline_e2e_epoch(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_cryptbank_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom epoch dataset against
    the refsol DataFrame.
    """
    cryptbank_pipeline_test_data.run_e2e_test(
        masked_graphs,
        sqlite_cryptbank_connection,
    )
