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
                "result = customers.TOP_K(3, by=key)",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3],
                        "first_name": ["alice", "bob", "carol"],
                        "last_name": ["johnson", "smith", "lee"],
                        "phone": ["555-123-4567", "555-234-5678", "555-345-6789"],
                        "email": [
                            "alice_j@example.org",
                            "bob.smith77@gmail.com",
                            "c.lee@outlook.com",
                        ],
                        "address": [
                            "123 Maple St;Portland;OR;97205",
                            "456 Oak Ave;Seattle;WA;98101",
                            "789 Pine Rd;Las Vegas;NV;89101",
                        ],
                        "birthday": ["1985-04-12", "1990-07-23", "1982-11-05"],
                    }
                ),
                "basic_scan_topk",
            ),
            id="basic_scan_topk",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name == 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "filter_count_1",
            ),
            id="filter_count_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name != 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "filter_count_2",
            ),
            id="filter_count_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [6]}),
                "filter_count_3",
            ),
            id="filter_count_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [14]}),
                "filter_count_4",
            ),
            id="filter_count_4",
        ),
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
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
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
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
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
def test_pipeline_e2e_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_cryptbank_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom cryptbank dataset
    against the refsol DataFrame.
    """
    cryptbank_pipeline_test_data.run_e2e_test(
        masked_graphs,
        sqlite_cryptbank_connection,
    )
