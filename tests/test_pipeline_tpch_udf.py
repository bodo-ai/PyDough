"""
Integration tests for the PyDough workflow with custom questions on the TPC-H
dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.udf_pydough_functions import (
    sqlite_combine_strings,
    sqlite_format_datetime,
    sqlite_percent_positive,
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                sqlite_format_datetime,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "key": [2159139, 1600323, 823814, 5267200, 5363650],
                        "d1": [
                            "23/04/1998",
                            "18/04/1992",
                            "31/01/1992",
                            "21/11/1993",
                            "14/01/1994",
                        ],
                        "d2": [
                            "1998:113",
                            "1992:109",
                            "1992:031",
                            "1993:325",
                            "1994:014",
                        ],
                        "d3": [893289600, 703555200, 696816000, 753840000, 758505600],
                    }
                ),
                "sqlite_format_datetime",
            ),
            id="sqlite_format_datetime",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_combine_strings,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "s1": ["AFRICA,AMERICA,ASIA,EUROPE,MIDDLE EAST"],
                        "s2": ["AFRICA, AMERICA, ASIA, MIDDLE EAST"],
                        "s3": ["AABCEEFGIIIIJJKMMPCRSVRUU"],
                        "s4": ["NOT SPECIFIED <=> MEDIUM <=> URGENT <=> LOW <=> HIGH"],
                    }
                ),
                "sqlite_combine_strings",
            ),
            id="sqlite_combine_strings",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_percent_positive,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "pct_cust_positive": [90.8, 90.81, 90.96, 90.97, 90.82],
                        "pct_supp_positive": [90.49, 91.94, 90.46, 91.04, 91.73],
                    }
                ),
                "sqlite_percent_positive",
            ),
            id="sqlite_percent_positive",
        ),
    ],
)
def tpch_sqlite_udf_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on custom queries using the TPC-H database and
    sqlite UDFs. Returns an instance of PyDoughPandasTest containing
    information about the test.
    """
    return request.param


def test_pipeline_until_relational_tpch_sqlite_udf(
    tpch_sqlite_udf_pipeline_test_data: PyDoughPandasTest,
    get_udf_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    relational tree, with the correct string representation. Run on custom
    queries with the TPC-H graph and sqlite-specific UDFs.
    """
    file_path: str = get_plan_test_filename(
        tpch_sqlite_udf_pipeline_test_data.test_name
    )
    tpch_sqlite_udf_pipeline_test_data.run_relational_test(
        get_udf_graph, file_path, update_tests
    )


def test_pipeline_until_sql_tpch_sqlite_udf(
    tpch_sqlite_udf_pipeline_test_data: PyDoughPandasTest,
    get_udf_graph: graph_fetcher,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    sqlite_tpch_db_context: DatabaseContext,
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    equivalent SQL text with the correct string representation. Run on custom
    queries with the TPC-H graph and sqlite-specific UDFs.
    """
    file_path: str = get_sql_test_filename(
        tpch_sqlite_udf_pipeline_test_data.test_name, sqlite_tpch_db_context.dialect
    )
    tpch_sqlite_udf_pipeline_test_data.run_sql_test(
        get_udf_graph, file_path, update_tests, sqlite_tpch_db_context
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch_sqlite_udf(
    tpch_sqlite_udf_pipeline_test_data: PyDoughPandasTest,
    get_udf_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the the custom queries with TPC-H data from the original
    code generation using user defined functions
    """
    tpch_sqlite_udf_pipeline_test_data.run_e2e_test(
        get_udf_graph, sqlite_tpch_db_context
    )
