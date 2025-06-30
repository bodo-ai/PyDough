"""
Integration tests for the PyDough workflow with custom questions on the TPC-H
dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.udf_pydough_functions import (
    sqlite_udf_combine_strings,
    sqlite_udf_covar_pop,
    sqlite_udf_cumulative_distribution,
    sqlite_udf_decode3,
    sqlite_udf_format_datetime,
    sqlite_udf_gcat,
    sqlite_udf_nval,
    sqlite_udf_percent_epsilon,
    sqlite_udf_percent_positive,
    sqlite_udf_relmin,
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_format_datetime,
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
                "sqlite_udf_format_datetime",
            ),
            id="sqlite_udf_format_datetime",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_combine_strings,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "s1": ["AFRICA,AMERICA,ASIA,EUROPE,MIDDLE EAST"],
                        "s2": ["AFRICA, AMERICA, ASIA, MIDDLE EAST"],
                        "s3": ["AABCEEFGIIIIJJKMMPCRSVRUU"],
                        "s4": ["NOT SPECIFIED <=> MEDIUM <=> URGENT <=> LOW <=> HIGH"],
                    }
                ),
                "sqlite_udf_combine_strings",
            ),
            id="sqlite_udf_combine_strings",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_percent_positive,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "pct_cust_positive": [90.8, 90.81, 90.96, 90.97, 90.82],
                        "pct_supp_positive": [90.49, 91.94, 90.46, 91.04, 91.73],
                    }
                ),
                "sqlite_udf_percent_positive",
            ),
            id="sqlite_udf_percent_positive",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_percent_epsilon,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "pct_e1": [0.0004],
                        "pct_e10": [0.007],
                        "pct_e100": [0.0696],
                        "pct_e1000": [0.7385],
                        "pct_e10000": [7.3967],
                    }
                ),
                "sqlite_udf_percent_epsilon",
            ),
            id="sqlite_udf_percent_epsilon",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_covar_pop,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "cvp_ab_otp": [0.388, -10.511, 1.734, 5.069, -14.71],
                    }
                ),
                "sqlite_udf_covar_pop",
            ),
            id="sqlite_udf_covar_pop",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_nval,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "rname": ["AFRICA"] * 5
                        + ["AMERICA"] * 5
                        + ["ASIA"] * 5
                        + ["EUROPE"] * 5
                        + ["MIDDLE EAST"] * 5,
                        "nname": [
                            "ALGERIA",
                            "ETHIOPIA",
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                            "CHINA",
                            "INDIA",
                            "INDONESIA",
                            "JAPAN",
                            "VIETNAM",
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                            "EGYPT",
                            "IRAN",
                            "IRAQ",
                            "JORDAN",
                            "SAUDI ARABIA",
                        ],
                        "v1": ["BRAZIL"] * 25,
                        "v2": ["ALGERIA"] * 5
                        + ["ARGENTINA"] * 5
                        + ["CHINA"] * 5
                        + ["FRANCE"] * 5
                        + ["EGYPT"] * 5,
                        "v3": [
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            None,
                            None,
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                            None,
                            None,
                            "INDONESIA",
                            "JAPAN",
                            "VIETNAM",
                            None,
                            None,
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                            None,
                            None,
                            "IRAQ",
                            "JORDAN",
                            "SAUDI ARABIA",
                            None,
                            None,
                        ],
                        "v4": [None] + ["CHINA"] * 4 + [None] * 3 + ["CHINA"] * 17,
                    }
                ),
                "sqlite_udf_nval",
            ),
            id="sqlite_udf_nval",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_gcat,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "c1": ["AFRICA-AMERICA-ASIA-EUROPE-MIDDLE EAST"] * 5,
                        "c2": ["MIDDLE EAST-EUROPE-ASIA-AMERICA-AFRICA"] * 5,
                        "c3": [
                            "AFRICA",
                            "AFRICA-AMERICA",
                            "AFRICA-AMERICA-ASIA",
                            "AFRICA-AMERICA-ASIA-EUROPE",
                            "AFRICA-AMERICA-ASIA-EUROPE-MIDDLE EAST",
                        ],
                    }
                ),
                "sqlite_udf_gcat",
            ),
            id="sqlite_udf_gcat",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_relmin,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "month": list(range(1, 13)),
                        "n_orders": [
                            3922,
                            3585,
                            3946,
                            3723,
                            3900,
                            3807,
                            3820,
                            3950,
                            3771,
                            3758,
                            3804,
                            3891,
                        ],
                        "m1": [3585] * 12,
                        "m2": [3922] + [3585] * 11,
                        "m3": [
                            3585,
                            3585,
                            3585,
                            3723,
                            3723,
                            3807,
                            3807,
                            3771,
                            3758,
                            3758,
                            3758,
                            3804,
                        ],
                    }
                ),
                "sqlite_udf_relmin",
            ),
            id="sqlite_udf_relmin",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_cumulative_distribution,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "c": [0.1489, 0.5508, 0.7007, 0.8491, 1.0],
                        "n": [26652, 71958, 26843, 26582, 27010],
                    }
                ),
                "sqlite_udf_cumulative_distribution",
            ),
            id="sqlite_udf_cumulative_distribution",
        ),
        pytest.param(
            PyDoughPandasTest(
                sqlite_udf_decode3,
                "TPCH_SQLITE_UDFS",
                lambda: pd.DataFrame(
                    {
                        "k": [
                            1,
                            839,
                            2338,
                            4579,
                            8452,
                            9185,
                            12163,
                            13508,
                            14277,
                            15073,
                        ],
                        "n": ["D", "A", "B", "B", "D", "B", "D", "D", "D", "C"],
                    }
                ),
                "sqlite_udf_decode3",
            ),
            id="sqlite_udf_decode3",
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
