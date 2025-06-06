"""
Integration tests for the PyDough workflow on edge cases regarding correlation
and decorrelation, using the TPC-H data.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.correlated_pydough_functions import (
    correl_1,
    correl_2,
    correl_3,
    correl_4,
    correl_5,
    correl_6,
    correl_7,
    correl_8,
    correl_9,
    correl_10,
    correl_11,
    correl_12,
    correl_13,
    correl_14,
    correl_15,
    correl_16,
    correl_17,
    correl_18,
    correl_19,
    correl_20,
    correl_21,
    correl_22,
    correl_23,
    correl_24,
    correl_25,
    correl_26,
    correl_27,
    correl_28,
    correl_29,
    correl_30,
    correl_31,
    correl_32,
    correl_33,
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                correl_1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "n_prefix_nations": [1, 1, 0, 0, 0],
                    }
                ),
                "correl_1",
            ),
            id="correl_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "EGYPT",
                            "FRANCE",
                            "GERMANY",
                            "IRAN",
                            "IRAQ",
                            "JORDAN",
                            "ROMANIA",
                            "RUSSIA",
                            "SAUDI ARABIA",
                            "UNITED KINGDOM",
                        ],
                        "n_selected_custs": [
                            19,
                            593,
                            595,
                            15,
                            21,
                            9,
                            588,
                            620,
                            19,
                            585,
                        ],
                    }
                ),
                "correl_2",
            ),
            id="correl_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "n_nations": [5, 5, 5, 0, 2],
                    }
                ),
                "correl_3",
            ),
            id="correl_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_4,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["ARGENTINA", "KENYA", "UNITED KINGDOM"],
                    }
                ),
                "correl_4",
            ),
            id="correl_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_5,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "ASIA", "MIDDLE EAST"],
                    }
                ),
                "correl_5",
            ),
            id="correl_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_6,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA"],
                        "n_prefix_nations": [1, 1],
                    }
                ),
                "correl_6",
            ),
            id="correl_6",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_7,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_prefix_nations": [0] * 3,
                    }
                ),
                "correl_7",
            ),
            id="correl_7",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_8,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "ALGERIA",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "CHINA",
                            "EGYPT",
                            "ETHIOPIA",
                            "FRANCE",
                            "GERMANY",
                            "INDIA",
                            "INDONESIA",
                            "IRAN",
                            "IRAQ",
                            "JAPAN",
                            "JORDAN",
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "PERU",
                            "ROMANIA",
                            "RUSSIA",
                            "SAUDI ARABIA",
                            "UNITED KINGDOM",
                            "UNITED STATES",
                            "VIETNAM",
                        ],
                        "rname": ["AFRICA", "AMERICA"] + [None] * 23,
                    }
                ),
                "correl_8",
            ),
            id="correl_8",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_9,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "ALGERIA",
                            "ARGENTINA",
                        ],
                        "rname": ["AFRICA", "AMERICA"],
                    }
                ),
                "correl_9",
            ),
            id="correl_9",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_10,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "BRAZIL",
                            "CANADA",
                            "CHINA",
                            "EGYPT",
                            "ETHIOPIA",
                            "FRANCE",
                            "GERMANY",
                            "INDIA",
                            "INDONESIA",
                            "IRAN",
                            "IRAQ",
                            "JAPAN",
                            "JORDAN",
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "PERU",
                            "ROMANIA",
                            "RUSSIA",
                            "SAUDI ARABIA",
                            "UNITED KINGDOM",
                            "UNITED STATES",
                            "VIETNAM",
                        ],
                        "rname": [None] * 23,
                    }
                ),
                "correl_10",
            ),
            id="correl_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_11,
                "TPCH",
                lambda: pd.DataFrame(
                    {"brand": ["Brand#33", "Brand#43", "Brand#45", "Brand#55"]}
                ),
                "correl_11",
            ),
            id="correl_11",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_12,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "brand": [
                            "Brand#14",
                            "Brand#31",
                            "Brand#33",
                            "Brand#43",
                            "Brand#55",
                        ]
                    }
                ),
                "correl_12",
            ),
            id="correl_12",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_13,
                "TPCH",
                lambda: pd.DataFrame({"n": [1129]}),
                "correl_13",
            ),
            id="correl_13",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_14,
                "TPCH",
                lambda: pd.DataFrame({"n": [9]}),
                "correl_14",
            ),
            id="correl_14",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_15,
                "TPCH",
                lambda: pd.DataFrame({"n": [7]}),
                "correl_15",
            ),
            id="correl_15",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_16,
                "TPCH",
                lambda: pd.DataFrame({"n": [929]}),
                "correl_16",
            ),
            id="correl_16",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_17,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "fullname": [
                            "africa-algeria",
                            "africa-ethiopia",
                            "africa-kenya",
                            "africa-morocco",
                            "africa-mozambique",
                            "america-argentina",
                            "america-brazil",
                            "america-canada",
                            "america-peru",
                            "america-united states",
                            "asia-china",
                            "asia-india",
                            "asia-indonesia",
                            "asia-japan",
                            "asia-vietnam",
                            "europe-france",
                            "europe-germany",
                            "europe-romania",
                            "europe-russia",
                            "europe-united kingdom",
                            "middle east-egypt",
                            "middle east-iran",
                            "middle east-iraq",
                            "middle east-jordan",
                            "middle east-saudi arabia",
                        ]
                    }
                ),
                "correl_17",
            ),
            id="correl_17",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_18,
                "TPCH",
                lambda: pd.DataFrame({"n": [697]}),
                "correl_18",
            ),
            id="correl_18",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_19,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000003934",
                            "Supplier#000003887",
                            "Supplier#000002628",
                            "Supplier#000008722",
                            "Supplier#000007971",
                        ],
                        "n_super_cust": [6160, 6142, 6129, 6127, 6117],
                    }
                ),
                "correl_19",
            ),
            id="correl_19",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_20,
                "TPCH",
                lambda: pd.DataFrame({"n": [3002]}),
                "correl_20",
            ),
            id="correl_20",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_21,
                "TPCH",
                lambda: pd.DataFrame({"n_sizes": [30]}),
                "correl_21",
            ),
            id="correl_21",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_22,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "container": [
                            "JUMBO DRUM",
                            "JUMBO PKG",
                            "MED DRUM",
                            "SM BAG",
                            "LG PKG",
                        ],
                        "n_types": [89, 86, 81, 81, 80],
                    }
                ),
                "correl_22",
            ),
            id="correl_22",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_23,
                "TPCH",
                lambda: pd.DataFrame({"n_sizes": [23]}),
                "correl_23",
            ),
            id="correl_23",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_24,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "year": [1992] * 11 + [1993] * 11,
                        "month": list(range(2, 13)) + [1, 2, 3] + list(range(5, 13)),
                        "n_orders_in_range": [
                            45,
                            7,
                            28,
                            36,
                            46,
                            43,
                            64,
                            83,
                            136,
                            26,
                            40,
                            2,
                            47,
                            14,
                            22,
                            27,
                            79,
                            104,
                            97,
                            43,
                            39,
                            103,
                        ],
                    }
                ),
                "correl_24",
            ),
            id="correl_24",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_25,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_region_name": [
                            "MIDDLE EAST",
                            "AMERICA",
                            "MIDDLE EAST",
                            "MIDDLE EAST",
                            "MIDDLE EAST",
                        ],
                        "cust_region_key": [4, 1, 4, 4, 4],
                        "cust_nation_name": [
                            "SAUDI ARABIA",
                            "CANADA",
                            "SAUDI ARABIA",
                            "JORDAN",
                            "SAUDI ARABIA",
                        ],
                        "cust_nation_key": [20, 3, 20, 13, 20],
                        "customer_name": [
                            f"Customer#{n:09}" for n in [361, 385, 956, 4703, 5248]
                        ],
                        "n_urgent_semi_domestic_rail_orders": [2, 2, 2, 2, 2],
                    }
                ),
                "correl_25",
            ),
            id="correl_25",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_26,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "n_selected_purchases": [310, 271, 291, 306, 282],
                    }
                ),
                "correl_26",
            ),
            id="correl_26",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_27,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "n_selected_purchases": [310, 271, 291, 306, 282],
                    }
                ),
                "correl_27",
            ),
            id="correl_27",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_28,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "n_selected_purchases": [310, 271, 291, 306, 282],
                    }
                ),
                "correl_28",
            ),
            id="correl_28",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_29,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_key": [1] * 5 + [3] * 5,
                        "nation_name": [
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "n_above_avg_customers": [
                            2957,
                            2962,
                            2978,
                            2968,
                            2999,
                            3028,
                            2924,
                            3074,
                            3066,
                            3007,
                        ],
                        "n_above_avg_suppliers": [
                            202,
                            192,
                            216,
                            219,
                            200,
                            204,
                            206,
                            206,
                            201,
                            197,
                        ],
                        "min_cust_acctbal": [
                            -993.95,
                            -999.26,
                            -999.65,
                            -999.16,
                            -999.85,
                            -999.99,
                            -995.92,
                            -997.97,
                            -999.42,
                            -994.91,
                        ],
                        "max_cust_acctbal": [
                            9994.84,
                            9999.49,
                            9998.32,
                            9999.23,
                            9999.72,
                            9998.86,
                            9999.74,
                            9999.47,
                            9998.09,
                            9998.51,
                        ],
                    }
                ),
                "correl_29",
            ),
            id="correl_29",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_30,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": ["america"] * 5 + ["europe"] * 5,
                        "nation_name": [
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "n_above_avg_customers": [
                            2957,
                            2962,
                            2978,
                            2968,
                            2999,
                            3028,
                            2924,
                            3074,
                            3066,
                            3007,
                        ],
                        "n_above_avg_suppliers": [
                            202,
                            192,
                            216,
                            219,
                            200,
                            204,
                            206,
                            206,
                            201,
                            197,
                        ],
                    }
                ),
                "correl_30",
            ),
            id="correl_30",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_31,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "UNITED KINGDOM",
                        ],
                        "mean_rev": [
                            49658.994,
                            24775.3189,
                            2352.0536,
                            80866.6208,
                            22315.0414,
                        ],
                        "median_rev": [
                            49658.994,
                            32136.624,
                            2352.0536,
                            80866.6208,
                            23984.22,
                        ],
                    }
                ),
                "correl_31",
            ),
            id="correl_31",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_32,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "customer_name": [
                            "Customer#000109717",
                            "Customer#000076752",
                            "Customer#000011666",
                            "Customer#000137694",
                            "Customer#000136251",
                        ],
                        "delta": [2.075, 4.1, 4.37, 4.435, 5.025],
                    }
                ),
                "correl_32",
            ),
            id="correl_32",
        ),
        pytest.param(
            PyDoughPandasTest(
                correl_33,
                "TPCH",
                lambda: pd.DataFrame({"n": [19330]}),
                "correl_33",
            ),
            id="correl_33",
        ),
    ],
)
def correl_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the correlation test queries run on TPC-H data.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param


def test_pipeline_until_relational_correlated(
    correl_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on the
    various correlation test queries.
    """
    file_path: str = get_plan_test_filename(correl_pipeline_test_data.test_name)
    correl_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


@pytest.mark.execute
def test_pipeline_e2e_correlated(
    correl_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    correl_pipeline_test_data.run_e2e_test(get_sample_graph, sqlite_tpch_db_context)
