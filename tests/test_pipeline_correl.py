"""
Integration tests for the PyDough workflow on edge cases regarding correlation
and decorrelation, using the TPC-H data.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.evaluation.evaluate_unqualified import _load_column_selection
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)
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
from tests.testing_utilities import (
    graph_fetcher,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                correl_1,
                None,
                "correl_1",
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
            ),
            id="correl_1",
        ),
        pytest.param(
            (
                correl_2,
                None,
                "correl_2",
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
            ),
            id="correl_2",
        ),
        pytest.param(
            (
                correl_3,
                None,
                "correl_3",
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
            ),
            id="correl_3",
        ),
        pytest.param(
            (
                correl_4,
                None,
                "correl_4",
                lambda: pd.DataFrame(
                    {
                        "name": ["ARGENTINA", "KENYA", "UNITED KINGDOM"],
                    }
                ),
            ),
            id="correl_4",
        ),
        pytest.param(
            (
                correl_5,
                None,
                "correl_5",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "ASIA", "MIDDLE EAST"],
                    }
                ),
            ),
            id="correl_5",
        ),
        pytest.param(
            (
                correl_6,
                None,
                "correl_6",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA"],
                        "n_prefix_nations": [1, 1],
                    }
                ),
            ),
            id="correl_6",
        ),
        pytest.param(
            (
                correl_7,
                None,
                "correl_7",
                lambda: pd.DataFrame(
                    {
                        "name": ["ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_prefix_nations": [0] * 3,
                    }
                ),
            ),
            id="correl_7",
        ),
        pytest.param(
            (
                correl_8,
                None,
                "correl_8",
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
            ),
            id="correl_8",
        ),
        pytest.param(
            (
                correl_9,
                None,
                "correl_9",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "ALGERIA",
                            "ARGENTINA",
                        ],
                        "rname": ["AFRICA", "AMERICA"],
                    }
                ),
            ),
            id="correl_9",
        ),
        pytest.param(
            (
                correl_10,
                None,
                "correl_10",
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
            ),
            id="correl_10",
        ),
        pytest.param(
            (
                correl_11,
                None,
                "correl_11",
                lambda: pd.DataFrame(
                    {"brand": ["Brand#33", "Brand#43", "Brand#45", "Brand#55"]}
                ),
            ),
            id="correl_11",
        ),
        pytest.param(
            (
                correl_12,
                None,
                "correl_12",
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
            ),
            id="correl_12",
        ),
        pytest.param(
            (
                correl_13,
                None,
                "correl_13",
                lambda: pd.DataFrame({"n": [1129]}),
            ),
            id="correl_13",
        ),
        pytest.param(
            (
                correl_14,
                None,
                "correl_14",
                lambda: pd.DataFrame({"n": [66]}),
            ),
            id="correl_14",
        ),
        pytest.param(
            (
                correl_15,
                None,
                "correl_15",
                lambda: pd.DataFrame({"n": [61]}),
            ),
            id="correl_15",
        ),
        pytest.param(
            (
                correl_16,
                None,
                "correl_16",
                lambda: pd.DataFrame({"n": [929]}),
            ),
            id="correl_16",
        ),
        pytest.param(
            (
                correl_17,
                None,
                "correl_17",
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
            ),
            id="correl_17",
        ),
        pytest.param(
            (
                correl_18,
                None,
                "correl_18",
                lambda: pd.DataFrame({"n": [697]}),
            ),
            id="correl_18",
        ),
        pytest.param(
            (
                correl_19,
                None,
                "correl_19",
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
            ),
            id="correl_19",
        ),
        pytest.param(
            (
                correl_20,
                None,
                "correl_20",
                lambda: pd.DataFrame({"n": [3002]}),
            ),
            id="correl_20",
        ),
        pytest.param(
            (
                correl_21,
                None,
                "correl_21",
                lambda: pd.DataFrame({"n_sizes": [30]}),
            ),
            id="correl_21",
        ),
        pytest.param(
            (
                correl_22,
                None,
                "correl_22",
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
            ),
            id="correl_22",
        ),
        pytest.param(
            (
                correl_23,
                None,
                "correl_23",
                lambda: pd.DataFrame({"n_sizes": [23]}),
            ),
            id="correl_23",
        ),
        pytest.param(
            (
                correl_24,
                None,
                "correl_24",
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
            ),
            id="correl_24",
        ),
        pytest.param(
            (
                correl_25,
                None,
                "correl_25",
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
            ),
            id="correl_25",
        ),
        pytest.param(
            (
                correl_26,
                None,
                "correl_26",
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
            ),
            id="correl_26",
        ),
        pytest.param(
            (
                correl_27,
                None,
                "correl_27",
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
            ),
            id="correl_27",
        ),
        pytest.param(
            (
                correl_28,
                None,
                "correl_28",
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
            ),
            id="correl_28",
        ),
        pytest.param(
            (
                correl_29,
                None,
                "correl_29",
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
            ),
            id="correl_29",
        ),
        pytest.param(
            (
                correl_30,
                None,
                "correl_30",
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
            ),
            id="correl_30",
        ),
        pytest.param(
            (
                correl_31,
                None,
                "correl_31",
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
            ),
            id="correl_31",
        ),
        pytest.param(
            (
                correl_32,
                None,
                "correl_32",
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
            ),
            id="correl_32",
        ),
        pytest.param(
            (
                correl_33,
                None,
                "correl_33",
                lambda: pd.DataFrame({"n": [19330]}),
            ),
            id="correl_33",
        ),
    ],
)
def pydough_pipeline_correl_test_data(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    dict[str, str] | list[str] | None,
    str,
    Callable[[], pd.DataFrame],
]:
    """
    Test data for `test_pipeline_e2e_correlated`. Returns a tuple of the
    following arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `columns`: a valid value for the `columns` argument of `to_sql` or
    `to_df`.
    3. `file_name`: the name of the file containing the expected relational
    plan.
    4. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational_correlated(
    pydough_pipeline_correl_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on the
    various correlation test queries.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, columns, file_name, _ = pydough_pipeline_correl_test_data
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("TPCH")
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph, default_config)
    assert isinstance(qualified, PyDoughCollectionQDAG), (
        "Expected qualified answer to be a collection, not an expression"
    )
    relational: RelationalRoot = convert_ast_to_relational(
        qualified, _load_column_selection({"columns": columns}), default_config
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert relational.to_tree_string() == expected_relational_string.strip(), (
            "Mismatch between tree string representation of relational node and expected Relational tree string"
        )


@pytest.mark.execute
def test_pipeline_e2e_correlated(
    pydough_pipeline_correl_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    unqualified_impl, columns, _, answer_impl = pydough_pipeline_correl_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, columns=columns, metadata=graph, database=sqlite_tpch_db_context
    )
    pd.testing.assert_frame_equal(result, answer_impl())
