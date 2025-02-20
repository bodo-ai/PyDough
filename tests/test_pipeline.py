"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from bad_pydough_functions import (
    bad_slice_1,
    bad_slice_2,
    bad_slice_3,
    bad_slice_4,
)
from simple_pydough_functions import (
    agg_partition,
    double_partition,
    exponentiation,
    function_sampler,
    hour_minute_day,
    minutes_seconds_datediff,
    percentile_customers_per_region,
    percentile_nations,
    rank_nations_by_region,
    rank_nations_per_region_by_customers,
    rank_parts_per_supplier_region_by_size,
    rank_with_filters_a,
    rank_with_filters_b,
    rank_with_filters_c,
    regional_suppliers_percentile,
    simple_filter_top_five,
    simple_scan_top_five,
    step_slicing,
    triple_partition,
    years_months_days_hours_datediff,
)
from test_utils import (
    graph_fetcher,
)
from tpch_outputs import (
    tpch_q1_output,
    tpch_q2_output,
    tpch_q3_output,
    tpch_q4_output,
    tpch_q5_output,
    tpch_q6_output,
    tpch_q7_output,
    tpch_q8_output,
    tpch_q9_output,
    tpch_q10_output,
    tpch_q11_output,
    tpch_q12_output,
    tpch_q13_output,
    tpch_q14_output,
    tpch_q15_output,
    tpch_q16_output,
    tpch_q17_output,
    tpch_q18_output,
    tpch_q19_output,
    tpch_q20_output,
    tpch_q21_output,
    tpch_q22_output,
)
from tpch_test_functions import (
    impl_tpch_q1,
    impl_tpch_q2,
    impl_tpch_q3,
    impl_tpch_q4,
    impl_tpch_q5,
    impl_tpch_q6,
    impl_tpch_q7,
    impl_tpch_q8,
    impl_tpch_q9,
    impl_tpch_q10,
    impl_tpch_q11,
    impl_tpch_q12,
    impl_tpch_q13,
    impl_tpch_q14,
    impl_tpch_q15,
    impl_tpch_q16,
    impl_tpch_q17,
    impl_tpch_q18,
    impl_tpch_q19,
    impl_tpch_q20,
    impl_tpch_q21,
    impl_tpch_q22,
)

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                impl_tpch_q1,
                "tpch_q1",
                tpch_q1_output,
            ),
            id="tpch_q1",
        ),
        pytest.param(
            (
                impl_tpch_q2,
                "tpch_q2",
                tpch_q2_output,
            ),
            id="tpch_q2",
        ),
        pytest.param(
            (
                impl_tpch_q3,
                "tpch_q3",
                tpch_q3_output,
            ),
            id="tpch_q3",
        ),
        pytest.param(
            (
                impl_tpch_q4,
                "tpch_q4",
                tpch_q4_output,
            ),
            id="tpch_q4",
        ),
        pytest.param(
            (
                impl_tpch_q5,
                "tpch_q5",
                tpch_q5_output,
            ),
            id="tpch_q5",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                impl_tpch_q6,
                "tpch_q6",
                tpch_q6_output,
            ),
            id="tpch_q6",
        ),
        pytest.param(
            (
                impl_tpch_q7,
                "tpch_q7",
                tpch_q7_output,
            ),
            id="tpch_q7",
        ),
        pytest.param(
            (
                impl_tpch_q8,
                "tpch_q8",
                tpch_q8_output,
            ),
            id="tpch_q8",
        ),
        pytest.param(
            (
                impl_tpch_q9,
                "tpch_q9",
                tpch_q9_output,
            ),
            id="tpch_q9",
        ),
        pytest.param(
            (
                impl_tpch_q10,
                "tpch_q10",
                tpch_q10_output,
            ),
            id="tpch_q10",
        ),
        pytest.param(
            (
                impl_tpch_q11,
                "tpch_q11",
                tpch_q11_output,
            ),
            id="tpch_q11",
        ),
        pytest.param(
            (
                impl_tpch_q12,
                "tpch_q12",
                tpch_q12_output,
            ),
            id="tpch_q12",
        ),
        pytest.param(
            (
                impl_tpch_q13,
                "tpch_q13",
                tpch_q13_output,
            ),
            id="tpch_q13",
        ),
        pytest.param(
            (
                impl_tpch_q14,
                "tpch_q14",
                tpch_q14_output,
            ),
            id="tpch_q14",
        ),
        pytest.param(
            (
                impl_tpch_q15,
                "tpch_q15",
                tpch_q15_output,
            ),
            id="tpch_q15",
        ),
        pytest.param(
            (
                impl_tpch_q16,
                "tpch_q16",
                tpch_q16_output,
            ),
            id="tpch_q16",
        ),
        pytest.param(
            (
                impl_tpch_q17,
                "tpch_q17",
                tpch_q17_output,
            ),
            id="tpch_q17",
        ),
        pytest.param(
            (
                impl_tpch_q18,
                "tpch_q18",
                tpch_q18_output,
            ),
            id="tpch_q18",
        ),
        pytest.param(
            (
                impl_tpch_q19,
                "tpch_q19",
                tpch_q19_output,
            ),
            id="tpch_q19",
        ),
        pytest.param(
            (
                impl_tpch_q20,
                "tpch_q20",
                tpch_q20_output,
            ),
            id="tpch_q20",
        ),
        pytest.param(
            (
                impl_tpch_q21,
                "tpch_q21",
                tpch_q21_output,
            ),
            id="tpch_q21",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                impl_tpch_q22,
                "tpch_q22",
                tpch_q22_output,
            ),
            id="tpch_q22",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                simple_scan_top_five,
                "simple_scan_top_five",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                    }
                ),
            ),
            id="simple_scan_top_five",
        ),
        pytest.param(
            (
                simple_filter_top_five,
                "simple_filter_top_five",
                lambda: pd.DataFrame(
                    {
                        "key": [5989315, 5935174, 5881093, 5876066, 5866437],
                        "total_price": [947.81, 974.01, 995.6, 967.55, 916.41],
                    }
                ),
            ),
            id="simple_filter_top_five",
        ),
        pytest.param(
            (
                rank_nations_by_region,
                "rank_nations_by_region",
                lambda: pd.DataFrame(
                    {
                        "name": [
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
                            "INDIA",
                            "INDONESIA",
                            "JAPAN",
                            "CHINA",
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
                        "rank": [1] * 5 + [6] * 5 + [11] * 5 + [16] * 5 + [21] * 5,
                    }
                ),
            ),
            id="rank_nations_by_region",
        ),
        pytest.param(
            (
                rank_nations_per_region_by_customers,
                "rank_nations_per_region_by_customers",
                lambda: pd.DataFrame(
                    {
                        "name": ["KENYA", "CANADA", "INDONESIA", "FRANCE", "JORDAN"],
                        "rank": [1] * 5,
                    }
                ),
            ),
            id="rank_nations_per_region_by_customers",
        ),
        pytest.param(
            (
                rank_parts_per_supplier_region_by_size,
                "rank_parts_per_supplier_region_by_size",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4],
                        "region": [
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "ASIA",
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "EUROPE",
                            "AFRICA",
                            "EUROPE",
                            "MIDDLE EAST",
                            "MIDDLE EAST",
                            "AFRICA",
                            "AFRICA",
                            "ASIA",
                        ],
                        "rank": [
                            84220,
                            86395,
                            86395,
                            85307,
                            95711,
                            98092,
                            98092,
                            96476,
                            55909,
                            56227,
                            57062,
                            57062,
                            69954,
                            69954,
                            70899,
                        ],
                    }
                ),
            ),
            id="rank_parts_per_supplier_region_by_size",
        ),
        pytest.param(
            (
                rank_with_filters_a,
                "rank_with_filters_a",
                lambda: pd.DataFrame(
                    {
                        "n": [
                            "Customer#000015980",
                            "Customer#000025320",
                            "Customer#000089900",
                        ],
                        "r": [9, 25, 29],
                    }
                ),
            ),
            id="rank_with_filters_a",
        ),
        pytest.param(
            (
                rank_with_filters_b,
                "rank_with_filters_b",
                lambda: pd.DataFrame(
                    {
                        "n": [
                            "Customer#000015980",
                            "Customer#000025320",
                            "Customer#000089900",
                        ],
                        "r": [9, 25, 29],
                    }
                ),
            ),
            id="rank_with_filters_b",
        ),
        pytest.param(
            (
                rank_with_filters_c,
                "rank_with_filters_c",
                lambda: pd.DataFrame(
                    {
                        "size": [46, 47, 48, 49, 50],
                        "name": [
                            "frosted powder drab burnished grey",
                            "lace khaki orange bisque beige",
                            "steel chartreuse navy ivory brown",
                            "forest azure almond antique violet",
                            "blanched floral red maroon papaya",
                        ],
                    }
                ),
            ),
            id="rank_with_filters_c",
        ),
        pytest.param(
            (
                percentile_nations,
                "percentile_nations",
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
                        "p": [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + [5] * 5,
                    }
                ),
            ),
            id="percentile_nations",
        ),
        pytest.param(
            (
                percentile_customers_per_region,
                "percentile_customers_per_region",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000059661",
                            "Customer#000063999",
                            "Customer#000071528",
                            "Customer#000074375",
                            "Customer#000089686",
                            "Customer#000098778",
                            "Customer#000100935",
                            "Customer#000102081",
                            "Customer#000110285",
                            "Customer#000136477",
                        ],
                    }
                ),
            ),
            id="percentile_customers_per_region",
        ),
        pytest.param(
            (
                regional_suppliers_percentile,
                "regional_suppliers_percentile",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Supplier#000009997",
                            "Supplier#000009978",
                            "Supplier#000009998",
                            "Supplier#000009995",
                            "Supplier#000009999",
                            "Supplier#000010000",
                            "Supplier#000009991",
                            "Supplier#000009996",
                        ]
                    }
                ),
            ),
            id="regional_suppliers_percentile",
        ),
        pytest.param(
            (
                function_sampler,
                "function_sampler",
                lambda: pd.DataFrame(
                    {
                        "a": [
                            "ASIA-INDIA-74",
                            "AMERICA-CANADA-51",
                            "EUROPE-GERMANY-40",
                            "AMERICA-ARGENTINA-60",
                            "AMERICA-UNITED STATES-76",
                            "MIDDLE EAST-IRAN-80",
                            "MIDDLE EAST-IRAQ-12",
                            "AMERICA-ARGENTINA-69",
                            "AFRICA-MOROCCO-48",
                            "EUROPE-UNITED KINGDOM-17",
                        ],
                        "b": [
                            15.6,
                            61.5,
                            39.2,
                            27.5,
                            35.1,
                            56.4,
                            40.2,
                            38.0,
                            58.4,
                            70.4,
                        ],
                        "c": [None] * 4
                        + ["Customer#000122476"]
                        + [None] * 4
                        + ["Customer#000057817"],
                        "d": [0, 0, 0, 1, 0, 0, 1, 1, 0, 0],
                        "e": [1] * 9 + [0],
                    }
                ),
            ),
            id="function_sampler",
        ),
        pytest.param(
            (
                agg_partition,
                "agg_partition",
                lambda: pd.DataFrame(
                    {
                        "best_year": [228637],
                    }
                ),
            ),
            id="agg_partition",
        ),
        pytest.param(
            (
                double_partition,
                "double_partition",
                lambda: pd.DataFrame(
                    {
                        "year": [1992, 1993, 1994, 1995, 1996, 1997, 1998],
                        "best_month": [19439, 19319, 19546, 19502, 19724, 19519, 19462],
                    }
                ),
            ),
            id="double_partition",
        ),
        pytest.param(
            (
                triple_partition,
                "triple_partition",
                lambda: pd.DataFrame(
                    {
                        "supp_region": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "avg_percentage": [
                            1.8038152,
                            1.9968418,
                            1.6850716,
                            1.7673618,
                            1.7373118,
                        ],
                    }
                ),
            ),
            id="triple_partition",
        ),
    ],
)
def pydough_pipeline_test_data(
    request,
) -> tuple[
    Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
]:
    """
    Test data for test_pydough_pipeline. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `file_name`: the name of the file containing the expected relational
    plan.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational(
    pydough_pipeline_test_data: tuple[
        Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, file_name, _ = pydough_pipeline_test_data
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("TPCH")
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    relational: RelationalRoot = convert_ast_to_relational(qualified, default_config)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert (
            relational.to_tree_string() == expected_relational_string.strip()
        ), "Mismatch between tree string representation of relational node and expected Relational tree string"


@pytest.mark.execute
def test_pipeline_e2e(
    pydough_pipeline_test_data: tuple[
        Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
    ],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    unqualified_impl, _, answer_impl = pydough_pipeline_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_tpch_db_context)
    pd.testing.assert_frame_equal(result, answer_impl())


@pytest.mark.execute
@pytest.mark.parametrize(
    "impl, error_msg",
    [
        pytest.param(
            bad_slice_1,
            "SLICE function currently only supports non-negative stop indices",
            id="bad_slice_1",
        ),
        pytest.param(
            bad_slice_2,
            "SLICE function currently only supports non-negative start indices",
            id="bad_slice_2",
        ),
        pytest.param(
            bad_slice_3,
            "SLICE function currently only supports a step of 1",
            id="bad_slice_3",
        ),
        pytest.param(
            bad_slice_4,
            "SLICE function currently only supports a step of 1",
            id="bad_slice_4",
        ),
    ],
)
def test_pipeline_e2e_errors(
    impl: Callable[[UnqualifiedRoot], UnqualifiedNode],
    error_msg: str,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    with pytest.raises(Exception, match=error_msg):
        root: UnqualifiedNode = init_pydough_context(graph)(impl)()
        to_df(root, metadata=graph, database=sqlite_tpch_db_context)


@pytest.fixture(
    params=[
        pytest.param(
            (
                hour_minute_day,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            "TX001",
                            "TX005",
                            "TX011",
                            "TX015",
                            "TX021",
                            "TX025",
                            "TX031",
                            "TX033",
                            "TX035",
                            "TX044",
                            "TX045",
                            "TX049",
                            "TX051",
                            "TX055",
                        ],
                        "_expr0": [9, 12, 9, 12, 9, 12, 0, 0, 0, 10, 10, 16, 0, 0],
                        "_expr1": [30, 30, 30, 30, 30, 30, 0, 0, 0, 0, 30, 0, 0, 0],
                        "_expr2": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    }
                ),
            ),
            id="broker_basic1",
        ),
        pytest.param(
            (
                exponentiation,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "low_square": [
                            6642.2500,
                            6740.4100,
                            6839.2900,
                            6938.8900,
                            7039.2100,
                            7140.2500,
                            7242.0100,
                            16576.5625,
                            16900.0000,
                            17292.2500,
                        ],
                        "low_sqrt": [
                            9.027735,
                            9.060905,
                            9.093954,
                            9.126883,
                            9.159694,
                            9.192388,
                            9.224966,
                            11.346806,
                            11.401754,
                            11.467345,
                        ],
                        "low_cbrt": [
                            4.335633,
                            4.346247,
                            4.356809,
                            4.367320,
                            4.377781,
                            4.388191,
                            4.398553,
                            5.049508,
                            5.065797,
                            5.085206,
                        ],
                    }
                ),
            ),
            id="exponentiation",
        ),
        pytest.param(
            (
                years_months_days_hours_datediff,
                "Broker",
                lambda: pd.DataFrame(
                    data={
                        "x": [
                            "2023-04-01 09:30:00",
                            "2023-04-01 10:15:00",
                            "2023-04-01 11:00:00",
                            "2023-04-01 11:45:00",
                            "2023-04-01 12:30:00",
                            "2023-04-01 13:15:00",
                            "2023-04-01 14:00:00",
                            "2023-04-01 14:45:00",
                            "2023-04-01 15:30:00",
                            "2023-04-01 16:15:00",
                            "2023-04-02 09:30:00",
                            "2023-04-02 10:15:00",
                            "2023-04-02 11:00:00",
                            "2023-04-02 11:45:00",
                            "2023-04-02 12:30:00",
                            "2023-04-02 13:15:00",
                            "2023-04-02 14:00:00",
                            "2023-04-02 14:45:00",
                            "2023-04-02 15:30:00",
                            "2023-04-02 16:15:00",
                            "2023-04-03 09:30:00",
                            "2023-04-03 10:15:00",
                            "2023-04-03 11:00:00",
                            "2023-04-03 11:45:00",
                            "2023-04-03 12:30:00",
                            "2023-04-03 13:15:00",
                            "2023-04-03 14:00:00",
                            "2023-04-03 14:45:00",
                            "2023-04-03 15:30:00",
                            "2023-04-03 16:15:00",
                        ],
                        "y1": ["2025-05-02 11:00:00"] * 30,
                        "years_diff": [2] * 30,
                        "c_years_diff": [2] * 30,
                        "c_y_diff": [2] * 30,
                        "y_diff": [2] * 30,
                        "months_diff": [25] * 30,
                        "c_months_diff": [25] * 30,
                        "mm_diff": [25] * 30,
                        "days_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "c_days_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "c_d_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "d_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "hours_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                        "c_hours_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                        "c_h_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                    }
                ),
            ),
            id="years_months_days_hours_datediff",
        ),
        pytest.param(
            (
                minutes_seconds_datediff,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "x": [
                            "2023-04-03 16:15:00",
                            "2023-04-03 15:30:00",
                            "2023-04-03 14:45:00",
                            "2023-04-03 14:00:00",
                            "2023-04-03 13:15:00",
                            "2023-04-03 12:30:00",
                            "2023-04-03 11:45:00",
                            "2023-04-03 11:00:00",
                            "2023-04-03 10:15:00",
                            "2023-04-03 09:30:00",
                            "2023-04-02 16:15:00",
                            "2023-04-02 15:30:00",
                            "2023-04-02 14:45:00",
                            "2023-04-02 14:00:00",
                            "2023-04-02 13:15:00",
                            "2023-04-02 12:30:00",
                            "2023-04-02 11:45:00",
                            "2023-04-02 11:00:00",
                            "2023-04-02 10:15:00",
                            "2023-04-02 09:30:00",
                            "2023-04-01 16:15:00",
                            "2023-04-01 15:30:00",
                            "2023-04-01 14:45:00",
                            "2023-04-01 14:00:00",
                            "2023-04-01 13:15:00",
                            "2023-04-01 12:30:00",
                            "2023-04-01 11:45:00",
                            "2023-04-01 11:00:00",
                            "2023-04-01 10:15:00",
                            "2023-04-01 09:30:00",
                        ],
                        "y": ["2023-04-03 13:16:30"] * 30,
                        "minutes_diff": [
                            -179,
                            -134,
                            -89,
                            -44,
                            1,
                            46,
                            91,
                            136,
                            181,
                            226,
                            1261,
                            1306,
                            1351,
                            1396,
                            1441,
                            1486,
                            1531,
                            1576,
                            1621,
                            1666,
                            2701,
                            2746,
                            2791,
                            2836,
                            2881,
                            2926,
                            2971,
                            3016,
                            3061,
                            3106,
                        ],
                        "seconds_diff": [
                            -10710,
                            -8010,
                            -5310,
                            -2610,
                            90,
                            2790,
                            5490,
                            8190,
                            10890,
                            13590,
                            75690,
                            78390,
                            81090,
                            83790,
                            86490,
                            89190,
                            91890,
                            94590,
                            97290,
                            99990,
                            162090,
                            164790,
                            167490,
                            170190,
                            172890,
                            175590,
                            178290,
                            180990,
                            183690,
                            186390,
                        ],
                    }
                ),
            ),
            id="minutes_seconds_datediff",
        ),
        pytest.param(
            (
                step_slicing,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "john doe",
                            "Jane Smith",
                            "Bob Johnson",
                            "Samantha Lee",
                            "Michael Chen",
                            "Emily Davis",
                            "David Kim",
                            "Sarah Nguyen",
                            "William Garcia",
                            "Jessica Hernandez",
                            "Alex Rodriguez",
                            "Olivia Johnson",
                            "Ethan Davis",
                            "Ava Wilson",
                            "Emma Brown",
                            "sophia martinez",
                            "Jacob Taylor",
                            "Michael Anderson",
                            "Isabella Thompson",
                            "Maurice Lee",
                        ]
                    }
                ).assign(
                    neg_none_step=lambda x: x["name"].str[-2::1],
                    pos_none_step=lambda x: x["name"].str[3::1],
                    none_pos_step=lambda x: x["name"].str[:3:1],
                    none_neg_step=lambda x: x["name"].str[:-2:1],
                    pos_pos_step=lambda x: x["name"].str[2:4:1],
                    pos_neg_step=lambda x: x["name"].str[2:-2:1],
                    neg_pos_step=lambda x: x["name"].str[-4:2:1],
                    neg_neg_step=lambda x: x["name"].str[-4:-2:1],
                    empty1=lambda x: x["name"].str[2:2:1],
                    empty2=lambda x: x["name"].str[-2:-2:1],
                    empty3=lambda x: x["name"].str[-2:-4:1],
                    empty4=lambda x: x["name"].str[4:2:1],
                    oob1=lambda x: x["name"].str[100:200:1],
                    oob2=lambda x: x["name"].str[-200:-100:1],
                    wo_step1=lambda x: x["name"].str[-2:],
                    wo_step2=lambda x: x["name"].str[3:],
                    wo_step3=lambda x: x["name"].str[:3],
                    wo_step4=lambda x: x["name"].str[:-2],
                    wo_step5=lambda x: x["name"].str[2:4],
                    wo_step6=lambda x: x["name"].str[2:-2],
                    wo_step7=lambda x: x["name"].str[-4:2],
                    wo_step8=lambda x: x["name"].str[-4:-2],
                    wo_step9=lambda x: x["name"].str[2:2],
                ),
            ),
            id="step_slicing",
        ),
    ],
)
def custom_defog_test_data(
    request,
) -> tuple[Callable[[], UnqualifiedNode], str, pd.DataFrame]:
    """
    Test data for test_defog_e2e. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a PyDough implementation function.
    2. `graph_name`: the name of the graph from the defog database to use.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a defog query as a Pandas DataFrame.
    """
    return request.param


@pytest.mark.execute
def test_defog_e2e_with_custom_data(
    custom_defog_test_data: tuple[Callable[[], UnqualifiedNode], str, pd.DataFrame],
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector.
    """
    unqualified_impl, graph_name, answer_impl = custom_defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_defog_connection)
    breakpoint()
    pd.testing.assert_frame_equal(result, answer_impl())
