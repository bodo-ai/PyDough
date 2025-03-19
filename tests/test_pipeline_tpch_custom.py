"""
Integration tests for the PyDough workflow with custom questions on the TPC-H
dataset.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from bad_pydough_functions import (
    bad_slice_1,
    bad_slice_2,
    bad_slice_3,
    bad_slice_4,
    bad_slice_5,
    bad_slice_6,
    bad_slice_7,
    bad_slice_8,
    bad_slice_9,
    bad_slice_10,
    bad_slice_11,
    bad_slice_12,
    bad_slice_13,
    bad_slice_14,
)
from simple_pydough_functions import (
    agg_partition,
    avg_gap_prev_urgent_same_clerk,
    avg_order_diff_per_customer,
    customer_largest_order_deltas,
    datetime_current,
    datetime_relative,
    double_partition,
    first_order_in_year,
    first_order_per_customer,
    function_sampler,
    month_year_sliding_windows,
    order_info_per_priority,
    percentile_customers_per_region,
    percentile_nations,
    prev_next_regions,
    rank_nations_by_region,
    rank_nations_per_region_by_customers,
    rank_parts_per_supplier_region_by_size,
    rank_with_filters_a,
    rank_with_filters_b,
    rank_with_filters_c,
    regional_suppliers_percentile,
    simple_filter_top_five,
    simple_scan,
    simple_scan_top_five,
    singular1,
    singular2,
    singular3,
    singular4,
    singular5,
    singular6,
    singular7,
    suppliers_bal_diffs,
    top_customers_by_orders,
    triple_partition,
    year_month_nation_orders,
    yoy_change_in_num_orders,
)
from test_utils import (
    graph_fetcher,
)

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


@pytest.fixture(
    params=[
        pytest.param(
            (
                simple_scan_top_five,
                None,
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
                ["key"],
                "simple_filter_top_five",
                lambda: pd.DataFrame(
                    {
                        "key": [5989315, 5935174, 5881093, 5876066, 5866437],
                    }
                ),
            ),
            id="simple_filter_top_five",
        ),
        pytest.param(
            (
                rank_nations_by_region,
                None,
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
                None,
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
                None,
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
                None,
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
                None,
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
                {"pname": "name", "psize": "size"},
                "rank_with_filters_c",
                lambda: pd.DataFrame(
                    {
                        "pname": [
                            "frosted powder drab burnished grey",
                            "lace khaki orange bisque beige",
                            "steel chartreuse navy ivory brown",
                            "forest azure almond antique violet",
                            "blanched floral red maroon papaya",
                        ],
                        "psize": [46, 47, 48, 49, 50],
                    }
                ),
            ),
            id="rank_with_filters_c",
        ),
        pytest.param(
            (
                percentile_nations,
                {"name": "name", "p1": "p", "p2": "p"},
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
                        "p1": [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + [5] * 5,
                        "p2": [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + [5] * 5,
                    }
                ),
            ),
            id="percentile_nations",
        ),
        pytest.param(
            (
                percentile_customers_per_region,
                None,
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
                ["name"],
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
                None,
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
                        "f": [
                            16.0,
                            61.0,
                            39.0,
                            28.0,
                            35.0,
                            56.0,
                            40.0,
                            38.0,
                            58.0,
                            70.0,
                        ],
                    }
                ),
            ),
            id="function_sampler",
        ),
        pytest.param(
            (
                order_info_per_priority,
                None,
                "order_info_per_priority",
                lambda: pd.DataFrame(
                    {
                        "order_priority": [
                            "1-URGENT",
                            "2-HIGH",
                            "3-MEDIUM",
                            "4-NOT SPECIFIED",
                            "5-LOW",
                        ],
                        "order_key": [3586919, 1474818, 972901, 1750466, 631651],
                        "order_total_price": [
                            522644.48,
                            491348.26,
                            508668.52,
                            555285.16,
                            504509.06,
                        ],
                    }
                ),
            ),
            id="order_info_per_priority",
        ),
        pytest.param(
            (
                year_month_nation_orders,
                None,
                "year_month_nation_orders",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "MOZAMBIQUE",
                            "MOZAMBIQUE",
                            "CHINA",
                            "ALGERIA",
                            "INDONESIA",
                        ],
                        "order_year": [1992, 1997, 1993, 1996, 1996],
                        "order_month": [10, 7, 8, 4, 5],
                        "n_orders": [198, 194, 188, 186, 185],
                    }
                ),
            ),
            id="year_month_nation_orders",
        ),
        pytest.param(
            (
                datetime_current,
                None,
                "datetime_current",
                lambda: pd.DataFrame(
                    {
                        "d1": [f"{pd.Timestamp.now(tz='UTC').year}-05-31"],
                        "d2": [
                            f"{pd.Timestamp.now(tz='UTC').year}-{pd.Timestamp.now(tz='UTC').month:02}-02 00:00:00"
                        ],
                        "d3": [
                            (
                                pd.Timestamp.now(tz="UTC").normalize()
                                + pd.Timedelta(hours=12, minutes=-150, seconds=2)
                            ).strftime("%Y-%m-%d %H:%M:%S")
                        ],
                    },
                ),
            ),
            id="datetime_current",
        ),
        pytest.param(
            (
                datetime_relative,
                None,
                "datetime_relative",
                lambda: pd.DataFrame(
                    {
                        "d1": [
                            f"{y}-01-01"
                            for y in [1992] * 3 + [1994] * 3 + [1996] * 3 + [1997]
                        ],
                        "d2": [
                            "1992-04-01",
                            "1992-04-01",
                            "1992-08-01",
                            "1994-05-01",
                            "1994-08-01",
                            "1994-12-01",
                            "1996-06-01",
                            "1996-07-01",
                            "1996-12-01",
                            "1997-03-01",
                        ],
                        "d3": [
                            "1981-12-29 04:57:01",
                            "1982-01-12 04:57:01",
                            "1982-05-15 04:57:01",
                            "1984-02-14 04:57:01",
                            "1984-05-21 04:57:01",
                            "1984-09-17 04:57:01",
                            "1986-03-22 04:57:01",
                            "1986-03-25 04:57:01",
                            "1986-09-02 04:57:01",
                            "1986-12-16 04:57:01",
                        ],
                        "d4": ["2025-07-04 12:00:00"] * 10,
                        "d5": ["2025-07-04 12:58:00"] * 10,
                        "d6": ["2025-07-26 02:45:25"] * 10,
                    },
                ),
            ),
            id="datetime_relative_tpch",
        ),
        pytest.param(
            (
                agg_partition,
                None,
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
                None,
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
                {"region": "supp_region", "avgpct": "avg_percentage"},
                "triple_partition",
                lambda: pd.DataFrame(
                    {
                        "region": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "avgpct": [
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
        pytest.param(
            (
                first_order_per_customer,
                None,
                "first_order_per_customer",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000097444",
                            "Customer#000092695",
                            "Customer#000142948",
                            "Customer#000095797",
                            "Customer#000050726",
                        ],
                        "first_order_date": [
                            "1992-03-01",
                            "1992-09-10",
                            "1992-09-07",
                            "1992-06-18",
                            "1992-11-01",
                        ],
                        "first_order_price": [
                            454639.91,
                            448940.71,
                            447699.76,
                            446979.77,
                            443394.94,
                        ],
                    }
                ),
            ),
            id="first_order_per_customer",
        ),
        pytest.param(
            (
                prev_next_regions,
                None,
                "prev_next_regions",
                lambda: pd.DataFrame(
                    {
                        "two_preceding": [None, None, "AFRICA", "AMERICA", "ASIA"],
                        "one_preceding": [None, "AFRICA", "AMERICA", "ASIA", "EUROPE"],
                        "current": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "one_following": [
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                            None,
                        ],
                        "two_following": ["ASIA", "EUROPE", "MIDDLE EAST", None, None],
                    }
                ),
            ),
            id="prev_next_regions",
        ),
        pytest.param(
            (
                avg_order_diff_per_customer,
                None,
                "avg_order_diff_per_customer",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000075872",
                            "Customer#000004796",
                            "Customer#000112880",
                            "Customer#000041345",
                            "Customer#000119474",
                        ],
                        "avg_diff": [2195.0, 1998.0, 1995.0, 1863.0, 1787.0],
                    }
                ),
            ),
            id="avg_order_diff_per_customer",
        ),
        pytest.param(
            (
                yoy_change_in_num_orders,
                None,
                "yoy_change_in_num_orders",
                lambda: pd.DataFrame(
                    {
                        "year": range(1992, 1999),
                        "current_year_orders": [
                            227089,
                            226645,
                            227597,
                            228637,
                            228626,
                            227783,
                            133623,
                        ],
                        "pct_change": [
                            None,
                            -0.195518,
                            0.420040,
                            0.456948,
                            -0.0048111,
                            -0.368724,
                            -41.337589,
                        ],
                    }
                ),
            ),
            id="yoy_change_in_num_orders",
        ),
        pytest.param(
            (
                first_order_in_year,
                None,
                "first_order_in_year",
                lambda: pd.DataFrame(
                    {
                        "order_date": [f"{yr}-01-01" for yr in range(1992, 1999)],
                        "key": [3271, 15233, 290, 14178, 4640, 5895, 20064],
                    }
                ),
            ),
            id="first_order_in_year",
        ),
        pytest.param(
            (
                customer_largest_order_deltas,
                None,
                "customer_largest_order_deltas",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000054733",
                            "Customer#000107128",
                            "Customer#000019063",
                            "Customer#000100810",
                            "Customer#000127003",
                        ],
                        "largest_diff": [
                            454753.9935,
                            447181.8195,
                            446706.3978,
                            443366.9780,
                            442893.6328,
                        ],
                    }
                ),
            ),
            id="customer_largest_order_deltas",
        ),
        pytest.param(
            (
                suppliers_bal_diffs,
                None,
                "suppliers_bal_diffs",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Supplier#000004473",
                            "Supplier#000000188",
                            "Supplier#000005963",
                            "Supplier#000004115",
                            "Supplier#000007267",
                        ],
                        "region_name": [
                            "ASIA",
                            "MIDDLE EAST",
                            "AMERICA",
                            "EUROPE",
                            "EUROPE",
                        ],
                        "acctbal_delta": [44.43, 43.25, 43.15, 41.54, 41.48],
                    }
                ),
            ),
            id="suppliers_bal_diffs",
        ),
        pytest.param(
            (
                month_year_sliding_windows,
                None,
                "month_year_sliding_windows",
                lambda: pd.DataFrame(
                    {
                        "year": [1996] * 6 + [1997] * 4 + [1998] * 4,
                        "month": [1, 3, 5, 8, 10, 12, 3, 5, 7, 10, 1, 3, 5, 7],
                    }
                ),
            ),
            id="month_year_sliding_windows",
        ),
        pytest.param(
            (
                singular1,
                None,
                "singular1",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "nation_4_name": [None, None, None, None, "EGYPT"],
                    }
                ),
            ),
            id="singular1",
        ),
        pytest.param(
            (
                singular2,
                None,
                "singular2",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "ALGERIA",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
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
                            "CHINA",
                            "ROMANIA",
                            "SAUDI ARABIA",
                            "VIETNAM",
                            "RUSSIA",
                            "UNITED KINGDOM",
                            "UNITED STATES",
                        ],
                        "okey": [None] * 15 + [454791] + [None] * 9,
                    }
                ),
            ),
            id="singular2",
        ),
        pytest.param(
            (
                singular3,
                None,
                "singular3",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000000003",
                            "Customer#000000005",
                            "Customer#000000001",
                            "Customer#000000004",
                            "Customer#000000002",
                        ],
                    }
                ),
            ),
            id="singular3",
        ),
        pytest.param(
            (
                singular4,
                None,
                "singular4",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000000018",
                            "Customer#000000153",
                            "Customer#000000204",
                            "Customer#000000284",
                            "Customer#000000312",
                        ]
                    }
                ),
            ),
            id="singular4",
        ),
        pytest.param(
            (
                singular5,
                None,
                "singular5",
                lambda: pd.DataFrame(
                    {
                        "container": [
                            "MED CAN",
                            "LG JAR",
                            "LG CASE",
                            "WRAP CAN",
                            "LG DRUM",
                        ],
                        "highest_price_ship_date": [
                            "1992-03-09",
                            "1992-08-02",
                            "1992-08-10",
                            "1992-11-01",
                            "1992-11-22",
                        ],
                    }
                ),
            ),
            id="singular5",
        ),
        pytest.param(
            (
                singular6,
                None,
                "singular6",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in [28763, 98677, 37480, 29545, 85243]
                        ],
                        "receipt_date": [
                            "1992-02-03",
                            "1992-02-18",
                            "1992-04-03",
                            "1992-07-21",
                            "1992-08-15",
                        ],
                        "nation_name": [
                            "ARGENTINA",
                            "FRANCE",
                            "FRANCE",
                            "IRAQ",
                            "UNITED KINGDOM",
                        ],
                    }
                ),
            ),
            id="singular6",
        ),
        pytest.param(
            (
                singular7,
                None,
                "singular7",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000000687",
                            "Supplier#000000565",
                            "Supplier#000000977",
                            "Supplier#000001251",
                            "Supplier#000004625",
                        ],
                        "part_name": [
                            "peach orange blanched firebrick ghost",
                            "sienna lime mint frosted beige",
                            "blanched goldenrod lawn metallic midnight",
                            "seashell deep almond cyan lemon",
                            "red ivory indian seashell deep",
                        ],
                        "n_orders": [8, 7, 7, 7, 7],
                    }
                ),
            ),
            id="singular7",
        ),
        pytest.param(
            (
                avg_gap_prev_urgent_same_clerk,
                None,
                "avg_gap_prev_urgent_same_clerk",
                lambda: pd.DataFrame({"avg_delta": [7.9820674]}),
            ),
            id="avg_gap_prev_urgent_same_clerk",
        ),
        pytest.param(
            (
                top_customers_by_orders,
                None,
                "top_customers_by_orders",
                lambda: pd.DataFrame(
                    {
                        "customer_key": [3451, 102004, 102022, 79300, 117082],
                        "n_orders": [41, 41, 41, 40, 40],
                    }
                ),
            ),
            id="top_customers_by_orders",
        ),
    ],
)
def pydough_pipeline_test_data(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    dict[str, str] | list[str] | None,
    str,
    Callable[[], pd.DataFrame],
]:
    """
    Test data for `test_pipeline_e2e_tpch_custom`. Returns a tuple of the
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


def test_pipeline_until_relational_tpch_custom(
    pydough_pipeline_test_data: tuple[
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
    qualified DAG version, with the correct string representation. Run on
    custom queries with the TPC-H graph.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, columns, file_name, _ = pydough_pipeline_test_data
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
def test_pipeline_e2e_tpch_custom(
    pydough_pipeline_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        Callable[[], pd.DataFrame],
    ],
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the the custom queries with TPC-H data from the original
    code generation.
    """
    unqualified_impl, columns, _, answer_impl = pydough_pipeline_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, columns=columns, metadata=graph, database=sqlite_tpch_db_context
    )
    pd.testing.assert_frame_equal(result, answer_impl())


@pytest.mark.execute
@pytest.mark.parametrize(
    "impl, columns, error_msg",
    [
        pytest.param(
            bad_slice_1,
            None,
            "SLICE function currently only supports the step being integer literal 1 or absent.",
            id="bad_slice_1",
        ),
        pytest.param(
            bad_slice_2,
            None,
            "SLICE function currently only supports the step being integer literal 1 or absent.",
            id="bad_slice_2",
        ),
        pytest.param(
            bad_slice_3,
            None,
            "SLICE function currently only supports the start index being integer literal or absent.",
            id="bad_slice_3",
        ),
        pytest.param(
            bad_slice_4,
            None,
            "SLICE function currently only supports the start index being integer literal or absent.",
            id="bad_slice_4",
        ),
        pytest.param(
            bad_slice_5,
            None,
            "SLICE function currently only supports the start index being integer literal or absent.",
            id="bad_slice_5",
        ),
        pytest.param(
            bad_slice_6,
            None,
            "SLICE function currently only supports the stop index being integer literal or absent.",
            id="bad_slice_6",
        ),
        pytest.param(
            bad_slice_7,
            None,
            "SLICE function currently only supports the stop index being integer literal or absent.",
            id="bad_slice_7",
        ),
        pytest.param(
            bad_slice_8,
            None,
            "SLICE function currently only supports the stop index being integer literal or absent.",
            id="bad_slice_8",
        ),
        pytest.param(
            bad_slice_9,
            None,
            "SLICE function currently only supports the step being integer literal 1 or absent.",
            id="bad_slice_9",
        ),
        pytest.param(
            bad_slice_10,
            None,
            "SLICE function currently only supports the step being integer literal 1 or absent.",
            id="bad_slice_10",
        ),
        pytest.param(
            bad_slice_11,
            None,
            "SLICE function currently only supports the step being integer literal 1 or absent.",
            id="bad_slice_11",
        ),
        pytest.param(
            bad_slice_12,
            None,
            "PyDough objects are currently not supported to be used as indices in Python slices.",
            id="bad_slice_12",
        ),
        pytest.param(
            bad_slice_13,
            None,
            "PyDough objects are currently not supported to be used as indices in Python slices.",
            id="bad_slice_13",
        ),
        pytest.param(
            bad_slice_14,
            None,
            "PyDough objects are currently not supported to be used as indices in Python slices.",
            id="bad_slice_14",
        ),
        pytest.param(
            simple_scan,
            [],
            "Column selection must not be empty",
            id="bad_columns_1",
        ),
        pytest.param(
            simple_scan,
            {},
            "Column selection must not be empty",
            id="bad_columns_2",
        ),
        pytest.param(
            simple_scan,
            ["A", "B", "C"],
            "Unrecognized term of simple table collection 'Orders' in graph 'TPCH': 'A'",
            id="bad_columns_3",
        ),
        pytest.param(
            simple_scan,
            {"X": "key", "W": "Y"},
            "Unrecognized term of simple table collection 'Orders' in graph 'TPCH': 'Y'",
            id="bad_columns_4",
        ),
        pytest.param(
            simple_scan,
            ["key", "key"],
            "Duplicate column names found in root.",
            id="bad_columns_5",
        ),
    ],
)
def test_pipeline_e2e_errors(
    impl: Callable[[], UnqualifiedNode],
    columns: dict[str, str] | list[str] | None,
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
        to_df(root, columns=columns, metadata=graph, database=sqlite_tpch_db_context)
