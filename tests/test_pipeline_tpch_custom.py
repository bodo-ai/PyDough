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
    avg_acctbal_wo_debt,
    avg_gap_prev_urgent_same_clerk,
    avg_order_diff_per_customer,
    common_prefix_a,
    common_prefix_aa,
    common_prefix_ab,
    common_prefix_ac,
    common_prefix_ad,
    common_prefix_b,
    common_prefix_c,
    common_prefix_d,
    common_prefix_e,
    common_prefix_f,
    common_prefix_g,
    common_prefix_h,
    common_prefix_i,
    common_prefix_j,
    common_prefix_k,
    common_prefix_l,
    common_prefix_m,
    common_prefix_n,
    common_prefix_o,
    common_prefix_p,
    common_prefix_q,
    common_prefix_r,
    common_prefix_s,
    common_prefix_t,
    common_prefix_u,
    common_prefix_v,
    common_prefix_w,
    common_prefix_x,
    common_prefix_y,
    common_prefix_z,
    customer_largest_order_deltas,
    customer_most_recent_orders,
    datetime_current,
    datetime_relative,
    double_partition,
    dumb_aggregation,
    first_order_in_year,
    first_order_per_customer,
    function_sampler,
    global_acctbal_breakdown,
    highest_priority_per_year,
    month_year_sliding_windows,
    n_orders_first_day,
    nation_acctbal_breakdown,
    nation_best_order,
    nation_window_aggs,
    odate_and_rdate_avggap,
    order_info_per_priority,
    order_quarter_test,
    orders_versus_first_orders,
    part_reduced_size,
    parts_quantity_increase_95_96,
    percentile_customers_per_region,
    percentile_nations,
    prev_next_regions,
    quarter_function_test,
    rank_nations_by_region,
    rank_nations_per_region_by_customers,
    rank_parts_per_supplier_region_by_size,
    rank_with_filters_a,
    rank_with_filters_b,
    rank_with_filters_c,
    region_acctbal_breakdown,
    region_nation_window_aggs,
    region_orders_from_nations_richest,
    regional_first_order_best_line_part,
    regional_suppliers_percentile,
    richest_customer_per_region,
    simple_filter_top_five,
    simple_int_float_string_cast,
    simple_scan,
    simple_scan_top_five,
    simple_smallest_or_largest,
    simple_var_std,
    simple_var_std_with_nulls,
    singular1,
    singular2,
    singular3,
    singular4,
    singular5,
    singular6,
    singular7,
    string_format_specifiers_sqlite,
    supplier_best_part,
    supplier_pct_national_qty,
    suppliers_bal_diffs,
    top_customers_by_orders,
    triple_partition,
    wealthiest_supplier,
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
                parts_quantity_increase_95_96,
                None,
                "parts_quantity_increase_95_96",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "spring wheat sandy cornsilk cornflower",
                            "cyan almond peach honeydew medium",
                            "royal blush forest papaya navajo",
                        ],
                        "qty_95": [11, 8, 30],
                        "qty_96": [156, 152, 167],
                    }
                ),
            ),
            id="parts_quantity_increase_95_96",
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
        pytest.param(
            (
                customer_most_recent_orders,
                None,
                "customer_most_recent_orders",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000036487",
                            "Customer#000088562",
                            "Customer#000059543",
                        ],
                        "total_recent_value": [1614134.33, 1592016.2, 1565721.92],
                    }
                ),
            ),
            id="customer_most_recent_orders",
        ),
        pytest.param(
            (
                richest_customer_per_region,
                None,
                "richest_customer_per_region",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "nation_name": [
                            "MOROCCO",
                            "UNITED STATES",
                            "VIETNAM",
                            "GERMANY",
                            "SAUDI ARABIA",
                        ],
                        "customer_name": [
                            "Customer#000061453",
                            "Customer#000002487",
                            "Customer#000081976",
                            "Customer#000144232",
                            "Customer#000076011",
                        ],
                        "balance": [9999.99, 9999.72, 9998.36, 9999.74, 9998.68],
                    }
                ),
            ),
            id="richest_customer_per_region",
        ),
        pytest.param(
            (
                n_orders_first_day,
                None,
                "n_orders_first_day",
                lambda: pd.DataFrame(
                    {
                        "n_orders": [621],
                    }
                ),
            ),
            id="n_orders_first_day",
        ),
        pytest.param(
            (
                wealthiest_supplier,
                None,
                "wealthiest_supplier",
                lambda: pd.DataFrame(
                    {
                        "name": ["Supplier#000009450"],
                        "account_balance": [9999.72],
                    }
                ),
            ),
            id="wealthiest_supplier",
        ),
        pytest.param(
            (
                supplier_best_part,
                None,
                "supplier_best_part",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000006340",
                            "Supplier#000000580",
                            "Supplier#000006090",
                        ],
                        "part_name": [
                            "black sky red lavender navy",
                            "dark red antique mint gainsboro",
                            "cream navajo thistle dodger red",
                        ],
                        "total_quantity": [131, 103, 99],
                        "n_shipments": [4, 3, 3],
                    }
                ),
            ),
            id="supplier_best_part",
        ),
        pytest.param(
            (
                region_orders_from_nations_richest,
                None,
                "region_orders_from_nations_richest",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "n_orders": [74, 19, 62, 73, 41],
                    }
                ),
            ),
            id="region_orders_from_nations_richest",
        ),
        pytest.param(
            (
                regional_first_order_best_line_part,
                None,
                "regional_first_order_best_line_part",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "part_name": [
                            "tomato saddle brown cornsilk khaki",
                            "coral midnight cyan burlywood maroon",
                            "azure peru burnished seashell green",
                            "ivory peach linen lemon powder",
                            "cyan sienna ivory powder forest",
                        ],
                    }
                ),
            ),
            id="regional_first_order_best_line_part",
        ),
        pytest.param(
            (
                orders_versus_first_orders,
                None,
                "orders_versus_first_orders",
                lambda: pd.DataFrame(
                    {
                        "customer_name": [
                            "Customer#000063541",
                            "Customer#000066847",
                            "Customer#000072955",
                            "Customer#000082832",
                            "Customer#000003661",
                        ],
                        "order_key": [985892, 4451681, 2699750, 2005667, 4447044],
                        "days_since_first_order": [2399, 2399, 2398, 2398, 2396],
                    }
                ),
            ),
            id="orders_versus_first_orders",
        ),
        pytest.param(
            (
                nation_window_aggs,
                None,
                "nation_window_aggs",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "CHINA",
                            "JAPAN",
                            "VIETNAM",
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "JORDAN",
                            "SAUDI ARABIA",
                        ],
                        "key_sum": [205] * 15,
                        "key_avg": [13.666666] * 15,
                        "n_short_comment": [6] * 15,
                        "n_nations": [15] * 15,
                    }
                ),
            ),
            id="nation_window_aggs",
        ),
        pytest.param(
            (
                region_nation_window_aggs,
                None,
                "region_nation_window_aggs",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "CHINA",
                            "JAPAN",
                            "VIETNAM",
                            "FRANCE",
                            "GERMANY",
                            "ROMANIA",
                            "RUSSIA",
                            "JORDAN",
                            "SAUDI ARABIA",
                        ],
                        "key_sum": [45] * 3 + [22] * 3 + [51] * 3 + [54] * 4 + [33] * 2,
                        "key_avg": [15.0] * 3
                        + [7.333333] * 3
                        + [17.0] * 3
                        + [13.5] * 4
                        + [16.5] * 2,
                        "n_short_comment": [1] * 3 + [0] * 3 + [2] * 7 + [1] * 2,
                        "n_nations": [3] * 9 + [4] * 4 + [2] * 2,
                    }
                ),
            ),
            id="region_nation_window_aggs",
        ),
        pytest.param(
            (
                supplier_pct_national_qty,
                None,
                "supplier_pct_national_qty",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000009271",
                            "Supplier#000000543",
                            "Supplier#000007718",
                            "Supplier#000006460",
                            "Supplier#000002509",
                        ],
                        "nation_name": [
                            "MOZAMBIQUE",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "MOROCCO",
                            "ETHIOPIA",
                        ],
                        "supplier_quantity": [
                            49,
                            46,
                            39,
                            27,
                            68,
                        ],
                        "national_qty_pct": [
                            41.88034188,
                            36.80000000,
                            33.33333333,
                            21.60000000,
                            21.58730159,
                        ],
                    }
                ),
            ),
            id="supplier_pct_national_qty",
        ),
        pytest.param(
            (
                highest_priority_per_year,
                None,
                "highest_priority_per_year",
                lambda: pd.DataFrame(
                    {
                        "order_year": range(1992, 1999),
                        "highest_priority": [
                            "4-NOT SPECIFIED",
                            "5-LOW",
                            "1-URGENT",
                            "2-HIGH",
                            "1-URGENT",
                            "4-NOT SPECIFIED",
                            "5-LOW",
                        ],
                        "priority_pct": [
                            20.15817586,
                            20.14074875,
                            20.15711982,
                            20.14109702,
                            20.12631984,
                            20.08666142,
                            20.21358598,
                        ],
                    }
                ),
            ),
            id="highest_priority_per_year",
        ),
        pytest.param(
            (
                nation_best_order,
                None,
                "nation_best_order",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "CHINA",
                            "INDIA",
                            "INDONESIA",
                            "JAPAN",
                            "VIETNAM",
                        ],
                        "customer_name": [
                            "Customer#000100978",
                            "Customer#000131215",
                            "Customer#000141403",
                            "Customer#000100159",
                            "Customer#000090235",
                        ],
                        "order_key": [3312576, 2854465, 5925477, 1395745, 5983202],
                        "order_value": [
                            472728.8,
                            465198.5,
                            435458.79,
                            502742.76,
                            447386.22,
                        ],
                        "value_percentage": [
                            0.05775783,
                            0.05645062,
                            0.05276872,
                            0.06349147,
                            0.05597731,
                        ],
                    }
                ),
            ),
            id="nation_best_order",
        ),
        pytest.param(
            (
                nation_acctbal_breakdown,
                None,
                "nation_acctbal_breakdown",
                lambda: pd.DataFrame(
                    {
                        "nation_name": [
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                        ],
                        "n_red_acctbal": [551, 536, 567, 560, 540],
                        "n_black_acctbal": [5424, 5463, 5453, 5415, 5443],
                        "median_red_acctbal": [
                            -508.82,
                            -472.575,
                            -510.96,
                            -493.74,
                            -505.86,
                        ],
                        "median_black_acctbal": [
                            4928.855,
                            4871.61,
                            4909.01,
                            4906.32,
                            5121.41,
                        ],
                        "median_overall_acctbal": [
                            4433.16,
                            4413.11,
                            4436.05,
                            4399.28,
                            4582.11,
                        ],
                    }
                ),
            ),
            id="nation_acctbal_breakdown",
        ),
        pytest.param(
            (
                region_acctbal_breakdown,
                None,
                "region_acctbal_breakdown",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "n_red_acctbal": [2739, 2754, 2729, 2726, 2744],
                        "n_black_acctbal": [27025, 27198, 27454, 27471, 27160],
                        "median_red_acctbal": [
                            -504.68,
                            -496.385,
                            -480.64,
                            -515.045,
                            -494.435,
                        ],
                        "median_black_acctbal": [
                            5005.21,
                            4941.92,
                            5008.5,
                            4993.3,
                            4990.1,
                        ],
                        "median_overall_acctbal": [
                            4498.735,
                            4449.68,
                            4500.78,
                            4494.33,
                            4448.26,
                        ],
                    }
                ),
            ),
            id="region_acctbal_breakdown",
        ),
        pytest.param(
            (
                global_acctbal_breakdown,
                None,
                "global_acctbal_breakdown",
                lambda: pd.DataFrame(
                    {
                        "n_red_acctbal": [13692],
                        "n_black_acctbal": [136308],
                        "median_red_acctbal": [-496.59],
                        "median_black_acctbal": [4988.755],
                        "median_overall_acctbal": [4477.3],
                    }
                ),
            ),
            id="global_acctbal_breakdown",
        ),
        pytest.param(
            (
                simple_int_float_string_cast,
                None,
                "simple_int_float_string_cast",
                lambda: pd.DataFrame(
                    {
                        "i1": [1],
                        "i2": [2],
                        "i3": [3],
                        "i4": [4],
                        "i5": [-5],
                        "i6": [-6],
                        "f1": [1.0],
                        "f2": [2.2],
                        "f3": [3.0],
                        "f4": [4.3],
                        "f5": [-5.888],
                        "f6": [-6.0],
                        "f7": [0.0],
                        "s1": ["1"],
                        "s2": ["2.2"],
                        "s3": ["3"],
                        "s4": ["4.3"],
                        "s5": ["-5.888"],
                        "s6": ["-6.0"],
                        "s7": ["0.0"],
                        "s8": ["0.0"],
                        "s9": ["abc def"],
                    }
                ),
            ),
            id="simple_int_float_string_cast",
        ),
        pytest.param(
            (
                string_format_specifiers_sqlite,
                None,
                "string_format_specifiers_sqlite",
                lambda: pd.DataFrame(
                    {
                        "d1": ["15"],
                        "d2": ["15"],
                        "d3": ["45.000"],
                        "d4": ["2023-07-15"],
                        "d5": ["14"],
                        "d6": ["02"],
                        "d7": ["196"],
                        "d8": ["2460141.1046875"],
                        "d9": ["14"],
                        "d10": [" 2"],
                        "d11": ["07"],
                        "d12": ["30"],
                        "d13": ["PM"],
                        "d14": ["pm"],
                        "d15": ["14:30"],
                        "d16": ["1689431445"],
                        "d17": ["45"],
                        "d18": ["14:30:45"],
                        "d19": ["6"],
                        "d20": ["6"],
                        "d21": ["28"],
                        "d22": ["2023"],
                        "d23": ["07-15-2023"],
                    }
                ),
            ),
            id="string_format_specifiers_sqlite",
        ),
        pytest.param(
            (
                part_reduced_size,
                None,
                "part_reduced_size",
                lambda: pd.DataFrame(
                    {
                        "reduced_size": [2.8, 2.8, 4.0, 4.0, 2.8],
                        "retail_price_int": [901, 901, 901, 901, 901],
                        "message": [
                            "old size: 7",
                            "old size: 7",
                            "old size: 10",
                            "old size: 10",
                            "old size: 7",
                        ],
                        "discount": [0.1, 0.1, 0.1, 0.1, 0.09],
                        "date_dmy": [
                            "01-11-1995",
                            "02-11-1992",
                            "07-11-1997",
                            "06-08-1996",
                            "06-07-1997",
                        ],
                        "date_md": ["11/01", "11/02", "11/07", "08/06", "07/06"],
                        "am_pm": ["00:00AM"] * 5,
                    }
                ),
            ),
            id="part_reduced_size",
        ),
        pytest.param(
            (
                simple_smallest_or_largest,
                None,
                "simple_smallest_or_largest",
                lambda: pd.DataFrame(
                    {
                        "s1": [10],
                        "s2": [20],
                        "s3": [0],
                        "s4": [-200],
                        "s5": [None],
                        "s6": [-0.34],
                        "s7": ["2023-01-01 00:00:00"],
                        "s8": [""],
                        "s9": [None],
                        "l1": [20],
                        "l2": [20],
                        "l3": [20],
                        "l4": [300],
                        "l5": [None],
                        "l6": [100.22],
                        "l7": ["2025-01-01 00:00:00"],
                        "l8": ["alphabet soup"],
                        "l9": [None],
                    }
                ),
            ),
            id="simple_smallest_or_largest",
        ),
        pytest.param(
            (
                avg_acctbal_wo_debt,
                None,
                "avg_acctbal_wo_debt",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "ASIA",
                            "EUROPE",
                            "MIDDLE EAST",
                        ],
                        "avg_bal_without_debt_erasure": [
                            4547.554121,
                            4536.852848,
                            4548.741422,
                            4539.072249,
                            4533.254352,
                        ],
                    },
                ),
            ),
            id="avg_acctbal_wo_debt",
        ),
        pytest.param(
            (
                odate_and_rdate_avggap,
                None,
                "odate_and_rdate_avggap",
                lambda: pd.DataFrame({"avg_gap": [50.41427]}),
            ),
            id="odate_and_rdate_avggap",
        ),
        pytest.param(
            (
                dumb_aggregation,
                None,
                "dumb_aggregation",
                lambda: pd.DataFrame(
                    {
                        "nation_name": ["ALGERIA", "ARGENTINA"],
                        "a1": ["AFRICA", "AMERICA"],
                        "a2": ["AFRICA", "AMERICA"],
                        "a3": [0, 1],
                        "a4": [1, 0],
                        "a5": [1, 1],
                        "a6": [0, 1],
                        "a7": ["AFRICA", "AMERICA"],
                        "a8": [0, 1],
                    }
                ),
            ),
            id="dumb_aggregation",
        ),
        pytest.param(
            (
                simple_var_std,
                None,
                "simple_var_std",
                lambda: pd.DataFrame(
                    {
                        "name": ["ALGERIA", "ARGENTINA"],
                        "var": [9.268000e06, 1.003823e07],
                        "std": [3044.339064, 3168.316441],
                        "sample_var": [9.290120e06, 1.006259e07],
                        "sample_std": [3047.969762, 3172.159155],
                        "pop_var": [9.268000e06, 1.003823e07],
                        "pop_std": [3044.339064, 3168.316441],
                    }
                ),
            ),
            id="simple_var_std",
        ),
        pytest.param(
            (
                simple_var_std_with_nulls,
                None,
                "simple_var_std_with_nulls",
                lambda: pd.DataFrame(
                    {
                        "var_samp_0_nnull": [None],
                        "var_samp_1_nnull": [None],
                        "var_samp_2_nnull": [27206154.83045],
                        "var_pop_0_nnull": [None],
                        "var_pop_1_nnull": [0.0],
                        "var_pop_2_nnull": [13603077.415225],
                        "std_samp_0_nnull": [None],
                        "std_samp_1_nnull": [None],
                        "std_samp_2_nnull": [5215.951958],
                        "std_pop_0_nnull": [None],
                        "std_pop_1_nnull": [0.0],
                        "std_pop_2_nnull": [3688.235],
                    }
                ),
            ),
            id="simple_var_std_with_nulls",
        ),
        pytest.param(
            (
                quarter_function_test,
                None,
                "quarter_function_test",
                lambda: pd.DataFrame(
                    {
                        "_expr0": [1],
                        "_expr1": [1],
                        "_expr2": [1],
                        "_expr3": [2],
                        "_expr4": [2],
                        "_expr5": [2],
                        "_expr6": [3],
                        "_expr7": [3],
                        "_expr8": [3],
                        "_expr9": [4],
                        "_expr10": [4],
                        "_expr11": [4],
                        "_expr12": [1],
                        "q1_jan": ["2023-01-01"],
                        "q1_feb": ["2023-01-01"],
                        "q1_mar": ["2023-01-01"],
                        "q2_apr": ["2023-04-01"],
                        "q2_may": ["2023-04-01"],
                        "q2_jun": ["2023-04-01"],
                        "q3_jul": ["2023-07-01"],
                        "q3_aug": ["2023-07-01"],
                        "q3_sep": ["2023-07-01"],
                        "q4_oct": ["2023-10-01"],
                        "q4_nov": ["2023-10-01"],
                        "q4_dec": ["2023-10-01"],
                        "ts_q1": ["2024-01-01"],
                        "alias1": ["2023-04-01"],
                        "alias2": ["2023-07-01"],
                        "alias3": ["2023-10-01"],
                        "alias4": ["2023-01-01"],
                        "chain1": ["2023-04-02 02:00:00"],
                        "chain2": ["2023-07-01"],
                        "chain3": ["2023-10-01"],
                        "plus_1q": ["2023-04-15 12:30:45"],
                        "plus_2q": ["2023-07-15 12:30:45"],
                        "plus_3q": ["2023-10-15 00:00:00"],
                        "minus_1q": ["2022-10-15 12:30:45"],
                        "minus_2q": ["2022-07-15 12:30:45"],
                        "minus_3q": ["2022-04-15 00:00:00"],
                        "syntax1": ["2023-08-15 00:00:00"],
                        "syntax2": ["2024-02-15 00:00:00"],
                        "syntax3": ["2024-08-15 00:00:00"],
                        "syntax4": ["2022-08-15 00:00:00"],
                        "q_diff1": [1],
                        "q_diff2": [2],
                        "q_diff3": [3],
                        "q_diff4": [3],
                        "q_diff5": [4],
                        "q_diff6": [5],
                        "q_diff7": [6],
                        "q_diff8": [20],
                        "q_diff9": [-1],
                        "q_diff10": [-4],
                        "q_diff11": [1],
                        "q_diff12": [1],
                    }
                ),
            ),
            id="quarter_function_test",
        ),
        pytest.param(
            (
                order_quarter_test,
                None,
                "order_quarter_test",
                lambda: pd.DataFrame(
                    {
                        "order_date": ["1995-01-01"],
                        "quarter": [1],
                        "quarter_start": ["1995-01-01"],
                        "next_quarter": ["1995-04-01 00:00:00"],
                        "prev_quarter": ["1994-10-01 00:00:00"],
                        "two_quarters_ahead": ["1995-07-01 00:00:00"],
                        "two_quarters_behind": ["1994-07-01 00:00:00"],
                        "quarters_since_1995": [0],
                        "quarters_until_2000": [20],
                        "same_quarter_prev_year": ["1994-01-01 00:00:00"],
                        "same_quarter_next_year": ["1996-01-01 00:00:00"],
                    }
                ),
            ),
            id="order_quarter_test",
        ),
        pytest.param(
            (
                common_prefix_a,
                None,
                "common_prefix_a",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                    }
                ),
            ),
            id="common_prefix_a",
        ),
        pytest.param(
            (
                common_prefix_b,
                None,
                "common_prefix_b",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                    }
                ),
            ),
            id="common_prefix_b",
        ),
        pytest.param(
            (
                common_prefix_c,
                None,
                "common_prefix_c",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                        "n_orders": [298994, 299103, 301740, 303286, 296877],
                        "n_parts": [156400, 162880, 160240, 158960, 161520],
                    }
                ),
            ),
            id="common_prefix_c",
        ),
        pytest.param(
            (
                common_prefix_d,
                None,
                "common_prefix_d",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                        "n_orders_94": [45152, 45335, 46008, 46093, 45009],
                        "n_orders_95": [45822, 45630, 45731, 46197, 45257],
                        "n_orders_96": [45352, 45549, 45976, 46518, 45231],
                    }
                ),
            ),
            id="common_prefix_d",
        ),
        pytest.param(
            (
                common_prefix_e,
                None,
                "common_prefix_e",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_nations": [5, 5, 5, 5, 5],
                    }
                ),
            ),
            id="common_prefix_e",
        ),
        pytest.param(
            (
                common_prefix_f,
                None,
                "common_prefix_f",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                    }
                ),
            ),
            id="common_prefix_f",
        ),
        pytest.param(
            (
                common_prefix_g,
                None,
                "common_prefix_g",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                        "n_nations": [5, 5, 5, 5, 5],
                    }
                ),
            ),
            id="common_prefix_g",
        ),
        pytest.param(
            (
                common_prefix_h,
                None,
                "common_prefix_h",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_orders": [298994, 299103, 301740, 303286, 296877],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_parts": [156400, 162880, 160240, 158960, 161520],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                    }
                ),
            ),
            id="common_prefix_h",
        ),
        pytest.param(
            (
                common_prefix_i,
                None,
                "common_prefix_i",
                lambda: pd.DataFrame(
                    {
                        "name": ["FRANCE", "ROMANIA", "RUSSIA", "JORDAN", "CHINA"],
                        "n_customers": [6100, 6100, 6078, 6033, 6024],
                        "n_selected_orders": [1, 2, 1, 1, 1],
                    }
                ),
            ),
            id="common_prefix_i",
        ),
        pytest.param(
            (
                common_prefix_j,
                None,
                "common_prefix_j",
                lambda: pd.DataFrame(
                    {
                        "cust_name": [f"Customer#{i:09}" for i in range(1, 6)],
                        "nation_name": [
                            "MOROCCO",
                            "JORDAN",
                            "ARGENTINA",
                            "EGYPT",
                            "CANADA",
                        ],
                        "region_name": [
                            "AFRICA",
                            "MIDDLE EAST",
                            "AMERICA",
                            "MIDDLE EAST",
                            "AMERICA",
                        ],
                    }
                ),
            ),
            id="common_prefix_j",
        ),
        pytest.param(
            (
                common_prefix_k,
                None,
                "common_prefix_k",
                lambda: pd.DataFrame(
                    {
                        "cust_name": [f"Customer#{i:09}" for i in range(1, 6)],
                        "region_name": [
                            "AFRICA",
                            "MIDDLE EAST",
                            "AMERICA",
                            "MIDDLE EAST",
                            "AMERICA",
                        ],
                        "nation_name": [
                            "MOROCCO",
                            "JORDAN",
                            "ARGENTINA",
                            "EGYPT",
                            "CANADA",
                        ],
                    }
                ),
            ),
            id="common_prefix_k",
        ),
        pytest.param(
            (
                common_prefix_l,
                None,
                "common_prefix_l",
                lambda: pd.DataFrame(
                    {
                        "cust_name": [f"Customer#{i:09}" for i in (11, 15, 18, 20, 26)],
                        "nation_name": [
                            "UNITED KINGDOM",
                            "UNITED KINGDOM",
                            "FRANCE",
                            "RUSSIA",
                            "RUSSIA",
                        ],
                        "n_selected_suppliers": [173, 173, 167, 182, 182],
                        "selected_suppliers_min": [
                            -898.3,
                            -898.3,
                            -993.76,
                            -878.57,
                            -878.57,
                        ],
                        "selected_suppliers_max": [
                            9938.53,
                            9938.53,
                            9807.46,
                            9837.53,
                            9837.53,
                        ],
                        "selected_suppliers_avg": [
                            4496.83,
                            4496.83,
                            4725.76,
                            4943.47,
                            4943.47,
                        ],
                        "selected_suppliers_sum": [
                            777952.16,
                            777952.16,
                            789202.26,
                            899711.81,
                            899711.81,
                        ],
                    }
                ),
            ),
            id="common_prefix_l",
        ),
        pytest.param(
            (
                common_prefix_m,
                None,
                "common_prefix_m",
                lambda: pd.DataFrame(
                    {
                        "cust_name": [f"Customer#{i:09}" for i in (11, 15, 18, 20, 26)],
                        "n_selected_suppliers": [173, 173, 167, 182, 182],
                        "selected_suppliers_min": [
                            -898.3,
                            -898.3,
                            -993.76,
                            -878.57,
                            -878.57,
                        ],
                        "selected_suppliers_max": [
                            9938.53,
                            9938.53,
                            9807.46,
                            9837.53,
                            9837.53,
                        ],
                        "selected_suppliers_avg": [
                            4496.83,
                            4496.83,
                            4725.76,
                            4943.47,
                            4943.47,
                        ],
                        "selected_suppliers_sum": [
                            777952.16,
                            777952.16,
                            789202.26,
                            899711.81,
                            899711.81,
                        ],
                        "nation_name": [
                            "UNITED KINGDOM",
                            "UNITED KINGDOM",
                            "FRANCE",
                            "RUSSIA",
                            "RUSSIA",
                        ],
                    }
                ),
            ),
            id="common_prefix_m",
        ),
        pytest.param(
            (
                common_prefix_n,
                None,
                "common_prefix_n",
                lambda: pd.DataFrame(
                    {
                        "key": [3292610, 927968, 1117219, 3874244, 1069636],
                        "order_date": [
                            "1998-07-19",
                            "1998-07-13",
                            "1998-07-11",
                            "1998-07-10",
                            "1998-06-23",
                        ],
                        "n_elements": [5, 4, 7, 7, 5],
                        "total_retail_price": [
                            7609.67,
                            6379.40,
                            9620.69,
                            9268.20,
                            7377.55,
                        ],
                        "n_unique_supplier_nations": [4, 3, 6, 6, 4],
                        "max_supplier_balance": [
                            8615.81,
                            8797.73,
                            9199.28,
                            9469.81,
                            7575.13,
                        ],
                        "n_small_parts": [1, 0, 2, 4, 1],
                    }
                ),
            ),
            id="common_prefix_n",
        ),
        pytest.param(
            (
                common_prefix_o,
                None,
                "common_prefix_o",
                lambda: pd.DataFrame(
                    {
                        "key": [435237, 4682917, 4069735, 464226, 791522],
                        "order_date": [
                            "1998-06-16",
                            "1998-06-14",
                            "1998-04-22",
                            "1998-01-07",
                            "1997-10-18",
                        ],
                        "n_elements": [7] * 5,
                        "n_unique_containers": [6] * 5,
                        "n_unique_supplier_nations": [6, 5, 6, 6, 6],
                        "max_supplier_balance": [
                            7205.20,
                            9487.41,
                            8471.66,
                            9852.52,
                            9182.14,
                        ],
                        "n_small_parts": [1, 2, 2, 2, 1],
                    }
                ),
            ),
            id="common_prefix_o",
        ),
        pytest.param(
            (
                common_prefix_p,
                None,
                "common_prefix_p",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (140698, 1, 2, 4, 7)],
                        "n_orders": [5, 2, 4, 3, 5],
                        "n_parts_ordered": [2, 1, 2, 1, 1],
                        "n_distinct_parts": [1, 1, 2, 1, 1],
                    }
                ),
            ),
            id="common_prefix_p",
        ),
        pytest.param(
            (
                common_prefix_q,
                None,
                "common_prefix_q",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (127207, 14839, 18637, 52351, 117196)
                        ],
                        "total_spent": [
                            907224.66,
                            902286.22,
                            856788.74,
                            842691.87,
                            836571.25,
                        ],
                        "line_price": [
                            86804.77,
                            83715.40,
                            69682.50,
                            71998.67,
                            93875.18,
                        ],
                        "part_name": [
                            "slate beige orange black burlywood",
                            "chiffon ivory salmon frosted linen",
                            "chartreuse cream royal misty cornflower",
                            "lavender tomato midnight orchid thistle",
                            "green navy sky blue lemon",
                        ],
                    }
                ),
            ),
            id="common_prefix_q",
        ),
        pytest.param(
            (
                common_prefix_r,
                None,
                "common_prefix_r",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (127207, 14839, 18637, 52351, 117196)
                        ],
                        "part_name": [
                            "slate beige orange black burlywood",
                            "chiffon ivory salmon frosted linen",
                            "chartreuse cream royal misty cornflower",
                            "lavender tomato midnight orchid thistle",
                            "green navy sky blue lemon",
                        ],
                        "line_price": [
                            86804.77,
                            83715.40,
                            69682.50,
                            71998.67,
                            93875.18,
                        ],
                        "total_spent": [
                            907224.66,
                            902286.22,
                            856788.74,
                            842691.87,
                            836571.25,
                        ],
                    }
                ),
            ),
            id="common_prefix_r",
        ),
        pytest.param(
            (
                common_prefix_s,
                None,
                "common_prefix_s",
                lambda: pd.DataFrame(
                    {
                        "name": ["Customer#000106507"],
                        "most_recent_order_date": ["1998-05-25"],
                        "most_recent_order_total": [7],
                        "most_recent_order_distinct": [6],
                    }
                ),
            ),
            id="common_prefix_s",
        ),
        pytest.param(
            (
                common_prefix_t,
                None,
                "common_prefix_t",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (126850, 80485, 86209, 73420, 146809)
                        ],
                        "total_qty": [3670, 3439, 3422, 3409, 3409],
                    }
                ),
            ),
            id="common_prefix_t",
        ),
        pytest.param(
            (
                common_prefix_u,
                None,
                "common_prefix_u",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (111613, 112126, 92282, 69872, 135349)
                        ],
                        "total_qty": [169, 162, 151, 150, 136],
                    }
                ),
            ),
            id="common_prefix_u",
        ),
        pytest.param(
            (
                common_prefix_v,
                None,
                "common_prefix_v",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (3, 14, 29, 30, 48)],
                        "region_name": [
                            "AMERICA",
                            "AMERICA",
                            "AFRICA",
                            "AMERICA",
                            "AFRICA",
                        ],
                    }
                ),
            ),
            id="common_prefix_v",
        ),
        pytest.param(
            (
                common_prefix_w,
                None,
                "common_prefix_w",
                lambda: pd.DataFrame(
                    {
                        "key": [37, 64, 68, 228, 293],
                        "cust_nation_name": ["ALGERIA"]
                        + ["ARGENTINA"] * 3
                        + ["ALGERIA"],
                    }
                ),
            ),
            id="common_prefix_w",
        ),
        pytest.param(
            (
                common_prefix_x,
                None,
                "common_prefix_x",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (3451, 102004, 102022, 79300, 117082)
                        ],
                        "n_orders": [41, 41, 41, 40, 40],
                    }
                ),
            ),
            id="common_prefix_x",
        ),
        pytest.param(
            (
                common_prefix_y,
                None,
                "common_prefix_y",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (138841, 36091, 54952, 103768, 46081)
                        ],
                        "n_orders": [21, 20, 19, 19, 17],
                    }
                ),
            ),
            id="common_prefix_y",
        ),
        pytest.param(
            (
                common_prefix_z,
                None,
                "common_prefix_z",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (7, 9, 19, 21, 25)],
                        "nation_name": ["CHINA", "INDIA"] * 2 + ["JAPAN"],
                    }
                ),
            ),
            id="common_prefix_z",
        ),
        pytest.param(
            (
                common_prefix_aa,
                None,
                "common_prefix_aa",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (1, 2, 4, 6, 7)],
                        "nation_name": [
                            "MOROCCO",
                            "JORDAN",
                            "EGYPT",
                            "SAUDI ARABIA",
                            "CHINA",
                        ],
                    }
                ),
            ),
            id="common_prefix_aa",
        ),
        pytest.param(
            (
                common_prefix_ab,
                None,
                "common_prefix_ab",
                lambda: pd.DataFrame({"n": [54318]}),
            ),
            id="common_prefix_ab",
        ),
        pytest.param(
            (
                common_prefix_ac,
                None,
                "common_prefix_ac",
                lambda: pd.DataFrame({"n": [137398]}),
            ),
            id="common_prefix_ac",
        ),
        pytest.param(
            (
                common_prefix_ad,
                None,
                "common_prefix_ad",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000004704",
                            "Supplier#000006661",
                            "Supplier#000009766",
                        ],
                        "part_name": [
                            "slate rosy misty medium mint",
                            "goldenrod plum dark aquamarine bisque",
                            "drab navajo rosy cornflower green",
                        ],
                        "qty_shipped": [4, 38, 31],
                    }
                ),
            ),
            id="common_prefix_ad",
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
    from pydough import to_sql

    print()
    print(
        to_sql(root, columns=columns, metadata=graph, database=sqlite_tpch_db_context)
    )
    print(result)
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
            "Unrecognized term of simple table collection 'orders' in graph 'TPCH': 'A'",
            id="bad_columns_3",
        ),
        pytest.param(
            simple_scan,
            {"X": "key", "W": "Y"},
            "Unrecognized term of simple table collection 'orders' in graph 'TPCH': 'Y'",
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
