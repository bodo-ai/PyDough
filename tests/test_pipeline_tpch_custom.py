"""
Integration tests for the PyDough workflow with custom questions on the TPC-H
dataset.
"""

import re
from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext
from pydough.database_connectors.database_connector import DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.test_pydough_functions.bad_pydough_functions import (
    bad_cross_1,
    bad_cross_2,
    bad_cross_3,
    bad_cross_4,
    bad_cross_5,
    bad_cross_6,
    bad_cross_7,
    bad_cross_8,
    bad_cross_9,
    bad_cross_10,
    bad_cross_11,
    bad_name_1,
    bad_name_2,
    bad_name_3,
    bad_name_4,
    bad_name_5,
    bad_name_6,
    bad_name_7,
    bad_name_8,
    bad_name_9,
    bad_name_10,
    bad_name_11,
    bad_name_12,
    bad_name_13,
    bad_name_14,
    bad_name_15,
    bad_name_16,
    bad_name_17,
    bad_name_18,
    bad_name_19,
    bad_name_20,
    bad_name_21,
    bad_name_22,
    bad_name_23,
    bad_name_24,
    bad_name_25,
    bad_quantile_1,
    bad_quantile_2,
    bad_quantile_3,
    bad_quantile_4,
    bad_quantile_5,
    bad_quantile_6,
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
from tests.test_pydough_functions.simple_pydough_functions import (
    agg_partition,
    aggregation_analytics_1,
    aggregation_analytics_2,
    aggregation_analytics_3,
    avg_acctbal_wo_debt,
    avg_gap_prev_urgent_same_clerk,
    avg_order_diff_per_customer,
    bad_child_reuse_1,
    bad_child_reuse_2,
    bad_child_reuse_3,
    bad_child_reuse_4,
    bad_child_reuse_5,
    customer_largest_order_deltas,
    customer_most_recent_orders,
    datetime_current,
    datetime_relative,
    deep_best_analysis,
    double_cross,
    double_partition,
    dumb_aggregation,
    extract_colors,
    first_order_in_year,
    first_order_per_customer,
    floor_and_ceil,
    floor_and_ceil_2,
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
    quantile_function_test_1,
    quantile_function_test_2,
    quantile_function_test_3,
    quantile_function_test_4,
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
    simple_cross_1,
    simple_cross_2,
    simple_cross_3,
    simple_cross_4,
    simple_cross_5,
    simple_cross_6,
    simple_cross_7,
    simple_cross_8,
    simple_cross_9,
    simple_cross_10,
    simple_cross_11,
    simple_cross_12,
    simple_cross_13,
    simple_cross_14,
    simple_cross_15,
    simple_cross_16,
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
    window_filter_order_1,
    window_filter_order_2,
    window_filter_order_3,
    window_filter_order_4,
    window_filter_order_5,
    window_filter_order_6,
    window_filter_order_7,
    window_filter_order_8,
    window_filter_order_9,
    window_filter_order_10,
    year_month_nation_orders,
    yoy_change_in_num_orders,
)
from tests.test_pydough_functions.user_collections import (
    simple_range_1,
    simple_range_2,
    simple_range_3,
    simple_range_4,
    simple_range_5,
    user_range_collection_1,
    user_range_collection_2,
    user_range_collection_3,
    user_range_collection_4,
    user_range_collection_5,
    user_range_collection_6,
)

from .conftest import tpch_custom_test_data_dialect_replacements
from .testing_utilities import PyDoughPandasTest, graph_fetcher, run_e2e_error_test


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                simple_scan_top_five,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                    }
                ),
                "simple_scan_top_five",
            ),
            id="simple_scan_top_five",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_filter_top_five,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [5989315, 5935174, 5881093, 5876066, 5866437],
                    }
                ),
                "simple_filter_top_five",
                columns=["key"],
            ),
            id="simple_filter_top_five",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_nations_by_region,
                "TPCH",
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
                "rank_nations_by_region",
            ),
            id="rank_nations_by_region",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_nations_per_region_by_customers,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["KENYA", "CANADA", "INDONESIA", "FRANCE", "JORDAN"],
                        "rank": [1] * 5,
                    }
                ),
                "rank_nations_per_region_by_customers",
            ),
            id="rank_nations_per_region_by_customers",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_parts_per_supplier_region_by_size,
                "TPCH",
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
                "rank_parts_per_supplier_region_by_size",
            ),
            id="rank_parts_per_supplier_region_by_size",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_with_filters_a,
                "TPCH",
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
                "rank_with_filters_a",
            ),
            id="rank_with_filters_a",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_with_filters_b,
                "TPCH",
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
                "rank_with_filters_b",
            ),
            id="rank_with_filters_b",
        ),
        pytest.param(
            PyDoughPandasTest(
                rank_with_filters_c,
                "TPCH",
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
                "rank_with_filters_c",
                columns={"pname": "name", "psize": "size"},
            ),
            id="rank_with_filters_c",
        ),
        pytest.param(
            PyDoughPandasTest(
                percentile_nations,
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
                        "p1": [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + [5] * 5,
                        "p2": [1] * 5 + [2] * 5 + [3] * 5 + [4] * 5 + [5] * 5,
                    }
                ),
                "percentile_nations",
                columns={"name": "name", "p1": "p", "p2": "p"},
            ),
            id="percentile_nations",
        ),
        pytest.param(
            PyDoughPandasTest(
                percentile_customers_per_region,
                "TPCH",
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
                "percentile_customers_per_region",
            ),
            id="percentile_customers_per_region",
        ),
        pytest.param(
            PyDoughPandasTest(
                regional_suppliers_percentile,
                "TPCH",
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
                "regional_suppliers_percentile",
                columns=["name"],
            ),
            id="regional_suppliers_percentile",
        ),
        pytest.param(
            PyDoughPandasTest(
                function_sampler,
                "TPCH",
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
                "function_sampler",
            ),
            id="function_sampler",
        ),
        pytest.param(
            PyDoughPandasTest(
                order_info_per_priority,
                "TPCH",
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
                "order_info_per_priority",
            ),
            id="order_info_per_priority",
        ),
        pytest.param(
            PyDoughPandasTest(
                year_month_nation_orders,
                "TPCH",
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
                "year_month_nation_orders",
            ),
            id="year_month_nation_orders",
        ),
        pytest.param(
            PyDoughPandasTest(
                floor_and_ceil,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "floor_frac": [5],
                        "ceil_frac": [6],
                        "floor_frac_neg": [-6],
                        "ceil_frac_neg": [-5],
                        "floor_int": [6],
                        "ceil_int": [6],
                        "floor_int_neg": [-6],
                        "ceil_int_neg": [-6],
                    }
                ),
                "floor_and_ceil",
            ),
            id="floor_and_ceil",
        ),
        pytest.param(
            PyDoughPandasTest(
                floor_and_ceil_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_key": [
                            5851,
                            5991,
                            7511,
                            7846,
                            1186,
                            8300,
                            4578,
                            8755,
                            1635,
                            7358,
                        ],
                        "part_key": [
                            185850,
                            15990,
                            45006,
                            137845,
                            116163,
                            175782,
                            117044,
                            43746,
                            51634,
                            87357,
                        ],
                        "complete_parts": [
                            9994,
                            9997,
                            9980,
                            9989,
                            9984,
                            9983,
                            9989,
                            9970,
                            9966,
                            9961,
                        ],
                        "total_cost": [
                            9994000,
                            9988903,
                            9979801,
                            9979211,
                            9973517,
                            9967627,
                            9964028,
                            9963919,
                            9963609,
                            9958411,
                        ],
                    }
                ),
                "floor_and_ceil_2",
            ),
            id="floor_and_ceil_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                datetime_current,
                "TPCH",
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
                "datetime_current",
            ),
            id="datetime_current",
        ),
        pytest.param(
            PyDoughPandasTest(
                datetime_relative,
                "TPCH",
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
                "datetime_relative",
            ),
            id="datetime_relative",
        ),
        pytest.param(
            PyDoughPandasTest(
                agg_partition,
                "TPCH",
                lambda: pd.DataFrame({"best_year": [228637]}),
                "agg_partition",
            ),
            id="agg_partition",
        ),
        pytest.param(
            PyDoughPandasTest(
                double_partition,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "year": [1992, 1993, 1994, 1995, 1996, 1997, 1998],
                        "best_month": [19439, 19319, 19546, 19502, 19724, 19519, 19462],
                    }
                ),
                "double_partition",
            ),
            id="double_partition",
        ),
        pytest.param(
            PyDoughPandasTest(
                triple_partition,
                "TPCH",
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
                "triple_partition",
                columns={"region": "supp_region", "avgpct": "avg_percentage"},
            ),
            id="triple_partition",
        ),
        pytest.param(
            PyDoughPandasTest(
                first_order_per_customer,
                "TPCH",
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
                "first_order_per_customer",
            ),
            id="first_order_per_customer",
        ),
        pytest.param(
            PyDoughPandasTest(
                prev_next_regions,
                "TPCH",
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
                "prev_next_regions",
            ),
            id="prev_next_regions",
        ),
        pytest.param(
            PyDoughPandasTest(
                avg_order_diff_per_customer,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000015038",
                            "Customer#000026734",
                            "Customer#000075320",
                            "Customer#000097888",
                            "Customer#000102875",
                        ],
                        "avg_diff": [2361.0, 2260.0, 2309.0, 2269.0, 2309.0],
                    }
                ),
                "avg_order_diff_per_customer",
            ),
            id="avg_order_diff_per_customer",
        ),
        pytest.param(
            PyDoughPandasTest(
                yoy_change_in_num_orders,
                "TPCH",
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
                "yoy_change_in_num_orders",
            ),
            id="yoy_change_in_num_orders",
        ),
        pytest.param(
            PyDoughPandasTest(
                first_order_in_year,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "order_date": [f"{yr}-01-01" for yr in range(1992, 1999)],
                        "key": [3271, 15233, 290, 14178, 4640, 5895, 20064],
                    }
                ),
                "first_order_in_year",
            ),
            id="first_order_in_year",
        ),
        pytest.param(
            PyDoughPandasTest(
                customer_largest_order_deltas,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000046238",
                            "Customer#000056794",
                            "Customer#000079627",
                            "Customer#000085429",
                            "Customer#000092803",
                        ],
                        "largest_diff": [
                            201862.1534,
                            208623.953,
                            201089.5987,
                            274455.2228,
                            204431.6412,
                        ],
                    }
                ),
                "customer_largest_order_deltas",
            ),
            id="customer_largest_order_deltas",
        ),
        pytest.param(
            PyDoughPandasTest(
                suppliers_bal_diffs,
                "TPCH",
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
                "suppliers_bal_diffs",
            ),
            id="suppliers_bal_diffs",
        ),
        pytest.param(
            PyDoughPandasTest(
                month_year_sliding_windows,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "year": [1994] * 5 + [1996] * 4 + [1997] * 2 + [1998] * 4,
                        "month": [1, 3, 5, 8, 12, 4, 7, 10, 12, 7, 10, 1, 3, 5, 7],
                    }
                ),
                "month_year_sliding_windows",
            ),
            id="month_year_sliding_windows",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "nation_4_name": [None, None, None, None, "EGYPT"],
                    }
                ),
                "singular1",
            ),
            id="singular1",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular2,
                "TPCH",
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
                "singular2",
            ),
            id="singular2",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular3,
                "TPCH",
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
                "singular3",
            ),
            id="singular3",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular4,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "Customer#000047056",
                            "Customer#000019210",
                            "Customer#000094175",
                            "Customer#000012947",
                            "Customer#000139547",
                        ]
                    }
                ),
                "singular4",
            ),
            id="singular4",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular5,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "container": [
                            "LG CASE",
                            "WRAP CAN",
                            "SM BAG",
                            "SM PKG",
                            "LG BOX",
                        ],
                        "highest_price_ship_date": [
                            "1992-08-10",
                            "1992-08-28",
                            "1992-09-22",
                            "1992-10-27",
                            "1992-10-31",
                        ],
                    }
                ),
                "singular5",
                order_sensitive=True,
            ),
            id="singular5",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular6,
                "TPCH",
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
                "singular6",
            ),
            id="singular6",
        ),
        pytest.param(
            PyDoughPandasTest(
                singular7,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000008210",
                            "Supplier#000002891",
                            "Supplier#000008529",
                            "Supplier#000009337",
                            "Supplier#000000114",
                        ],
                        "part_name": [
                            "navajo azure ivory rosy gainsboro",
                            "magenta firebrick puff moccasin drab",
                            "deep midnight hot white sky",
                            "cornflower dark steel burlywood salmon",
                            "smoke navajo spring cream cornflower",
                        ],
                        "n_orders": [7, 5, 5, 5, 4],
                    }
                ),
                "singular7",
                order_sensitive=True,
            ),
            id="singular7",
        ),
        pytest.param(
            PyDoughPandasTest(
                parts_quantity_increase_95_96,
                "TPCH",
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
                "parts_quantity_increase_95_96",
            ),
            id="parts_quantity_increase_95_96",
        ),
        pytest.param(
            PyDoughPandasTest(
                avg_gap_prev_urgent_same_clerk,
                "TPCH",
                lambda: pd.DataFrame({"avg_delta": [7.9820674]}),
                "avg_gap_prev_urgent_same_clerk",
            ),
            id="avg_gap_prev_urgent_same_clerk",
        ),
        pytest.param(
            PyDoughPandasTest(
                top_customers_by_orders,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "customer_key": [3451, 102004, 102022, 79300, 117082],
                        "n_orders": [41, 41, 41, 40, 40],
                    }
                ),
                "top_customers_by_orders",
            ),
            id="top_customers_by_orders",
        ),
        pytest.param(
            PyDoughPandasTest(
                customer_most_recent_orders,
                "TPCH",
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
                "customer_most_recent_orders",
            ),
            id="customer_most_recent_orders",
        ),
        pytest.param(
            PyDoughPandasTest(
                richest_customer_per_region,
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
                "richest_customer_per_region",
            ),
            id="richest_customer_per_region",
        ),
        pytest.param(
            PyDoughPandasTest(
                n_orders_first_day,
                "TPCH",
                lambda: pd.DataFrame({"n_orders": [621]}),
                "n_orders_first_day",
            ),
            id="n_orders_first_day",
        ),
        pytest.param(
            PyDoughPandasTest(
                wealthiest_supplier,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["Supplier#000009450"],
                        "account_balance": [9999.72],
                    }
                ),
                "wealthiest_supplier",
            ),
            id="wealthiest_supplier",
        ),
        pytest.param(
            PyDoughPandasTest(
                supplier_best_part,
                "TPCH",
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
                "supplier_best_part",
            ),
            id="supplier_best_part",
        ),
        pytest.param(
            PyDoughPandasTest(
                region_orders_from_nations_richest,
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
                        "n_orders": [74, 19, 62, 73, 41],
                    }
                ),
                "region_orders_from_nations_richest",
            ),
            id="region_orders_from_nations_richest",
        ),
        pytest.param(
            PyDoughPandasTest(
                regional_first_order_best_line_part,
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
                        "part_name": [
                            "tomato saddle brown cornsilk khaki",
                            "coral midnight cyan burlywood maroon",
                            "azure peru burnished seashell green",
                            "ivory peach linen lemon powder",
                            "cyan sienna ivory powder forest",
                        ],
                    }
                ),
                "regional_first_order_best_line_part",
            ),
            id="regional_first_order_best_line_part",
        ),
        pytest.param(
            PyDoughPandasTest(
                orders_versus_first_orders,
                "TPCH",
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
                "orders_versus_first_orders",
            ),
            id="orders_versus_first_orders",
        ),
        pytest.param(
            PyDoughPandasTest(
                nation_window_aggs,
                "TPCH",
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
                "nation_window_aggs",
            ),
            id="nation_window_aggs",
        ),
        pytest.param(
            PyDoughPandasTest(
                region_nation_window_aggs,
                "TPCH",
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
                "region_nation_window_aggs",
            ),
            id="region_nation_window_aggs",
        ),
        pytest.param(
            PyDoughPandasTest(
                supplier_pct_national_qty,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_name": [
                            "Supplier#000002367",
                            "Supplier#000003027",
                            "Supplier#000004494",
                            "Supplier#000005363",
                            "Supplier#000005639",
                        ],
                        "nation_name": [
                            "ALGERIA",
                            "ALGERIA",
                            "ALGERIA",
                            "MOROCCO",
                            "KENYA",
                        ],
                        "supplier_quantity": [11, 23, 17, 24, 32],
                        "national_qty_pct": [
                            15.068493150684931,
                            31.506849315068493,
                            23.28767123287671,
                            100.0,
                            100.0,
                        ],
                    }
                ),
                "supplier_pct_national_qty",
            ),
            id="supplier_pct_national_qty",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " regions"
                " .nations"
                " .customers"
                " .BEST(by=account_balance.DESC(), per='regions')"
                " .CALCULATE(key)"
                ")",
                "TPCH",
                lambda: pd.DataFrame({"key": [2487, 61453, 76011, 81976, 144232]}),
                "richest_customer_key_per_region",
            ),
            id="richest_customer_key_per_region",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " lines"
                " .TOP_K(7, by=(order_key.ASC(), line_number.ASC()))"
                " .CALCULATE(order_key, line_number, part_size=part_and_supplier.part.size, supplier_nation=part_and_supplier.supplier.nation.key)"
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "order_key": [1, 1, 1, 1, 1, 1, 2],
                        "line_number": [1, 2, 3, 4, 5, 6, 1],
                        "part_size": [9, 47, 16, 20, 44, 46, 19],
                        "supplier_nation": [23, 13, 5, 24, 20, 8, 0],
                    }
                ),
                "top_lineitems_info_1",
            ),
            id="top_lineitems_info_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " parts"
                " .CALCULATE(part_size=size, selected_part_key=key)"
                " .supply_records.CALCULATE(selected_supplier_key=supplier_key)"
                " .CROSS(nations.CALCULATE(supplier_nation=key).suppliers.supply_records.lines)"
                " .WHERE((part_key == selected_part_key) & (supplier_key == selected_supplier_key))"
                " .TOP_K(7, by=(order_key.ASC(), line_number.ASC()))"
                " .CALCULATE(order_key, line_number, part_size, supplier_nation)"
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "order_key": [1, 1, 1, 1, 1, 1, 2],
                        "line_number": [1, 2, 3, 4, 5, 6, 1],
                        "part_size": [9, 47, 16, 20, 44, 46, 19],
                        "supplier_nation": [23, 13, 5, 24, 20, 8, 0],
                    }
                ),
                "top_lineitems_info_2",
            ),
            id="top_lineitems_info_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key).WHERE(nation_key == 1)"
                " .nation"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [18],
                    }
                ),
                "many_net_filter_1",
            ),
            id="many_net_filter_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation.WHERE(key == 2)"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [10],
                    }
                ),
                "many_net_filter_2",
            ),
            id="many_net_filter_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation"
                " .customers.WHERE(nation_key == 3)"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [14],
                    }
                ),
                "many_net_filter_3",
            ),
            id="many_net_filter_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key).WHERE(nation_key == 4)"
                " .nation"
                " .region"
                " .nations"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [88],
                    }
                ),
                "many_net_filter_4",
            ),
            id="many_net_filter_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation.WHERE(key == 5)"
                " .region"
                " .nations"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [81],
                    }
                ),
                "many_net_filter_5",
            ),
            id="many_net_filter_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation"
                " .region"
                " .nations.WHERE(key == 6)"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [77],
                    }
                ),
                "many_net_filter_6",
            ),
            id="many_net_filter_6",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation"
                " .region"
                " .nations"
                " .customers.WHERE(nation_key == 7)"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [81],
                    }
                ),
                "many_net_filter_7",
            ),
            id="many_net_filter_7",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation.WHERE(region_key == 0)"
                " .region"
                " .nations"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [403],
                    }
                ),
                "many_net_filter_8",
            ),
            id="many_net_filter_8",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation"
                " .region.WHERE(key == 1)"
                " .nations"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [399],
                    }
                ),
                "many_net_filter_9",
            ),
            id="many_net_filter_9",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key)"
                " .nation"
                " .region"
                " .nations.WHERE(region_key == 2)"
                " .customers"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [401],
                    }
                ),
                "many_net_filter_10",
            ),
            id="many_net_filter_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT("
                " suppliers.CALCULATE(sk = key).WHERE(~ISIN(nation_key, list(range(0, 25, 3))))"
                " .nation.WHERE(region_key < 3)"
                " .region"
                " .nations.WHERE(region_key > 0)"
                " .customers.WHERE(~ISIN(nation_key, list(range(1, 25, 3))))"
                " .WHERE(key == sk)"
                "))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [269],
                    }
                ),
                "many_net_filter_11",
            ),
            id="many_net_filter_11",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_1,
                "TPCH",
                lambda: pd.DataFrame({"n": [969]}),
                "window_filter_order_1",
            ),
            id="window_filter_order_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_2,
                "TPCH",
                lambda: pd.DataFrame({"n": [969]}),
                "window_filter_order_2",
            ),
            id="window_filter_order_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_3,
                "TPCH",
                lambda: pd.DataFrame({"n": [969]}),
                "window_filter_order_3",
            ),
            id="window_filter_order_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_4,
                "TPCH",
                lambda: pd.DataFrame({"n": [1936]}),
                "window_filter_order_4",
            ),
            id="window_filter_order_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_5,
                "TPCH",
                lambda: pd.DataFrame({"n": [8229]}),
                "window_filter_order_5",
            ),
            id="window_filter_order_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_6,
                "TPCH",
                lambda: pd.DataFrame({"n": [8229]}),
                "window_filter_order_6",
            ),
            id="window_filter_order_6",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_7,
                "TPCH",
                lambda: pd.DataFrame({"n": [22984]}),
                "window_filter_order_7",
            ),
            id="window_filter_order_7",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_8,
                "TPCH",
                lambda: pd.DataFrame({"n": [906]}),
                "window_filter_order_8",
            ),
            id="window_filter_order_8",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_9,
                "TPCH",
                lambda: pd.DataFrame({"n": [525]}),
                "window_filter_order_9",
            ),
            id="window_filter_order_9",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_filter_order_10,
                "TPCH",
                lambda: pd.DataFrame({"n": [0]}),
                "window_filter_order_10",
            ),
            id="window_filter_order_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                highest_priority_per_year,
                "TPCH",
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
                "highest_priority_per_year",
            ),
            id="highest_priority_per_year",
        ),
        pytest.param(
            PyDoughPandasTest(
                nation_best_order,
                "TPCH",
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
                "nation_best_order",
            ),
            id="nation_best_order",
        ),
        pytest.param(
            PyDoughPandasTest(
                nation_acctbal_breakdown,
                "TPCH",
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
                "nation_acctbal_breakdown",
            ),
            id="nation_acctbal_breakdown",
        ),
        pytest.param(
            PyDoughPandasTest(
                region_acctbal_breakdown,
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
                "region_acctbal_breakdown",
            ),
            id="region_acctbal_breakdown",
        ),
        pytest.param(
            PyDoughPandasTest(
                global_acctbal_breakdown,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n_red_acctbal": [13692],
                        "n_black_acctbal": [136308],
                        "median_red_acctbal": [-496.59],
                        "median_black_acctbal": [4988.755],
                        "median_overall_acctbal": [4477.3],
                    }
                ),
                "global_acctbal_breakdown",
            ),
            id="global_acctbal_breakdown",
        ),
        pytest.param(
            PyDoughPandasTest(
                extract_colors,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 6)),
                        "c1": ["GOLDENROD", "BLUSH", "SPRING", "CORNFLOWER", "FOREST"],
                        "c2": ["LAVENDER", "THISTLE", "GREEN", "CHOCOLATE", "BROWN"],
                        "c3": ["SPRING", "BLUE", "YELLOW", "SMOKE", "CORAL"],
                        "c4": ["CHOCOLATE", "YELLOW", "PURPLE", "GREEN", "PUFF"],
                        "c5": ["LACE", "SADDLE", "CORNSILK", "PINK", "CREAM"],
                        "c6": [None] * 5,
                    }
                ),
                "extract_colors",
            ),
            id="extract_colors",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_int_float_string_cast,
                "TPCH",
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
                        "s6": ["-6.1"],
                        "s7": ["0.1"],
                        "s8": ["0.0"],
                        "s9": ["abc def"],
                    }
                ),
                "simple_int_float_string_cast",
            ),
            id="simple_int_float_string_cast",
        ),
        pytest.param(
            PyDoughPandasTest(
                string_format_specifiers_sqlite,
                "TPCH",
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
                "string_format_specifiers",
            ),
            id="string_format_specifiers",
        ),
        pytest.param(
            PyDoughPandasTest(
                part_reduced_size,
                "TPCH",
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
                "part_reduced_size",
            ),
            id="part_reduced_size",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_smallest_or_largest,
                "TPCH",
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
                "simple_smallest_or_largest",
            ),
            id="simple_smallest_or_largest",
        ),
        pytest.param(
            PyDoughPandasTest(
                avg_acctbal_wo_debt,
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
                        "avg_bal_without_debt_erasure": [
                            4547.554121,
                            4536.852848,
                            4548.741422,
                            4539.072249,
                            4533.254352,
                        ],
                    },
                ),
                "avg_acctbal_wo_debt",
            ),
            id="avg_acctbal_wo_debt",
        ),
        pytest.param(
            PyDoughPandasTest(
                odate_and_rdate_avggap,
                "TPCH",
                lambda: pd.DataFrame({"avg_gap": [50.41427]}),
                "odate_and_rdate_avggap",
            ),
            id="odate_and_rdate_avggap",
        ),
        pytest.param(
            PyDoughPandasTest(
                dumb_aggregation,
                "TPCH",
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
                "dumb_aggregation",
            ),
            id="dumb_aggregation",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "r1": ["AFRICA"] * 5
                        + ["AMERICA"] * 5
                        + ["ASIA"] * 5
                        + ["EUROPE"] * 5
                        + ["MIDDLE EAST"] * 5,
                        "r2": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
                        * 5,
                    }
                ),
                "simple_cross_1",
            ),
            id="simple_cross_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                deep_best_analysis,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "r_name": [
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "AMERICA",
                            "ASIA",
                            "MIDDLE EAST",
                            "AFRICA",
                            "EUROPE",
                            "EUROPE",
                            "ASIA",
                        ],
                        "n_name": [
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
                        ],
                        "c_key": [
                            34047,
                            12437,
                            43044,
                            112796,
                            100214,
                            115942,
                            129934,
                            96205,
                            144232,
                            148812,
                        ],
                        "c_bal": [
                            9998.97,
                            9994.84,
                            9999.49,
                            9998.32,
                            9997.73,
                            9996.32,
                            9999.59,
                            9998.86,
                            9999.74,
                            9998.32,
                        ],
                        "cr_bal": [None] * 8 + [9999.74, None],
                        "s_key": [None] * 7 + [3836] + [None] * 2,
                        "p_key": [None] * 7 + [18833] + [None] * 2,
                        "p_qty": [None] * 7 + [9999] + [None] * 2,
                        "cg_key": [None] * 10,
                    }
                ),
                "deep_best_analysis",
                order_sensitive=True,
            ),
            id="deep_best_analysis",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "r1": ["AFRICA"] * 4
                        + ["AMERICA"] * 4
                        + ["ASIA"] * 4
                        + ["EUROPE"] * 4
                        + ["MIDDLE EAST"] * 4,
                        "r2": ["AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"]
                        + ["AFRICA", "ASIA", "EUROPE", "MIDDLE EAST"]
                        + ["AFRICA", "AMERICA", "EUROPE", "MIDDLE EAST"]
                        + ["AFRICA", "AMERICA", "ASIA", "MIDDLE EAST"]
                        + ["AFRICA", "AMERICA", "ASIA", "EUROPE"],
                    }
                ),
                "simple_cross_2",
            ),
            id="simple_cross_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_nation": ["INDIA"] * 5
                        + ["INDONESIA"] * 4
                        + ["JAPAN"] * 3
                        + ["CHINA"] * 5
                        + ["VIETNAM"] * 4,
                        "customer_nation": [
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                        ]
                        + ["ARGENTINA", "BRAZIL", "CANADA", "PERU"]
                        + ["CANADA", "PERU", "UNITED STATES"]
                        + ["ARGENTINA", "BRAZIL", "CANADA", "PERU", "UNITED STATES"]
                        + ["ARGENTINA", "BRAZIL", "CANADA", "UNITED STATES"],
                        "nation_combinations": [2, 1, 1, 2, 1]
                        + [2, 3, 2, 2]
                        + [2, 1, 2]
                        + [5, 1, 3, 3, 2]
                        + [4, 1, 3, 2],
                    }
                ),
                "simple_cross_3",
            ),
            id="simple_cross_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_4,
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
                        "n_other_regions": [2, 2, 2, 0, 0],
                    }
                ),
                "simple_cross_4",
            ),
            id="simple_cross_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_5,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_size": range(1, 11),
                        "best_order_priority": [
                            "4-NOT SPECIFIED",
                            None,
                            None,
                            "1-URGENT",
                            "3-MEDIUM",
                            None,
                            "4-NOT SPECIFIED",
                            "2-HIGH",
                            None,
                            None,
                        ],
                        "best_order_priority_qty": [
                            50,
                            None,
                            None,
                            33,
                            12,
                            None,
                            3,
                            34,
                            None,
                            None,
                        ],
                    }
                ),
                "simple_cross_5",
            ),
            id="simple_cross_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_6,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n_pairs": [100],
                    }
                ),
                "simple_cross_6",
            ),
            id="simple_cross_6",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_7,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "original_part_key": [12850, 7635, 14848, 51810, 914],
                        "n_other_parts": [9, 8, 8, 8, 7],
                    }
                ),
                "simple_cross_7",
            ),
            id="simple_cross_7",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_8,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "supplier_region": ["ASIA", "MIDDLE EAST"],
                        "customer_region": ["AMERICA", "MIDDLE EAST"],
                        "region_combinations": [1] * 2,
                    }
                ),
                "simple_cross_8",
            ),
            id="simple_cross_8",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_9,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": ["ALGERIA"] * 4 + ["ARGENTINA"] * 4 + ["BRAZIL"] * 2,
                        "n2": [
                            "ETHIOPIA",
                            "KENYA",
                            "MOROCCO",
                            "MOZAMBIQUE",
                            "BRAZIL",
                            "CANADA",
                            "PERU",
                            "UNITED STATES",
                            "ARGENTINA",
                            "CANADA",
                        ],
                    }
                ),
                "simple_cross_9",
            ),
            id="simple_cross_9",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_10,
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
                        "n_other_nations": [1, 1, 2, 2, 2],
                    }
                ),
                "simple_cross_10",
            ),
            id="simple_cross_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_11,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [621],
                    }
                ),
                "simple_cross_11",
            ),
            id="simple_cross_11",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_12,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "order_priorities": ["1-URGENT"] * 5
                        + ["2-HIGH"] * 5
                        + ["3-MEDIUM"] * 5
                        + ["4-NOT SPECIFIED"] * 5
                        + ["5-LOW"] * 5,
                        "market_segments": [
                            "AUTOMOBILE",
                            "BUILDING",
                            "FURNITURE",
                            "HOUSEHOLD",
                            "MACHINERY",
                        ]
                        * 5,
                    }
                ),
                "simple_cross_12",
            ),
            id="simple_cross_12",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_13,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "a": ["foo"],
                        "b": ["bar"],
                        "c": ["fizz"],
                        "d": ["buzz"],
                        "e": ["foobar"],
                        "f": ["fizzbuzz"],
                        "g": ["yay"],
                    }
                ),
                "simple_cross_13",
            ),
            id="simple_cross_13",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_14,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "x": ["foo"] * 5,
                        "n": [1, 3, 1, 0, 0],
                    }
                ),
                "simple_cross_14",
            ),
            id="simple_cross_14",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_15,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "a": ["*"] * 8 + ["A"] * 8,
                        "e": list("****EEEE") * 2,
                        "i": list("**II") * 4,
                        "o": list("*O") * 8,
                    }
                ),
                "simple_cross_15",
            ),
            id="simple_cross_15",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_cross_16,
                "TPCH",
                lambda: pd.DataFrame({"n1": [142], "n2": [8]}),
                "simple_cross_16",
            ),
            id="simple_cross_16",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_var_std,
                "TPCH",
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
                "simple_var_std",
            ),
            id="simple_var_std",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_var_std_with_nulls,
                "TPCH",
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
                "simple_var_std_with_nulls",
            ),
            id="simple_var_std_with_nulls",
        ),
        pytest.param(
            PyDoughPandasTest(
                quarter_function_test,
                "TPCH",
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
                        "plus_3q": ["2023-10-15"],
                        "minus_1q": ["2022-10-15 12:30:45"],
                        "minus_2q": ["2022-07-15 12:30:45"],
                        "minus_3q": ["2022-04-15"],
                        "syntax1": ["2023-08-15"],
                        "syntax2": ["2024-02-15"],
                        "syntax3": ["2024-08-15"],
                        "syntax4": ["2022-08-15"],
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
                "quarter_function_test",
            ),
            id="quarter_function_test",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE("
                " n1=COUNT(customers.WHERE(MONOTONIC(500, account_balance, 600))), "
                " n2=COUNT(customers.WHERE((market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))), "
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1379],
                        "n2": [268],
                    }
                ),
                "count_multiple_filters_a",
                skip_sql=True,
            ),
            id="count_multiple_filters_a",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = customers.WHERE(MONOTONIC(500, account_balance, 600))\n"
                "c2 = customers.WHERE((market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))\n"
                "result = TPCH.CALCULATE("
                " n1=COUNT(c1), "
                " n2=COUNT(c2), "
                ").WHERE(HAS(c1))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1379],
                        "n2": [268],
                    }
                ),
                "count_multiple_filters_b",
                skip_sql=True,
            ),
            id="count_multiple_filters_b",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = customers.WHERE(MONOTONIC(500, account_balance, 600))\n"
                "c2 = customers.WHERE((market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))\n"
                "result = TPCH.CALCULATE("
                " n1=COUNT(c1), "
                " n2=COUNT(c2), "
                ").WHERE(HAS(c2))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1379],
                        "n2": [268],
                    }
                ),
                "count_multiple_filters_c",
                skip_sql=True,
            ),
            id="count_multiple_filters_c",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = customers.WHERE(MONOTONIC(500, account_balance, 600))\n"
                "c2 = customers.WHERE((market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))\n"
                "result = TPCH.CALCULATE("
                " n1=COUNT(c1), "
                " n2=COUNT(c2), "
                ").WHERE(HAS(c1) & HAS(c2))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1379],
                        "n2": [268],
                    }
                ),
                "count_multiple_filters_d",
                skip_sql=True,
            ),
            id="count_multiple_filters_d",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE("
                " n1=COUNT(customers.WHERE(MONOTONIC(500, account_balance, 600))), "
                " n2=COUNT(customers.WHERE(market_segment == 'BUILDING')), "
                " n3=COUNT(customers.WHERE((market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))), "
                " n4=COUNT(customers.WHERE(MONOTONIC(500, account_balance, 600) & STARTSWITH(phone, '11'))), "
                " n5=COUNT(customers.WHERE(STARTSWITH(phone, '11') & (market_segment == 'BUILDING'))), "
                " n6=COUNT(customers.WHERE(MONOTONIC(500, account_balance, 600) & STARTSWITH(phone, '11') & (market_segment == 'BUILDING'))), "
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1379],
                        "n2": [30142],
                        "n3": [268],
                        "n4": [54],
                        "n5": [1261],
                        "n6": [19],
                    }
                ),
                "count_multiple_filters_e",
                skip_sql=True,
            ),
            id="count_multiple_filters_e",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE("
                " n1=COUNT(customers), "
                " n2=COUNT(customers.WHERE(market_segment == 'BUILDING')), "
                " n3=COUNT(customers.WHERE(MONOTONIC(500, account_balance, 600))), "
                " n4=COUNT(customers.WHERE(STARTSWITH(phone, '11'))), "
                " n5=COUNT(customers.WHERE(STARTSWITH(phone, '11') & (market_segment == 'BUILDING'))), "
                " n6=COUNT(customers.WHERE(STARTSWITH(phone, '11') & (market_segment == 'BUILDING') & MONOTONIC(500, account_balance, 600))), "
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [150000],
                        "n2": [30142],
                        "n3": [1379],
                        "n4": [5975],
                        "n5": [1261],
                        "n6": [19],
                    }
                ),
                "count_multiple_filters_f",
                skip_sql=True,
            ),
            id="count_multiple_filters_f",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = customers.WHERE(PERCENTILE(by=account_balance.ASC()) == 100)\n"
                "c2 = customers.WHERE(nation.name == 'GERMANY').WHERE(PERCENTILE(by=account_balance.ASC()) == 100)\n"
                "c3 = customers.WHERE(nation.name == 'GERMANY')\n"
                "c4 = customers.WHERE(nation.name == 'CHINA').WHERE(PERCENTILE(by=account_balance.ASC()) == 100)\n"
                "c5 = customers.WHERE((PERCENTILE(by=account_balance.ASC()) == 100) & (nation.name == 'CHINA'))\n"
                "c6 = customers.WHERE(nation.name == 'CHINA')\n"
                "c6 = customers.WHERE(nation.name == 'CHINA')\n"
                "result = TPCH.CALCULATE("
                " n1=COUNT(c1), "
                " n2=COUNT(c2), "
                " n3=COUNT(c3), "
                " n4=COUNT(c4), "
                " n5=COUNT(c5), "
                " n6=COUNT(c6), "
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n1": [1500],
                        "n2": [59],
                        "n3": [5908],
                        "n4": [60],
                        "n5": [57],
                        "n6": [6024],
                    }
                ),
                "count_multiple_filters_g",
                skip_sql=True,
            ),
            id="count_multiple_filters_g",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(nations.customers.orders), "
                " n3=COUNT(nations.customers.orders.WHERE(order_priority == '1-URGENT')), "
                " n4=COUNT(nations.customers.orders.WHERE(order_priority == '2-HIGH')), "
                " n5=COUNT(nations.customers.orders.WHERE(order_priority == '3-MEDIUM')), "
                ")",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [298994, 299103, 301740, 303286, 296877],
                        "n3": [59767, 59902, 60166, 60373, 60135],
                        "n4": [59511, 60232, 60246, 60901, 59201],
                        "n5": [59597, 59230, 60485, 60375, 59036],
                    }
                ),
                "count_multiple_filters_h",
                skip_sql=True,
            ),
            id="count_multiple_filters_h",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(nations.customers.orders), "
                " n3=COUNT(nations.customers.orders.WHERE(order_priority == '1-URGENT')), "
                " n4=COUNT(nations.customers.orders.WHERE(order_priority == '2-HIGH')), "
                " n5=COUNT(nations.customers.orders.WHERE(order_priority == '3-MEDIUM')), "
                ").WHERE(HAS(nations.customers.orders.WHERE(order_priority == '2-HIGH')))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [298994, 299103, 301740, 303286, 296877],
                        "n3": [59767, 59902, 60166, 60373, 60135],
                        "n4": [59511, 60232, 60246, 60901, 59201],
                        "n5": [59597, 59230, 60485, 60375, 59036],
                    }
                ),
                "count_multiple_filters_i",
                skip_sql=True,
            ),
            id="count_multiple_filters_i",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(nations.customers.orders.WHERE(order_priority == '1-URGENT')), "
                " n3=COUNT(nations.customers.orders.WHERE(order_priority == '2-HIGH')), "
                " n4=COUNT(nations.customers.orders.WHERE(order_priority == '3-MEDIUM')), "
                ")",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [59767, 59902, 60166, 60373, 60135],
                        "n3": [59511, 60232, 60246, 60901, 59201],
                        "n4": [59597, 59230, 60485, 60375, 59036],
                    }
                ),
                "count_multiple_filters_j",
                skip_sql=True,
            ),
            id="count_multiple_filters_j",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(nations.customers.orders.WHERE(order_priority == '1-URGENT')), "
                " n3=COUNT(nations.customers.orders.WHERE(order_priority == '2-HIGH')), "
                " n4=COUNT(nations.customers.orders.WHERE(order_priority == '3-MEDIUM')), "
                ").WHERE(HAS(nations.customers.orders.WHERE(order_priority == '1-URGENT')))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [59767, 59902, 60166, 60373, 60135],
                        "n3": [59511, 60232, 60246, 60901, 59201],
                        "n4": [59597, 59230, 60485, 60375, 59036],
                    }
                ),
                "count_multiple_filters_k",
                skip_sql=True,
            ),
            id="count_multiple_filters_k",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(nations.customers.orders.WHERE((order_priority == '1-URGENT') | (order_priority == '2-HIGH'))), "
                " n3=COUNT(nations.customers.orders.WHERE((order_priority == '2-HIGH') | (order_priority == '3-MEDIUM'))), "
                " n4=COUNT(nations.customers.orders.WHERE((order_priority == '3-MEDIUM') | (order_priority == '4-NOT SPECIFIED'))), "
                ")",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [119278, 120134, 120412, 121274, 119336],
                        "n3": [119108, 119462, 120731, 121276, 118237],
                        "n4": [119665, 119193, 121015, 121129, 117975],
                    }
                ),
                "count_multiple_filters_l",
                skip_sql=True,
            ),
            id="count_multiple_filters_l",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = nations.customers.orders.WHERE((order_priority == '1-URGENT') | (order_priority == '2-HIGH'))\n"
                "c2 = nations.customers.orders.WHERE((order_priority == '2-HIGH') | (order_priority == '3-MEDIUM'))\n"
                "c3 = nations.customers.orders.WHERE((order_priority == '3-MEDIUM') | (order_priority == '4-NOT SPECIFIED'))\n"
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(c1), "
                " n3=COUNT(c2), "
                " n4=COUNT(c3), "
                ").WHERE(HAS(c1))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [119278, 120134, 120412, 121274, 119336],
                        "n3": [119108, 119462, 120731, 121276, 118237],
                        "n4": [119665, 119193, 121015, 121129, 117975],
                    }
                ),
                "count_multiple_filters_m",
                skip_sql=True,
            ),
            id="count_multiple_filters_m",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = nations.customers.orders.WHERE((order_priority == '1-URGENT') | (order_priority == '2-HIGH'))\n"
                "c2 = nations.customers.orders.WHERE((order_priority == '2-HIGH') | (order_priority == '3-MEDIUM'))\n"
                "c3 = nations.customers.orders.WHERE((order_priority == '3-MEDIUM') | (order_priority == '4-NOT SPECIFIED'))\n"
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(c1), "
                " n3=COUNT(c2), "
                " n4=COUNT(c3), "
                ").WHERE(HAS(c1) & HAS(c2))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [119278, 120134, 120412, 121274, 119336],
                        "n3": [119108, 119462, 120731, 121276, 118237],
                        "n4": [119665, 119193, 121015, 121129, 117975],
                    }
                ),
                "count_multiple_filters_n",
                skip_sql=True,
            ),
            id="count_multiple_filters_n",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = nations.customers.orders.WHERE((order_priority == '1-URGENT') | (order_priority == '2-HIGH'))\n"
                "c2 = nations.customers.orders.WHERE((order_priority == '2-HIGH') | (order_priority == '3-MEDIUM'))\n"
                "c3 = nations.customers.orders.WHERE((order_priority == '3-MEDIUM') | (order_priority == '4-NOT SPECIFIED'))\n"
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(c1), "
                " n3=COUNT(c2), "
                " n4=COUNT(c3), "
                ").WHERE(HAS(c1) & HAS(c3))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [119278, 120134, 120412, 121274, 119336],
                        "n3": [119108, 119462, 120731, 121276, 118237],
                        "n4": [119665, 119193, 121015, 121129, 117975],
                    }
                ),
                "count_multiple_filters_o",
                skip_sql=True,
            ),
            id="count_multiple_filters_o",
        ),
        pytest.param(
            PyDoughPandasTest(
                "c1 = nations.customers.orders.WHERE((order_priority == '1-URGENT') | (order_priority == '2-HIGH'))\n"
                "c2 = nations.customers.orders.WHERE((order_priority == '2-HIGH') | (order_priority == '3-MEDIUM'))\n"
                "c3 = nations.customers.orders.WHERE((order_priority == '3-MEDIUM') | (order_priority == '4-NOT SPECIFIED'))\n"
                "result = regions.CALCULATE("
                " region_name=name, "
                " n1=COUNT(nations.customers), "
                " n2=COUNT(c1), "
                " n3=COUNT(c2), "
                " n4=COUNT(c3), "
                ").WHERE(HAS(c1) & HAS(c2) & HAS(c3))",
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
                        "n1": [29764, 29952, 30183, 30197, 29904],
                        "n2": [119278, 120134, 120412, 121274, 119336],
                        "n3": [119108, 119462, 120731, 121276, 118237],
                        "n4": [119665, 119193, 121015, 121129, 117975],
                    }
                ),
                "count_multiple_filters_p",
                skip_sql=True,
            ),
            id="count_multiple_filters_p",
        ),
        pytest.param(
            PyDoughPandasTest(
                order_quarter_test,
                "TPCH",
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
                "order_quarter_test",
            ),
            id="order_quarter_test",
        ),
        pytest.param(
            PyDoughPandasTest(
                double_cross,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "wk": list(range(1, 10)),
                        "n_lines": [9, 23, 58, 143, 195, 274, 348, 393, 503],
                        "n_orders": [891, 847, 870, 918, 893, 850, 854, 863, 824],
                        "lpo": [
                            0.0101,
                            0.0184,
                            0.0345,
                            0.0661,
                            0.0969,
                            0.1332,
                            0.1715,
                            0.2066,
                            0.2492,
                        ],
                    }
                ),
                "double_cross",
            ),
            id="double_cross",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(customers.WHERE(HAS(nation.WHERE(region.name == 'ASIA')))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [30183],
                    }
                ),
                "redundant_has",
            ),
            id="redundant_has",
        ),
        # Nested HAS on singular chain (supplier -> nation -> region), both should optimize to INNER
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(suppliers.WHERE(HAS(nation.WHERE(HAS(region.WHERE(name == 'AFRICA')))))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [1955],
                    }
                ),
                "redundant_has_nested",
            ),
            id="redundant_has_nested",
        ),
        # HAS on plural relationship (orders) - should NOT optimize, stays SEMI
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(customers.WHERE(HAS(orders.WHERE(total_price > 400000)))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [3533],
                    }
                ),
                "redundant_has_on_plural",
            ),
            id="redundant_has_on_plural",
        ),
        # HAS on singular relationship with additional filter
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(suppliers.WHERE(HAS(nation.WHERE(region.name == 'EUROPE')))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [1987],
                    }
                ),
                "redundant_has_singular_chain",
            ),
            id="redundant_has_singular_chain",
        ),
        # HAS on plural relationship (lineitems) - should NOT optimize, stays SEMI
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(orders.WHERE(HAS(lines.WHERE(quantity > 49)))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [115066],
                    }
                ),
                "redundant_has_on_plural_lineitems",
            ),
            id="redundant_has_on_plural_lineitems",
        ),
        # HASNOT on singular relationship - should optimize to ANTI join or similar
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(suppliers.WHERE(HASNOT(nation.WHERE(region.name == 'AFRICA')))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [8045],
                    }
                ),
                "redundant_has_not_on_singular",
                skip_relational=True,
                skip_sql=True,
            ),
            id="redundant_has_not_on_singular",
        ),
        # HAS without WHERE filter on singular - should optimize to INNER
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(customers.WHERE(HAS(nation))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [150000],
                    }
                ),
                "redundant_has_no_filter_singular",
                skip_relational=True,
                skip_sql=True,
            ),
            id="redundant_has_no_filter_singular",
        ),
        # HAS on singular within plural context - orders whose customer is from ASIA
        pytest.param(
            PyDoughPandasTest(
                "result = TPCH.CALCULATE(n=COUNT(orders.WHERE(HAS(customer.WHERE(nation.region.name == 'ASIA')))))",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "n": [301740],
                    }
                ),
                "redundant_has_singular_in_plural_context",
                skip_relational=True,
                skip_sql=True,
            ),
            id="redundant_has_singular_in_plural_context",
        ),
        pytest.param(
            PyDoughPandasTest(
                bad_child_reuse_1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [15980, 23828, 61453, 63655, 129934, 144232],
                        "n_orders": [8, 8, 29, 15, 22, 16],
                    }
                ),
                "bad_child_reuse_1",
            ),
            id="bad_child_reuse_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                bad_child_reuse_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [
                            15980,
                            23828,
                            27412,
                            39133,
                            61453,
                            63655,
                            96205,
                            129934,
                            144232,
                            148504,
                        ],
                        "n_orders": [8, 8, 17, 28, 29, 15, 21, 22, 16, 15],
                        "n_cust": [
                            5974,
                            5974,
                            5983,
                            5992,
                            5921,
                            6100,
                            6100,
                            5952,
                            5908,
                            5992,
                        ],
                    }
                ),
                "bad_child_reuse_2",
            ),
            id="bad_child_reuse_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                bad_child_reuse_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [
                            15980,
                            23828,
                            27412,
                            39133,
                            61453,
                            63655,
                            96205,
                            129934,
                            144232,
                            148504,
                        ],
                        "n_orders": [8, 8, 17, 28, 29, 15, 21, 22, 16, 15],
                        "n_cust": [
                            5974,
                            5974,
                            5983,
                            5992,
                            5921,
                            6100,
                            6100,
                            5952,
                            5908,
                            5992,
                        ],
                    }
                ),
                "bad_child_reuse_3",
            ),
            id="bad_child_reuse_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                bad_child_reuse_4,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [
                            15980,
                            23828,
                            26876,
                            68822,
                            72359,
                            89900,
                            100214,
                            118361,
                            125284,
                            138209,
                        ],
                        "n_orders": [8, 8, 8, 9, 8, 6, 10, 8, 7, 6],
                    }
                ),
                "bad_child_reuse_4",
            ),
            id="bad_child_reuse_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                bad_child_reuse_5,
                "TPCH",
                lambda: pd.DataFrame(
                    {"key": [2487, 43044, 69321, 76146], "n_orders": [0] * 4}
                ),
                "bad_child_reuse_5",
            ),
            id="bad_child_reuse_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                aggregation_analytics_1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_name": [
                            "ghost cornflower purple chartreuse blue",
                            "hot maroon purple navajo floral",
                            "lavender deep powder cream orchid",
                            "navajo blush honeydew slate forest",
                            "red almond goldenrod tomato cornsilk",
                            "linen blanched mint pale blue",
                            "rose sienna maroon cornsilk azure",
                            "pale rosy blanched navy black",
                        ],
                        "revenue": [
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            0.0,
                            15376.45,
                            24876.23,
                            31277.99,
                        ],
                    }
                ),
                "aggregation_analytics_1",
                order_sensitive=True,
            ),
            id="aggregation_analytics_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                aggregation_analytics_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_name": [
                            "sky misty beige azure lace",
                            "azure pale hot ghost brown",
                            "steel sky lavender green khaki",
                            "lime lemon indian papaya wheat",
                        ],
                        "revenue": [1276.69, 11278.96, 34936.07, 35220.91],
                    }
                ),
                "aggregation_analytics_2",
                order_sensitive=True,
            ),
            id="aggregation_analytics_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                aggregation_analytics_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_name": [
                            "lawn puff chartreuse smoke firebrick",
                            "moccasin cornsilk azure royal rose",
                            "lime blush midnight chartreuse grey",
                        ],
                        "revenue": [106.53, 158.72, 179.28],
                    }
                ),
                "aggregation_analytics_3",
                order_sensitive=True,
            ),
            id="aggregation_analytics_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                quantile_function_test_1,
                "TPCH",
                lambda: pd.DataFrame({"seventieth_order_price": [200536.2]}),
                "quantile_function_test_1",
            ),
            id="quantile_function_test_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                quantile_function_test_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "AMERICA",
                            "ASIA",
                        ],
                        "nation_name": [
                            "ALGERIA",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "CHINA",
                        ],
                        "orders_min": [1052.98, 1085.81, 1062.33, 1040.95, 1146.71],
                        "orders_1_percent": [
                            5999.3,
                            7003.64,
                            5048.08,
                            4057.84,
                            5441.97,
                        ],
                        "orders_10_percent": [
                            39638.93,
                            35979.56,
                            39705.76,
                            38877.67,
                            37364.74,
                        ],
                        "orders_25_percent": [
                            80248.44,
                            74602.29,
                            78630.93,
                            75621.71,
                            79468.12,
                        ],
                        "orders_median": [
                            145362.1,
                            140232.54,
                            143825.37,
                            143063.83,
                            145382.11,
                        ],
                        "orders_75_percent": [
                            219335.6,
                            216272.55,
                            212565.84,
                            215758.39,
                            216461.21,
                        ],
                        "orders_90_percent": [
                            276313.4,
                            273094.87,
                            270427.82,
                            274811.54,
                            274194.5,
                        ],
                        "orders_99_percent": [
                            363376.69,
                            359892.4,
                            360018.62,
                            359149.24,
                            358762.01,
                        ],
                        "orders_max": [
                            470709.7,
                            439803.36,
                            455713.74,
                            452718.68,
                            472728.8,
                        ],
                    }
                ),
                "quantile_function_test_2",
            ),
            id="quantile_function_test_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                quantile_function_test_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "AMERICA",
                            "ASIA",
                        ],
                        "nation_name": [
                            "ALGERIA",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "CHINA",
                        ],
                        "orders_min": [1052.98, 1085.81, 1062.33, 1040.95, 1146.71],
                        "orders_1_percent": [
                            5999.3,
                            7003.64,
                            5048.08,
                            4057.84,
                            5441.97,
                        ],
                        "orders_10_percent": [
                            39638.93,
                            35979.56,
                            39705.76,
                            38877.67,
                            37364.74,
                        ],
                        "orders_25_percent": [
                            80248.44,
                            74602.29,
                            78630.93,
                            75621.71,
                            79468.12,
                        ],
                        "orders_median": [
                            145362.1,
                            140232.54,
                            143825.37,
                            143063.83,
                            145382.11,
                        ],
                        "orders_75_percent": [
                            219335.6,
                            216272.55,
                            212565.84,
                            215758.39,
                            216461.21,
                        ],
                        "orders_90_percent": [
                            276313.4,
                            273094.87,
                            270427.82,
                            274811.54,
                            274194.5,
                        ],
                        "orders_99_percent": [
                            363376.69,
                            359892.4,
                            360018.62,
                            359149.24,
                            358762.01,
                        ],
                        "orders_max": [
                            470709.7,
                            439803.36,
                            455713.74,
                            452718.68,
                            472728.8,
                        ],
                    }
                ),
                "quantile_function_test_3",
            ),
            id="quantile_function_test_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                quantile_function_test_4,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "region_name": [
                            "AFRICA",
                            "AMERICA",
                            "AMERICA",
                            "AMERICA",
                            "ASIA",
                        ],
                        "nation_name": [
                            "ALGERIA",
                            "ARGENTINA",
                            "BRAZIL",
                            "CANADA",
                            "CHINA",
                        ],
                        "orders_min": [5390.99, 2622.17, 10183.86, 10722.74, 15050.91],
                        "orders_1_percent": [
                            5390.99,
                            2622.17,
                            10183.86,
                            10722.74,
                            15050.91,
                        ],
                        "orders_10_percent": [
                            56337.7,
                            60753.17,
                            19861.78,
                            40190.75,
                            51271.16,
                        ],
                        "orders_25_percent": [
                            93604.69,
                            106187.25,
                            76036.8,
                            65426.56,
                            85049.94,
                        ],
                        "orders_median": [
                            157615.53,
                            160145.36,
                            147766.19,
                            108165.94,
                            134156.84,
                        ],
                        "orders_75_percent": [
                            210510.63,
                            222249.89,
                            209519.89,
                            165556.88,
                            195812.23,
                        ],
                        "orders_90_percent": [
                            282754.57,
                            298230.29,
                            263862.04,
                            230003.53,
                            246470.76,
                        ],
                        "orders_99_percent": [
                            389176.08,
                            349270.55,
                            354968.79,
                            350760.86,
                            398201.08,
                        ],
                        "orders_max": [
                            389176.08,
                            349270.55,
                            354968.79,
                            350760.86,
                            398201.08,
                        ],
                    }
                ),
                "quantile_function_test_4",
            ),
            id="quantile_function_test_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_range_1,
                "TPCH",
                lambda: pd.DataFrame({"value": range(10)}),
                "simple_range_1",
            ),
            id="simple_range_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_range_2,
                "TPCH",
                lambda: pd.DataFrame({"value": range(9, -1, -1)}),
                "simple_range_2",
            ),
            id="simple_range_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_range_3,
                "TPCH",
                lambda: pd.DataFrame({"foo": range(15, 20)}),
                "simple_range_3",
            ),
            id="simple_range_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_range_4,
                "TPCH",
                lambda: pd.DataFrame({"foo": range(10, 0, -1)}),
                "simple_range_4",
            ),
            id="simple_range_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                simple_range_5,
                "TPCH",
                # TODO: even though generated SQL has CAST(NULL AS INT) AS x
                # it returns x as object datatype.
                # using `x: range(-1)` returns int64 so temp. using dtype=object
                lambda: pd.DataFrame({"x": pd.Series(range(-1), dtype="object")}),
                "simple_range_5",
            ),
            id="simple_range_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_1,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_size": [
                            1,
                            6,
                            11,
                            16,
                            21,
                            26,
                            31,
                            36,
                            41,
                            46,
                            51,
                            56,
                            61,
                            66,
                            71,
                            76,
                            81,
                            86,
                            91,
                            96,
                        ],
                        "n_parts": [
                            228,
                            225,
                            206,
                            234,
                            228,
                            221,
                            231,
                            208,
                            245,
                            226,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                        ],
                    }
                ),
                "user_range_collection_1",
            ),
            id="user_range_collection_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_2,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "x": [0, 2, 4, 6, 8],
                        "n_prefix": [1, 56, 56, 56, 56],
                        "n_suffix": [101, 100, 100, 100, 100],
                    }
                ),
                "user_range_collection_2",
            ),
            id="user_range_collection_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_3,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "x": [0, 2, 4, 6, 8],
                        "n_prefix": [1, 56, 56, 56, 56],
                        "n_suffix": [101, 100, 100, 100, 100],
                    }
                ),
                "user_range_collection_3",
            ),
            id="user_range_collection_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_4,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_size": [1, 2, 4, 5, 6, 10],
                        "name": [
                            "azure lime burnished blush salmon",
                            "spring green chocolate azure navajo",
                            "cornflower bisque thistle floral azure",
                            "azure aquamarine tomato lace peru",
                            "antique cyan tomato azure dim",
                            "red cream rosy hot azure",
                        ],
                        "retail_price": [
                            1217.13,
                            1666.60,
                            1863.87,
                            1114.16,
                            1716.72,
                            1746.81,
                        ],
                    }
                ),
                "user_range_collection_4",
            ),
            id="user_range_collection_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_5,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_size": [1, 11, 21, 31, 41, 51, 6, 16, 26, 36, 46, 56],
                        "n_parts": [
                            1135,
                            1067,
                            1128,
                            1109,
                            1038,
                            0,
                            1092,
                            1154,
                            1065,
                            1094,
                            1088,
                            0,
                        ],
                    }
                ),
                "user_range_collection_5",
            ),
            id="user_range_collection_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                user_range_collection_6,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "year": [
                            1990,
                            1991,
                            1992,
                            1993,
                            1994,
                            1995,
                            1996,
                            1997,
                            1998,
                            1999,
                            2000,
                        ],
                        "n_orders": [0, 0, 1, 2, 0, 0, 1, 1, 2, 0, 0],
                    }
                ),
                "user_range_collection_6",
            ),
            id="user_range_collection_6",
        ),
    ],
)
def tpch_custom_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on custom queries using the TPC-H database.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param


def test_pipeline_until_relational_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on
    custom queries with the TPC-H graph.
    """
    file_path: str = get_plan_test_filename(tpch_custom_pipeline_test_data.test_name)
    tpch_custom_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Same as test_pipeline_until_relational_tpch, but for the generated SQL text.
    """

    tpch_custom_pipeline_test_data = tpch_custom_test_data_dialect_replacements(
        empty_context_database.dialect, tpch_custom_pipeline_test_data
    )

    file_path: str = get_sql_test_filename(
        tpch_custom_pipeline_test_data.test_name, empty_context_database.dialect
    )
    tpch_custom_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the the custom queries with TPC-H data from the original
    code generation.
    """
    tpch_custom_pipeline_test_data.run_e2e_test(
        get_sample_graph, sqlite_tpch_db_context
    )


@pytest.mark.execute
@pytest.mark.parametrize(
    "pydough_impl, columns, error_message",
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
            "Expected `columns` argument to be a non-empty list",
            id="bad_columns_1",
        ),
        pytest.param(
            simple_scan,
            {},
            "Expected `columns` argument to be a non-empty dictionary",
            id="bad_columns_2",
        ),
        pytest.param(
            simple_scan,
            ["A", "B", "C"],
            re.escape(
                "Unrecognized term of TPCH.orders.CALCULATE(key=key): 'A'. Did you mean: key, clerk, lines?"
            ),
            id="bad_columns_3",
        ),
        pytest.param(
            simple_scan,
            {"X": "key", "W": "Y"},
            re.escape(
                "Unrecognized term of TPCH.orders.CALCULATE(key=key): 'Y'. Did you mean: key, clerk, lines?"
            ),
            id="bad_columns_4",
        ),
        pytest.param(
            simple_scan,
            ["key", "key"],
            "Duplicate column names found in root.",
            id="bad_columns_5",
        ),
        pytest.param(
            bad_name_1,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'c_name'. Did you mean: name, key, phone?"
            ),
            id="bad_name_1",
        ),
        pytest.param(
            bad_name_2,
            None,
            re.escape(
                "Unrecognized term of TPCH: 'CUSTS'. Did you mean: parts, lines, customers, orders?"
            ),
            id="bad_name_2",
        ),
        pytest.param(
            bad_name_3,
            None,
            re.escape(
                "Unrecognized term of TPCH.CALCULATE(foo=1, bar=2, fizz=3, BUZZ=4): 'fizzbuzz'. Did you mean: fizz, BUZZ, foo?"
            ),
            id="bad_name_3",
        ),
        pytest.param(
            bad_name_4,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers.orders: 'totalPrice'. Did you mean: total_price, clerk, key?"
            ),
            id="bad_name_4",
        ),
        pytest.param(
            bad_name_5,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers.orders: 'c_name'. Did you mean: key, lines, clerk, comment, customer?"
            ),
            id="bad_name_5",
        ),
        pytest.param(
            bad_name_6,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'suppliers'. Did you mean: orders, key, name, address, phone?"
            ),
            id="bad_name_6",
        ),
        pytest.param(
            bad_name_7,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'NAME'. Did you mean: name, key, phone?"
            ),
            id="bad_name_7",
        ),
        pytest.param(
            bad_name_8,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'n123ame'. Did you mean: name, key, phone?"
            ),
            id="bad_name_8",
        ),
        pytest.param(
            bad_name_9,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: '__phone__'. Did you mean: phone, key, name?"
            ),
            id="bad_name_9",
        ),
        pytest.param(
            bad_name_10,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'nam'. Did you mean: name, key, nation?"
            ),
            id="bad_name_10",
        ),
        pytest.param(
            bad_name_11,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'namex'. Did you mean: name, key, nation?"
            ),
            id="bad_name_11",
        ),
        pytest.param(
            bad_name_12,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: '___'. Did you mean: key, name, phone?"
            ),
            id="bad_name_12",
        ),
        pytest.param(
            bad_name_13,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'thisisareallylargename_that_exceeds_the_system_limit'. Did you mean: market_segment, name, orders, address, key?"
            ),
            id="bad_name_13",
        ),
        pytest.param(
            bad_name_14,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'keyname'. Did you mean: key, name, phone?"
            ),
            id="bad_name_14",
        ),
        pytest.param(
            bad_name_15,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'namekey'. Did you mean: name, key, nation?"
            ),
            id="bad_name_15",
        ),
        pytest.param(
            bad_name_16,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers: 'no_exist'. Did you mean: name, key, comment, nation, orders?"
            ),
            id="bad_name_16",
        ),
        pytest.param(
            bad_name_17,
            None,
            re.escape(
                "Unrecognized term of TPCH.Partition(orders.CALCULATE(year=YEAR(order_date)), name='years', by=year): 'yrs'. Did you mean: year, orders?"
            ),
            id="bad_name_17",
        ),
        pytest.param(
            bad_name_18,
            None,
            re.escape(
                "Unrecognized term of TPCH.Partition(orders.CALCULATE(year=YEAR(order_date)), name='years', by=year).CALCULATE(n_orders=COUNT(orders)).orders: 'nords'. Did you mean: n_orders, key, lines, year, clerk?"
            ),
            id="bad_name_18",
        ),
        pytest.param(
            bad_name_19,
            None,
            re.escape(
                "Unrecognized term of TPCH.Partition(orders.CALCULATE(year=YEAR(order_date)), name='years', by=year): 'order_date'. Did you mean: orders, year?"
            ),
            id="bad_name_19",
        ),
        pytest.param(
            bad_name_20,
            None,
            re.escape(
                "Unrecognized term of TPCH.Partition(orders.CALCULATE(year=YEAR(order_date)), name='years', by=year).CALCULATE(n_orders=COUNT(orders)).orders: 'orders'. Did you mean: n_orders, clerk, key, lines, year?"
            ),
            id="bad_name_20",
        ),
        pytest.param(
            bad_name_21,
            None,
            re.escape(
                "Unrecognized term of TPCH.regions.CALCULATE(rname=name, rkey=key, rcomment=comment).nations: 'RNAME'. Did you mean: rname, name, rkey?"
            ),
            id="bad_name_21",
        ),
        pytest.param(
            bad_name_22,
            None,
            re.escape(
                "Unrecognized term of TPCH.CALCULATE(anthro_pomorph_IZATION=1, counte_rintelligence=2, OVERIN_tellectualizers=3, ultra_revolution_aries=4, PROFESSION_alization=5, De_Institutionalizations=6, over_intellect_ualiz_ation=7): 'Over_Intellectual_Ization'. Did you mean: over_intellect_ualiz_ation, OVERIN_tellectualizers, PROFESSION_alization?"
            ),
            id="bad_name_22",
        ),
        pytest.param(
            bad_name_23,
            None,
            re.escape(
                "Unrecognized term of TPCH.CALCULATE(anthro_pomorph_IZATION=1, counte_rintelligence=2, OVERIN_tellectualizers=3, ultra_revolution_aries=4, PROFESSION_alization=5, De_Institutionalizations=6, over_intellect_ualiz_ation=7): 'paio_eo_aliz_ation'. Did you mean: PROFESSION_alization, nations, parts, regions?"
            ),
            id="bad_name_23",
        ),
        pytest.param(
            bad_name_24,
            None,
            re.escape(
                "Unrecognized term of TPCH.CALCULATE(anthro_pomorph_IZATION=1, counte_rintelligence=2, OVERIN_tellectualizers=3, ultra_revolution_aries=4, PROFESSION_alization=5, De_Institutionalizations=6, over_intellect_ualiz_ation=7): '_a_r_h_x_n_t_p_o_q__z_m_o_p_i__a_o_n_z_'. Did you mean: nations, parts, anthro_pomorph_IZATION, lines, regions?"
            ),
            id="bad_name_24",
        ),
        pytest.param(
            bad_name_25,
            None,
            re.escape(
                "Unrecognized term of TPCH.CALCULATE(anthro_pomorph_IZATION=1, counte_rintelligence=2, OVERIN_tellectualizers=3, ultra_revolution_aries=4, PROFESSION_alization=5, De_Institutionalizations=6, over_intellect_ualiz_ation=7): 'anthropomorphization_and_overintellectualization_and_ultrarevolutionaries'. Did you mean: over_intellect_ualiz_ation, anthro_pomorph_IZATION, OVERIN_tellectualizers, ultra_revolution_aries?"
            ),
            id="bad_name_25",
        ),
        pytest.param(
            bad_cross_1,
            None,
            "Cannot qualify int: 42",
            id="bad_cross_1",
        ),
        pytest.param(
            bad_cross_2,
            None,
            "Expected a collection, but received an expression",
            id="bad_cross_2",
        ),
        pytest.param(
            bad_cross_3,
            None,
            re.escape(
                "Unrecognized term of TPCH.customers.TPCH: 'foo'. Did you mean: lines, parts, nations, orders, regions?"
            ),
            id="bad_cross_3",
        ),
        pytest.param(
            bad_cross_4,
            None,
            "Name 'customers' conflicts with a collection name in the graph",
            id="bad_cross_4",
        ),
        pytest.param(
            bad_cross_5,
            None,
            re.escape(
                "nclear whether 'name' refers to a term of the current context or ancestor of collection TPCH.regions.CALCULATE(name=name).TPCH.regions"
            ),
            id="bad_cross_5",
        ),
        pytest.param(
            bad_cross_6,
            None,
            re.escape(
                "Unrecognized term of TPCH.suppliers.TPCH.parts: 'suppliers'. Did you mean: size, lines, key, name, supply_records?"
            ),
            id="bad_cross_6",
        ),
        pytest.param(
            bad_cross_7,
            None,
            "",
            id="bad_cross_7",
        ),
        pytest.param(
            bad_cross_8,
            None,
            re.escape(
                "Unrecognized term of TPCH.regions.CALCULATE(r1=name).TPCH.nations: 'r_key'. Did you mean: key, r1, name?"
            ),
            id="bad_cross_8",
        ),
        pytest.param(
            bad_cross_9,
            None,
            "Expected an expression, but received a collection: TPCH.regions",
            id="bad_cross_9",
        ),
        pytest.param(
            bad_cross_10,
            None,
            "PyDough does yet support aggregations whose arguments mix between subcollection data of the current context and fields of the context itself",
            id="bad_cross_10",
        ),
        pytest.param(
            bad_cross_11,
            None,
            "Unrecognized term of TPCH.nations.TPCH.regions: 'customers'. Did you mean: comment, name, key, nations?",
            id="bad_cross_11",
        ),
        pytest.param(
            bad_quantile_1,
            None,
            re.escape(
                "Invalid operator invocation 'QUANTILE(orders.total_price)': Expected 2 arguments, received 1"
            ),
            id="bad_quantile_1",
        ),
        pytest.param(
            bad_quantile_2,
            None,
            re.escape(
                "Expected aggregation call to contain references to exactly one child collection, but found 0 in QUANTILE('orders.total_price', 0.7)"
            ),
            id="bad_quantile_2",
        ),
        pytest.param(
            bad_quantile_3,
            None,
            re.escape(
                "Expected second argument to QUANTILE to be a numeric literal between 0 and 1, instead received 40"
            ),
            id="bad_quantile_3",
        ),
        pytest.param(
            bad_quantile_4,
            None,
            re.escape(
                "Expected second argument to QUANTILE to be a numeric literal between 0 and 1, instead received -10"
            ),
            id="bad_quantile_4",
        ),
        pytest.param(
            bad_quantile_5,
            None,
            re.escape(
                "Non-expression argument orders of type ChildReferenceCollection found in operator 'QUANTILE'"
            ),
            id="bad_quantile_5",
        ),
        pytest.param(
            bad_quantile_6,
            None,
            re.escape(
                "Expected aggregation call to contain references to exactly one child collection, but found 0 in QUANTILE(20, 0.9)"
            ),
            id="bad_quantile_6",
        ),
    ],
)
def test_pipeline_e2e_errors(
    pydough_impl: Callable[..., UnqualifiedNode],
    columns: dict[str, str] | list[str] | None,
    error_message: str,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    run_e2e_error_test(
        pydough_impl,
        error_message,
        graph,
        columns=columns,
        database=sqlite_tpch_db_context,
    )
