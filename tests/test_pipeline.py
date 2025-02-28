"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from bad_pydough_functions import (
    bad_lpad_1,
    bad_lpad_2,
    bad_lpad_3,
    bad_lpad_4,
    bad_lpad_5,
    bad_lpad_6,
    bad_lpad_7,
    bad_lpad_8,
    bad_rpad_1,
    bad_rpad_2,
    bad_rpad_3,
    bad_rpad_4,
    bad_rpad_5,
    bad_rpad_6,
    bad_rpad_7,
    bad_rpad_8,
    bad_slice_1,
    bad_slice_2,
    bad_slice_3,
    bad_slice_4,
)
from correlated_pydough_functions import (
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
)
from simple_pydough_functions import (
    agg_partition,
    datetime_current,
    datetime_relative,
    double_partition,
    exponentiation,
    function_sampler,
    hour_minute_day,
    minutes_seconds_datediff,
    multi_partition_access_1,
    multi_partition_access_2,
    multi_partition_access_3,
    multi_partition_access_4,
    multi_partition_access_5,
    multi_partition_access_6,
    padding_functions,
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
    simple_scan,
    simple_scan_top_five,
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

from pydough import init_pydough_context, to_df, to_sql
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
                impl_tpch_q1,
                None,
                "tpch_q1",
                tpch_q1_output,
            ),
            id="tpch_q1",
        ),
        pytest.param(
            (
                impl_tpch_q2,
                None,
                "tpch_q2",
                tpch_q2_output,
            ),
            id="tpch_q2",
        ),
        pytest.param(
            (
                impl_tpch_q3,
                None,
                "tpch_q3",
                tpch_q3_output,
            ),
            id="tpch_q3",
        ),
        pytest.param(
            (
                impl_tpch_q4,
                None,
                "tpch_q4",
                tpch_q4_output,
            ),
            id="tpch_q4",
        ),
        pytest.param(
            (
                impl_tpch_q5,
                None,
                "tpch_q5",
                tpch_q5_output,
            ),
            id="tpch_q5",
        ),
        pytest.param(
            (
                impl_tpch_q6,
                None,
                "tpch_q6",
                tpch_q6_output,
            ),
            id="tpch_q6",
        ),
        pytest.param(
            (
                impl_tpch_q7,
                None,
                "tpch_q7",
                tpch_q7_output,
            ),
            id="tpch_q7",
        ),
        pytest.param(
            (
                impl_tpch_q8,
                None,
                "tpch_q8",
                tpch_q8_output,
            ),
            id="tpch_q8",
        ),
        pytest.param(
            (
                impl_tpch_q9,
                None,
                "tpch_q9",
                tpch_q9_output,
            ),
            id="tpch_q9",
        ),
        pytest.param(
            (
                impl_tpch_q10,
                None,
                "tpch_q10",
                tpch_q10_output,
            ),
            id="tpch_q10",
        ),
        pytest.param(
            (
                impl_tpch_q11,
                None,
                "tpch_q11",
                tpch_q11_output,
            ),
            id="tpch_q11",
        ),
        pytest.param(
            (
                impl_tpch_q12,
                None,
                "tpch_q12",
                tpch_q12_output,
            ),
            id="tpch_q12",
        ),
        pytest.param(
            (
                impl_tpch_q13,
                None,
                "tpch_q13",
                tpch_q13_output,
            ),
            id="tpch_q13",
        ),
        pytest.param(
            (
                impl_tpch_q14,
                None,
                "tpch_q14",
                tpch_q14_output,
            ),
            id="tpch_q14",
        ),
        pytest.param(
            (
                impl_tpch_q15,
                None,
                "tpch_q15",
                tpch_q15_output,
            ),
            id="tpch_q15",
        ),
        pytest.param(
            (
                impl_tpch_q16,
                None,
                "tpch_q16",
                tpch_q16_output,
            ),
            id="tpch_q16",
        ),
        pytest.param(
            (
                impl_tpch_q17,
                None,
                "tpch_q17",
                tpch_q17_output,
            ),
            id="tpch_q17",
        ),
        pytest.param(
            (
                impl_tpch_q18,
                None,
                "tpch_q18",
                tpch_q18_output,
            ),
            id="tpch_q18",
        ),
        pytest.param(
            (
                impl_tpch_q19,
                None,
                "tpch_q19",
                tpch_q19_output,
            ),
            id="tpch_q19",
        ),
        pytest.param(
            (
                impl_tpch_q20,
                None,
                "tpch_q20",
                tpch_q20_output,
            ),
            id="tpch_q20",
        ),
        pytest.param(
            (
                impl_tpch_q21,
                None,
                "tpch_q21",
                tpch_q21_output,
            ),
            id="tpch_q21",
        ),
        pytest.param(
            (
                impl_tpch_q22,
                None,
                "tpch_q22",
                tpch_q22_output,
            ),
            id="tpch_q22",
        ),
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
                    }
                ),
            ),
            id="function_sampler",
        ),
        pytest.param(
            (
                datetime_current,
                None,
                "datetime_current",
                lambda: pd.DataFrame(
                    {
                        "d1": [f"{pd.Timestamp.now().year}-05-31 00:00:00"],
                        "d2": [
                            f"{pd.Timestamp.now().year}-{pd.Timestamp.now().month:02}-02 00:00:00"
                        ],
                        "d3": [
                            (
                                pd.Timestamp.now().normalize()
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
                            f"{y}-01-01 00:00:00"
                            for y in [1992] * 3 + [1994] * 3 + [1996] * 3 + [1997]
                        ],
                        "d2": [
                            "1992-04-01 00:00:00",
                            "1992-04-01 00:00:00",
                            "1992-08-01 00:00:00",
                            "1994-05-01 00:00:00",
                            "1994-08-01 00:00:00",
                            "1994-12-01 00:00:00",
                            "1996-06-01 00:00:00",
                            "1996-07-01 00:00:00",
                            "1996-12-01 00:00:00",
                            "1997-03-01 00:00:00",
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
    Test data for test_pydough_pipeline. Returns a tuple of the following
    arguments:
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


def test_pipeline_until_relational(
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
    qualified DAG version, with the correct string representation.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, columns, file_name, _ = pydough_pipeline_test_data
    file_path: str = get_plan_test_filename(file_name)
    graph: GraphMetadata = get_sample_graph("TPCH")
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    relational: RelationalRoot = convert_ast_to_relational(
        qualified, _load_column_selection({"columns": columns}), default_config
    )
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
            "SLICE function currently only supports non-negative stop indices",
            id="bad_slice_1",
        ),
        pytest.param(
            bad_slice_2,
            None,
            "SLICE function currently only supports non-negative start indices",
            id="bad_slice_2",
        ),
        pytest.param(
            bad_slice_3,
            None,
            "SLICE function currently only supports a step of 1",
            id="bad_slice_3",
        ),
        pytest.param(
            bad_slice_4,
            None,
            "SLICE function currently only supports a step of 1",
            id="bad_slice_4",
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


@pytest.fixture(
    params=[
        pytest.param(
            (
                multi_partition_access_1,
                None,
                "Broker",
                "multi_partition_access_1",
                lambda: pd.DataFrame(
                    {"symbol": ["AAPL", "AMZN", "BRK.B", "FB", "GOOG"]}
                ),
            ),
            id="multi_partition_access_1",
        ),
        pytest.param(
            (
                multi_partition_access_2,
                None,
                "Broker",
                "multi_partition_access_2",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [f"TX{i:03}" for i in (22, 24, 25, 27, 56)],
                        "name": [
                            "Jane Smith",
                            "Samantha Lee",
                            "Michael Chen",
                            "David Kim",
                            "Jane Smith",
                        ],
                        "symbol": ["MSFT", "TSLA", "GOOGL", "BRK.B", "FB"],
                        "transaction_type": ["sell", "sell", "buy", "buy", "sell"],
                        "cus_tick_typ_avg_shares": [56.66667, 55.0, 4.0, 55.5, 47.5],
                        "cust_tick_avg_shares": [
                            50.0,
                            41.66667,
                            3.33333,
                            37.33333,
                            47.5,
                        ],
                        "cust_avg_shares": [50.625, 46.25, 40.0, 37.33333, 50.625],
                    }
                ),
            ),
            id="multi_partition_access_2",
        ),
        pytest.param(
            (
                multi_partition_access_3,
                None,
                "Broker",
                "multi_partition_access_3",
                lambda: pd.DataFrame(
                    {
                        "symbol": [
                            "AAPL",
                            "AMZN",
                            "FB",
                            "GOOGL",
                            "JPM",
                            "MSFT",
                            "NFLX",
                            "PG",
                            "TSLA",
                            "V",
                        ],
                        "close": [
                            153.5,
                            3235,
                            207,
                            2535,
                            133.75,
                            284,
                            320.5,
                            143.25,
                            187.75,
                            223.5,
                        ],
                    }
                ),
            ),
            id="multi_partition_access_3",
        ),
        pytest.param(
            (
                multi_partition_access_4,
                None,
                "Broker",
                "multi_partition_access_4",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (3, 4, 5, 6, 7, 8, 9, 40, 41, 42, 43, 47, 48, 49)
                        ],
                    }
                ),
            ),
            id="multi_partition_access_4",
        ),
        pytest.param(
            (
                multi_partition_access_5,
                None,
                "Broker",
                "multi_partition_access_5",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (
                                40,
                                41,
                                42,
                                43,
                                2,
                                4,
                                6,
                                22,
                                24,
                                26,
                                32,
                                34,
                                36,
                                46,
                                48,
                                50,
                                52,
                                54,
                                56,
                            )
                        ],
                        "n_ticker_type_trans": [1] * 4 + [5] * 15,
                        "n_ticker_trans": [1] * 4 + [6] * 15,
                        "n_type_trans": [29, 27] * 2 + [27] * 15,
                    }
                ),
            ),
            id="multi_partition_access_5",
        ),
        pytest.param(
            (
                multi_partition_access_6,
                None,
                "Broker",
                "multi_partition_access_6",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (
                                11,
                                12,
                                13,
                                14,
                                15,
                                16,
                                17,
                                18,
                                19,
                                20,
                                30,
                                46,
                                47,
                                48,
                                49,
                                50,
                            )
                        ],
                    }
                ),
            ),
            id="multi_partition_access_6",
        ),
        pytest.param(
            (
                hour_minute_day,
                None,
                "Broker",
                "hour_minute_day",
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
            id="hour_minute_day",
        ),
        pytest.param(
            (
                exponentiation,
                None,
                "Broker",
                "exponentiation",
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
                None,
                "Broker",
                "years_months_days_hours_datediff",
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
                None,
                "Broker",
                "minutes_seconds_datediff",
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
                padding_functions,
                None,
                "Broker",
                "padding_functions",
                lambda: pd.DataFrame(
                    {
                        "original_name": [
                            "Alex Rodriguez",
                            "Ava Wilson",
                            "Bob Johnson",
                            "David Kim",
                            "Emily Davis",
                        ]
                    }
                ).assign(
                    ref_rpad=lambda x: "Cust0001**********************",
                    ref_lpad=lambda x: "**********************Cust0001",
                    right_padded=lambda x: x.original_name.apply(
                        lambda s: (s + "*" * 30)[:30]
                    ),
                    # This lambda only works when each string is less than 30 characters
                    left_padded=lambda x: x.original_name.apply(
                        lambda s: ("#" * 30 + s)[-30:]
                    ),
                    truncated_right=[
                        "Alex Rod",
                        "Ava Wils",
                        "Bob John",
                        "David Ki",
                        "Emily Da",
                    ],
                    truncated_left=[
                        "Alex Rod",
                        "Ava Wils",
                        "Bob John",
                        "David Ki",
                        "Emily Da",
                    ],
                    zero_pad_right=[""] * 5,
                    zero_pad_left=[""] * 5,
                    right_padded_space=lambda x: x.original_name.apply(
                        lambda s: (s + " " * 30)[:30]
                    ),
                    left_padded_space=lambda x: x.original_name.apply(
                        lambda s: (" " * 30 + s)[-30:]
                    ),
                ),
            ),
            id="padding_functions",
        ),
    ],
)
def custom_defog_test_data(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    dict[str, str] | list[str] | None,
    str,
    str,
    pd.DataFrame,
]:
    """
    Test data for test_defog_e2e. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a PyDough implementation function.
    2. `columns`: the columns to select from the relational plan (optional).
    3. `graph_name`: the name of the graph from the defog database to use.
    4. `file_name`: the name of the file containing the expected relational
    plan.
    5. `answer_impl`: a function that takes in nothing and returns the answer
    to a defog query as a Pandas DataFrame.
    """
    return request.param


def test_defog_until_relational(
    custom_defog_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        str,
        pd.DataFrame,
    ],
    defog_graphs: graph_fetcher,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
):
    """
    Same as `test_pipeline_until_relational`, but for defog data.
    """
    unqualified_impl, columns, graph_name, file_name, _ = custom_defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    init_pydough_context(graph)(unqualified_impl)()
    file_path: str = get_plan_test_filename(file_name)
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    relational: RelationalRoot = convert_ast_to_relational(
        qualified, _load_column_selection({"columns": columns}), default_config
    )
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
def test_defog_e2e_with_custom_data(
    custom_defog_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        str,
        pd.DataFrame,
    ],
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector.
    """
    unqualified_impl, columns, graph_name, _, answer_impl = custom_defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, columns=columns, metadata=graph, database=sqlite_defog_connection
    )
    breakpoint()
    pd.testing.assert_frame_equal(result, answer_impl())


@pytest.mark.parametrize(
    "impl, graph_name, error_msg",
    [
        pytest.param(
            bad_lpad_1,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_1",
        ),
        pytest.param(
            bad_lpad_2,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_2",
        ),
        pytest.param(
            bad_lpad_3,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_3",
        ),
        pytest.param(
            bad_lpad_4,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_4",
        ),
        pytest.param(
            bad_lpad_5,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_5",
        ),
        pytest.param(
            bad_lpad_6,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_6",
        ),
        pytest.param(
            bad_lpad_7,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_7",
        ),
        pytest.param(
            bad_lpad_8,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_8",
        ),
        pytest.param(
            bad_rpad_1,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_1",
        ),
        pytest.param(
            bad_rpad_2,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_2",
        ),
        pytest.param(
            bad_rpad_3,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_3",
        ),
        pytest.param(
            bad_rpad_4,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_4",
        ),
        pytest.param(
            bad_rpad_5,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_5",
        ),
        pytest.param(
            bad_rpad_6,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_6",
        ),
        pytest.param(
            bad_rpad_7,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_7",
        ),
        pytest.param(
            bad_rpad_8,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_8",
        ),
    ],
)
def test_defog_e2e_errors(
    impl: Callable[[], UnqualifiedNode],
    graph_name: str,
    error_msg: str,
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised for defog database.
    """
    graph: GraphMetadata = defog_graphs(graph_name)
    with pytest.raises(Exception, match=error_msg):
        root: UnqualifiedNode = init_pydough_context(graph)(impl)()
        to_sql(root, metadata=graph, database=sqlite_defog_connection)
