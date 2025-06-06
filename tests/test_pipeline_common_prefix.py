"""
Integration tests for the PyDough workflow on edge cases regarding common
prefixes of logic that can be syncretized, using the TPC-H data.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.common_prefix_pydough_functions import (
    common_prefix_a,
    common_prefix_aa,
    common_prefix_ab,
    common_prefix_ac,
    common_prefix_ad,
    common_prefix_ae,
    common_prefix_af,
    common_prefix_ag,
    common_prefix_ah,
    common_prefix_ai,
    common_prefix_aj,
    common_prefix_ak,
    common_prefix_al,
    common_prefix_am,
    common_prefix_an,
    common_prefix_ao,
    common_prefix_ap,
    common_prefix_aq,
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
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                common_prefix_a,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                    }
                ),
                "common_prefix_a",
            ),
            id="common_prefix_a",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_b,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                    }
                ),
                "common_prefix_b",
            ),
            id="common_prefix_b",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_c,
                "TPCH",
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
                "common_prefix_c",
            ),
            id="common_prefix_c",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_d,
                "TPCH",
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
                "common_prefix_d",
            ),
            id="common_prefix_d",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_e,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_nations": [5, 5, 5, 5, 5],
                    }
                ),
                "common_prefix_e",
            ),
            id="common_prefix_e",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_f,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_nations": [5, 5, 5, 5, 5],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                    }
                ),
                "common_prefix_f",
            ),
            id="common_prefix_f",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_g,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "n_customers": [29764, 29952, 30183, 30197, 29904],
                        "n_suppliers": [1955, 2036, 2003, 1987, 2019],
                        "n_nations": [5, 5, 5, 5, 5],
                    }
                ),
                "common_prefix_g",
            ),
            id="common_prefix_g",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_h,
                "TPCH",
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
                "common_prefix_h",
            ),
            id="common_prefix_h",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_i,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["FRANCE", "ROMANIA", "RUSSIA", "JORDAN", "CHINA"],
                        "n_customers": [6100, 6100, 6078, 6033, 6024],
                        "n_selected_orders": [1, 2, 1, 1, 1],
                    }
                ),
                "common_prefix_i",
            ),
            id="common_prefix_i",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_j,
                "TPCH",
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
                "common_prefix_j",
            ),
            id="common_prefix_j",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_k,
                "TPCH",
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
                "common_prefix_k",
            ),
            id="common_prefix_k",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_l,
                "TPCH",
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
                "common_prefix_l",
            ),
            id="common_prefix_l",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_m,
                "TPCH",
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
                "common_prefix_m",
            ),
            id="common_prefix_m",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_n,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [63587, 961063, 2817185, 4231617, 4610691],
                        "order_date": [f"1996-11-{d}" for d in (19, 19, 20, 21, 25)],
                        "n_elements": [2] * 5,
                        "total_retail_price": [
                            2535.36,
                            3786.75,
                            2230.21,
                            2224.33,
                            2648.72,
                        ],
                        "n_unique_supplier_nations": [1] * 5,
                        "max_supplier_balance": [
                            5843.53,
                            2475.73,
                            1636.09,
                            8112.42,
                            1559.72,
                        ],
                        "n_small_parts": [0, 0, 1, 0, 0],
                    }
                ),
                "common_prefix_n",
            ),
            id="common_prefix_n",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_o,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [1067298, 1469184, 1715782, 2817185, 3696544],
                        "order_date": [f"1996-11-{d}" for d in (18, 16, 18, 20, 15)],
                        "n_elements": [3, 2, 3, 2, 3],
                        "total_retail_price": [
                            4642.71,
                            2603.58,
                            3715.75,
                            2230.21,
                            4493.44,
                        ],
                        "n_unique_supplier_nations": [2, 1, 2, 1, 2],
                        "max_supplier_balance": [
                            7767.63,
                            2523.28,
                            8423.81,
                            1636.09,
                            6996.82,
                        ],
                        "n_small_parts": [2, 1, 1, 1, 2],
                    }
                ),
                "common_prefix_o",
            ),
            id="common_prefix_o",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_p,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (140698, 1, 2, 4, 7)],
                        "n_orders": [5, 2, 4, 3, 5],
                        "n_parts_ordered": [2, 1, 2, 1, 1],
                        "n_distinct_parts": [1, 1, 2, 1, 1],
                    }
                ),
                "common_prefix_p",
            ),
            id="common_prefix_p",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_q,
                "TPCH",
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
                "common_prefix_q",
            ),
            id="common_prefix_q",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_r,
                "TPCH",
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
                "common_prefix_r",
            ),
            id="common_prefix_r",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_s,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": ["Customer#000106507"],
                        "most_recent_order_date": ["1998-05-25"],
                        "most_recent_order_total": [7],
                        "most_recent_order_distinct": [6],
                    }
                ),
                "common_prefix_s",
            ),
            id="common_prefix_s",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_t,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (126850, 80485, 86209, 73420, 146809)
                        ],
                        "total_qty": [3670, 3439, 3422, 3409, 3409],
                    }
                ),
                "common_prefix_t",
            ),
            id="common_prefix_t",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_u,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (111613, 112126, 92282, 69872, 135349)
                        ],
                        "total_qty": [169, 162, 151, 150, 136],
                    }
                ),
                "common_prefix_u",
            ),
            id="common_prefix_u",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_v,
                "TPCH",
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
                "common_prefix_v",
            ),
            id="common_prefix_v",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_w,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [37, 64, 68, 228, 293],
                        "cust_nation_name": ["ALGERIA"]
                        + ["ARGENTINA"] * 3
                        + ["ALGERIA"],
                    }
                ),
                "common_prefix_w",
            ),
            id="common_prefix_w",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_x,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (3451, 102004, 102022, 79300, 117082)
                        ],
                        "n_orders": [41, 41, 41, 40, 40],
                    }
                ),
                "common_prefix_x",
            ),
            id="common_prefix_x",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_y,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            f"Customer#{i:09}"
                            for i in (138841, 36091, 54952, 103768, 46081)
                        ],
                        "n_orders": [21, 20, 19, 19, 17],
                    }
                ),
                "common_prefix_y",
            ),
            id="common_prefix_y",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_z,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "name": [f"Customer#{i:09}" for i in (7, 9, 19, 21, 25)],
                        "nation_name": ["CHINA", "INDIA"] * 2 + ["JAPAN"],
                    }
                ),
                "common_prefix_z",
            ),
            id="common_prefix_z",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_aa,
                "TPCH",
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
                "common_prefix_aa",
            ),
            id="common_prefix_aa",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ab,
                "TPCH",
                lambda: pd.DataFrame({"n": [54318]}),
                "common_prefix_ab",
            ),
            id="common_prefix_ab",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ac,
                "TPCH",
                lambda: pd.DataFrame({"n": [137398]}),
                "common_prefix_ac",
            ),
            id="common_prefix_ac",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ad,
                "TPCH",
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
                        "part_qty": [9291, 9570, 1002],
                        "qty_shipped": [4, 38, 31],
                    }
                ),
                "common_prefix_ad",
            ),
            id="common_prefix_ad",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ae,
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
                        "n_customers": [6024, 6042, 6161, 5948, 6008],
                        "customer_name": [
                            "Customer#000018193",
                            "Customer#000052147",
                            "Customer#000067505",
                            None,
                            "Customer#000148697",
                        ],
                    }
                ),
                "common_prefix_ae",
            ),
            id="common_prefix_ae",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_af,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "nation_name": ["CHINA", "INDIA", "INDONESIA", "VIETNAM"],
                        "n_customers": [6024, 6042, 6161, 6008],
                        "customer_name": [
                            "Customer#000018193",
                            "Customer#000052147",
                            "Customer#000067505",
                            "Customer#000148697",
                        ],
                    }
                ),
                "common_prefix_af",
            ),
            id="common_prefix_af",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ag,
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
                        "n_machine_cust": [1167, 1197, 1273, 1223, 1158],
                        "n_machine_high_orders": [210, 223, 229, 244, 226],
                        "n_machine_high_domestic_lines": [1, 1, 4, 4, 5],
                        "total_machine_high_domestic_revenue": [
                            13539.14,
                            19693.28,
                            45887.34,
                            83430.91,
                            101991.05,
                        ],
                    }
                ),
                "common_prefix_ag",
            ),
            id="common_prefix_ag",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ah,
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
                        "n_machine_high_orders": [210, 223, 229, 244, 226],
                        "n_machine_high_domestic_lines": [1, 1, 4, 4, 5],
                        "total_machine_high_domestic_revenue": [
                            13539.14,
                            19693.28,
                            45887.34,
                            83430.91,
                            101991.05,
                        ],
                    }
                ),
                "common_prefix_ah",
            ),
            id="common_prefix_ah",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ai,
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
                        "n_machine_cust": [1167, 1197, 1273, 1223, 1158],
                        "n_machine_high_domestic_lines": [1, 1, 4, 4, 5],
                        "total_machine_high_domestic_revenue": [
                            13539.14,
                            19693.28,
                            45887.34,
                            83430.91,
                            101991.05,
                        ],
                    }
                ),
                "common_prefix_ai",
            ),
            id="common_prefix_ai",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_aj,
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
                        "n_machine_cust": [1167, 1197, 1273, 1223, 1158],
                        "n_machine_high_orders": [210, 223, 229, 244, 226],
                        "total_machine_high_domestic_revenue": [
                            13539.14,
                            19693.28,
                            45887.34,
                            83430.91,
                            101991.05,
                        ],
                    }
                ),
                "common_prefix_aj",
            ),
            id="common_prefix_aj",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ak,
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
                        "n_machine_cust": [1167, 1197, 1273, 1223, 1158],
                        "n_machine_high_orders": [210, 223, 229, 244, 226],
                        "n_machine_high_domestic_lines": [1, 1, 4, 4, 5],
                    }
                ),
                "common_prefix_ak",
            ),
            id="common_prefix_ak",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_al,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [14, 28, 49],
                        "n_orders": [10, 18, 17],
                        "n_no_tax_discount": [2, 2, 1],
                    }
                ),
                "common_prefix_al",
            ),
            id="common_prefix_al",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_am,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [14],
                        "n_orders": [10],
                        "n_no_tax_discount": [2],
                    }
                ),
                "common_prefix_am",
            ),
            id="common_prefix_am",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_an,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [49, 64, 97, 136, 170, 187, 196],
                        "n_orders": [17, 20, 27, 21, 14, 23, 25],
                        "n_no_tax_discount": [1, 3, 5, 1, 3, 1, 2],
                    }
                ),
                "common_prefix_an",
            ),
            id="common_prefix_an",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ao,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "cust_key": [4540],
                        "n_orders": [19],
                        "n_no_tax_discount": [2],
                        "n_part_purchases": [1],
                    }
                ),
                "common_prefix_ao",
            ),
            id="common_prefix_ao",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_ap,
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "part_name": [
                            "beige gainsboro lawn honeydew pink",
                            "dodger pink blanched lace peru",
                            "floral beige pink papaya drab",
                            "ghost pink burnished lace spring",
                            "red burnished pink khaki misty",
                            "wheat ghost medium pink mint",
                        ],
                        "supplier_name": [
                            "Supplier#000004129",
                            "Supplier#000009118",
                            "Supplier#000000476",
                            "Supplier#000002386",
                            "Supplier#000009118",
                            "Supplier#000008230",
                        ],
                        "supplier_quantity": [8815, 8082, 5228, 5916, 9665, 9390],
                        "supplier_nation": [
                            "INDONESIA",
                            "ALGERIA",
                            "ALGERIA",
                            "RUSSIA",
                            "ALGERIA",
                            "INDONESIA",
                        ],
                    }
                ),
                "common_prefix_ap",
            ),
            id="common_prefix_ap",
        ),
        pytest.param(
            PyDoughPandasTest(
                common_prefix_aq,
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
                            "ALGERIA",
                            "ARGENTINA",
                            "CHINA",
                            "FRANCE",
                            "EGYPT",
                        ],
                        "best_supplier": [
                            f"Supplier#{s:09}" for s in (4637, 4282, 1252, 2302, 6343)
                        ],
                        "best_part": [
                            "floral burlywood red saddle navy",
                            "lime wheat lace cornflower coral",
                            "spring tomato dodger ivory magenta",
                            "metallic medium spring floral sandy",
                            "dark chocolate white slate red",
                        ],
                        "best_quantity": [9976, 9644, 9985, 9975, 9970],
                    }
                ),
                "common_prefix_aq",
            ),
            id="common_prefix_aq",
        ),
    ],
)
def common_prefix_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on custom queries using the TPC-H database.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param


def test_pipeline_until_relational_common_prefix(
    common_prefix_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on
    the common prefix queries with the TPC-H graph.
    """
    file_path: str = get_plan_test_filename(common_prefix_pipeline_test_data.test_name)
    common_prefix_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


@pytest.mark.execute
def test_pipeline_e2e_common_prefix(
    common_prefix_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the the common prefix queries with TPC-H data against the
    expected output.
    """
    common_prefix_pipeline_test_data.run_e2e_test(
        get_sample_graph, sqlite_tpch_db_context
    )
