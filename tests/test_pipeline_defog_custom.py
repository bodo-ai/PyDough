"""
Integration tests for the PyDough workflow on custom queries using the defog.ai
schemas.
"""

import re
from collections.abc import Callable

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df, to_sql
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.test_pydough_functions.bad_pydough_functions import (
    bad_lpad_1,
    bad_lpad_2,
    bad_lpad_3,
    bad_lpad_4,
    bad_lpad_5,
    bad_lpad_6,
    bad_lpad_7,
    bad_lpad_8,
    bad_round1,
    bad_round2,
    bad_rpad_1,
    bad_rpad_2,
    bad_rpad_3,
    bad_rpad_4,
    bad_rpad_5,
    bad_rpad_6,
    bad_rpad_7,
    bad_rpad_8,
)
from tests.test_pydough_functions.simple_pydough_functions import (
    cumulative_stock_analysis,
    exponentiation,
    find,
    hour_minute_day,
    minutes_seconds_datediff,
    multi_partition_access_1,
    multi_partition_access_2,
    multi_partition_access_3,
    multi_partition_access_4,
    multi_partition_access_5,
    multi_partition_access_6,
    padding_functions,
    replace,
    sign,
    simple_week_sampler,
    step_slicing,
    strip,
    time_threshold_reached,
    transaction_week_sampler,
    week_offset,
    window_sliding_frame_relsize,
    window_sliding_frame_relsum,
    years_months_days_hours_datediff,
)

from .testing_utilities import PyDoughPandasTest, graph_fetcher, run_e2e_error_test


# Helper functions for week calculations
def get_start_of_week(dt: pd.Timestamp | str, start_of_week: DayOfWeek):
    """
    Calculate the start of week date for a given datetime
    Args:
        dt : The datetime to find the start of week for
        start_of_week: Enum value representing which day is considered
                        the start of the week (e.g., DayOfWeek.MONDAY)

    Returns:
        The start of the week for the given datetime
    """
    # Convert to pandas datetime if not already
    dt_ts: pd.Timestamp = pd.to_datetime(dt)
    # Get the day of week (0-6, where 0 is Monday)
    weekday: int = dt_ts.weekday()
    # Calculate days to subtract to get to start of week
    days_to_subtract: int = (weekday - start_of_week.pandas_dow) % 7
    # Get start of week and set to midnight
    sow: pd.Timestamp = dt_ts - pd.Timedelta(days=days_to_subtract)
    # Return only year, month, day
    return pd.Timestamp(sow.year, sow.month, sow.day)


def get_day_name(dt: pd.Timestamp):
    """
    Get the name of the day for a given datetime
    Args:
        dt: The datetime to get the day name for
    Returns:
        The name of the day for the given datetime
    """
    day_names: list[str] = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    return day_names[dt.weekday()]


def get_day_of_week(
    dt: pd.Timestamp, start_of_week: DayOfWeek, start_week_as_zero: bool
):
    """
    Calculate day of week (0-based or 1-based depending on configuration)
    Args:
        dt: The datetime to get the day of week for
        start_of_week: Enum value representing which day is considered
                        the start of the week (e.g., DayOfWeek.MONDAY)
        start_week_as_zero: Whether to start counting from 0 or 1
    """
    # Get days since start of week
    start_of_week_date: pd.Timestamp = get_start_of_week(dt, start_of_week)
    days_since_start: int = (dt - start_of_week_date).days
    # Adjust based on whether we start counting from 0 or 1
    return days_since_start if start_week_as_zero else days_since_start + 1


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_1,
                "Broker",
                lambda: pd.DataFrame(
                    {"symbol": ["AAPL", "AMZN", "BRK.B", "FB", "GOOG"]}
                ),
                "multi_partition_access_1",
            ),
            id="multi_partition_access_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_2,
                "Broker",
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
                "multi_partition_access_2",
            ),
            id="multi_partition_access_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_3,
                "Broker",
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
                "multi_partition_access_3",
            ),
            id="multi_partition_access_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_4,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (3, 4, 5, 6, 7, 8, 9, 40, 41, 42, 43, 47, 48, 49)
                        ],
                    }
                ),
                "multi_partition_access_4",
            ),
            id="multi_partition_access_4",
        ),
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_5,
                "Broker",
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
                "multi_partition_access_5",
            ),
            id="multi_partition_access_5",
        ),
        pytest.param(
            PyDoughPandasTest(
                multi_partition_access_6,
                "Broker",
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
                "multi_partition_access_6",
            ),
            id="multi_partition_access_6",
        ),
        pytest.param(
            PyDoughPandasTest(
                cumulative_stock_analysis,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "date_time": [
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
                            "2023-04-02 11:00:00",
                            "2023-04-02 11:45:00",
                            "2023-04-02 12:30:00",
                            "2023-04-02 13:15:00",
                            "2023-04-02 14:00:00",
                            "2023-04-02 14:45:00",
                            "2023-04-02 15:30:00",
                            "2023-04-02 16:15:00",
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
                        "txn_within_day": [
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            9,
                            10,
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            9,
                            1,
                            2,
                            3,
                            4,
                            5,
                            6,
                            7,
                            8,
                            9,
                        ],
                        "n_buys_within_day": [
                            1,
                            1,
                            2,
                            2,
                            3,
                            3,
                            4,
                            4,
                            5,
                            5,
                            0,
                            0,
                            1,
                            1,
                            2,
                            2,
                            3,
                            3,
                            4,
                            0,
                            1,
                            1,
                            2,
                            2,
                            3,
                            3,
                            4,
                            4,
                        ],
                        "pct_apple_txns": [
                            100.0,
                            50.0,
                            66.67,
                            50.0,
                            40.0,
                            33.33,
                            28.57,
                            25.0,
                            22.22,
                            20.0,
                            27.27,
                            33.33,
                            30.77,
                            28.57,
                            26.67,
                            25.0,
                            23.53,
                            22.22,
                            21.05,
                            20.0,
                            23.81,
                            22.73,
                            21.74,
                            20.83,
                            20.0,
                            19.23,
                            18.52,
                            17.86,
                        ],
                        "share_change": [
                            100,
                            50,
                            60,
                            35,
                            40,
                            -35,
                            -34,
                            -134,
                            -84,
                            -164,
                            -214,
                            -219,
                            -204,
                            -206,
                            -156,
                            -157,
                            -82,
                            -107,
                            -47,
                            -87,
                            -79,
                            -99,
                            -96,
                            -156,
                            -155,
                            -245,
                            -205,
                            -275,
                        ],
                        "rolling_avg_amount": [
                            15000.0,
                            14500.0,
                            20333.33,
                            16375.0,
                            15600.0,
                            15500.0,
                            70428.57,
                            63250.0,
                            57444.44,
                            52820.0,
                            48706.82,
                            45986.25,
                            42661.73,
                            39973.32,
                            37985.1,
                            60704.78,
                            57712.96,
                            54814.32,
                            52376.99,
                            50324.14,
                            49157.08,
                            47091.99,
                            45373.47,
                            43996.66,
                            58336.79,
                            56552.59,
                            54787.31,
                            53186.87,
                        ],
                    }
                ),
                "cumulative_stock_analysis",
            ),
            id="cumulative_stock_analysis",
        ),
        pytest.param(
            PyDoughPandasTest(
                time_threshold_reached,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "date_time": [
                            "2023-01-15 10:00:00",
                            "2023-01-16 10:30:00",
                            "2023-01-30 13:15:00",
                            "2023-02-20 11:30:00",
                            "2023-02-28 16:00:00",
                            "2023-03-25 14:45:00",
                            "2023-03-30 09:45:00",
                            "2023-04-01 13:15:00",
                            "2023-04-02 14:45:00",
                            "2023-04-03 13:15:00",
                        ],
                    }
                ),
                "time_threshold_reached",
            ),
            id="time_threshold_reached",
        ),
        pytest.param(
            PyDoughPandasTest(
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
                "hour_minute_day",
            ),
            id="hour_minute_day",
        ),
        pytest.param(
            PyDoughPandasTest(
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
                "exponentiation",
            ),
            id="exponentiation",
        ),
        pytest.param(
            PyDoughPandasTest(
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
                "years_months_days_hours_datediff",
            ),
            id="years_months_days_hours_datediff",
        ),
        pytest.param(
            PyDoughPandasTest(
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
                "minutes_seconds_datediff",
            ),
            id="minutes_seconds_datediff",
        ),
        pytest.param(
            PyDoughPandasTest(
                padding_functions,
                "Broker",
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
                    ref_rpad="Cust0001**********************",
                    ref_lpad="**********************Cust0001",
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
                "padding_functions",
            ),
            id="padding_functions",
        ),
        pytest.param(
            PyDoughPandasTest(
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
                    neg_pos_step=lambda x: x["name"].str[-12:2:1],
                    neg_neg_step=lambda x: x["name"].str[-4:-2:1],
                    inbetween_chars=lambda x: x["name"].str[1:-1:1],
                    empty1=lambda x: x["name"].str[2:2:1],
                    empty2=lambda x: x["name"].str[-2:-2:1],
                    empty3=lambda x: x["name"].str[-2:-4:1],
                    empty4=lambda x: x["name"].str[4:2:1],
                    oob1=lambda x: x["name"].str[100:200:1],
                    oob2=lambda x: x["name"].str[-200:-100:1],
                    oob3=lambda x: x["name"].str[100::1],
                    oob4=lambda x: x["name"].str[-200::1],
                    oob5=lambda x: x["name"].str[:100:1],
                    oob6=lambda x: x["name"].str[:-200:1],
                    oob7=lambda x: x["name"].str[100:-200:1],
                    oob8=lambda x: x["name"].str[-200:100:1],
                    oob9=lambda x: x["name"].str[100:-1:1],
                    oob10=lambda x: x["name"].str[-100:-1:1],
                    oob11=lambda x: x["name"].str[-3:100:1],
                    oob12=lambda x: x["name"].str[-3:-100:1],
                    zero1=lambda x: x["name"].str[0:0:1],
                    zero2=lambda x: x["name"].str[0:1:1],
                    zero3=lambda x: x["name"].str[-1:0:1],
                    zero4=lambda x: x["name"].str[1:0:1],
                    zero5=lambda x: x["name"].str[0:-1:1],
                    zero6=lambda x: x["name"].str[0:-20:1],
                    zero7=lambda x: x["name"].str[0:100:1],
                    zero8=lambda x: x["name"].str[20:0:1],
                    zero9=lambda x: x["name"].str[-20:0:1],
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
                "step_slicing",
            ),
            id="step_slicing",
        ),
        pytest.param(
            PyDoughPandasTest(
                sign,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "high": [83.0, 83.6, 84.2, 84.8, 85.4],
                    }
                ).assign(
                    high_neg=lambda x: x["high"] * -1,
                    high_zero=lambda x: x["high"] * 0,
                    sign_high=1,
                    sign_high_neg=-1,
                    sign_high_zero=0,
                ),
                "sign",
            ),
            id="sign",
        ),
        pytest.param(
            PyDoughPandasTest(
                find,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "name": ["Alex Rodriguez"],
                        "idx_Alex": ["Alex Rodriguez".find("Alex")],
                        "idx_Rodriguez": ["Alex Rodriguez".find("Rodriguez")],
                        "idx_bob": ["Alex Rodriguez".find("bob")],
                        "idx_e": ["Alex Rodriguez".find("e")],
                        "idx_space": ["Alex Rodriguez".find(" ")],
                        "idx_of_R": ["Alex Rodriguez".find("R")],
                        "idx_of_Alex_Rodriguez": [
                            "Alex Rodriguez".find("Alex Rodriguez")
                        ],
                    }
                ),
                "find",
            ),
            id="find",
        ),
        pytest.param(
            PyDoughPandasTest(
                strip,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "stripped_name": [""],
                        "stripped_name1": ["Alex Rodriguez"],
                        "stripped_name_with_chars": ["x Rodrigu"],
                        "stripped_alt_name1": ["Alex Rodriguez"],
                        "stripped_alt_name2": ["Alex Rodriguez"],
                        "stripped_alt_name3": ["Alex Rodriguez"],
                        "stripped_alt_name4": ["Alex Rodriguez"],
                    }
                ),
                "strip",
            ),
            id="strip",
        ),
        pytest.param(
            PyDoughPandasTest(
                replace,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "replaced_name": ["Alexander Rodriguez"],
                        "removed_name": [" Rodriguez"],
                        "case_name": ["Alex Rodriguez"],
                        "replace_empty_text": [""],
                        "replace_with_empty_pattern": ["abc"],
                        "remove_substring": ["bc"],
                        "empty_all": [""],
                        "substring_not_found": ["hello"],
                        "overlapping_matches": ["ba"],
                        "multiple_occurrences": ["b b b"],
                        "case_sensitive": ["Apple"],
                        "unicode_handling": ["cafe"],
                        "special_character_replace": ["abc"],
                        "longer_replacement": ["xyz"],
                        "shorter_replacement": ["xx"],
                        "same_value_args": ["foofoo"],
                        "nested_like_replace": ["abcabcabcabc"],
                    }
                ),
                "replace",
            ),
            id="replace",
        ),
        pytest.param(
            PyDoughPandasTest(
                week_offset,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "date_time": [
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
                            "2023-01-15 10:00:00",
                            "2023-01-16 10:30:00",
                            "2023-02-20 11:30:00",
                            "2023-03-25 14:45:00",
                            "2023-01-30 13:15:00",
                            "2023-02-28 16:00:00",
                            "2023-03-30 09:45:00",
                        ],
                        "week_adj1": [
                            "2023-04-09 09:30:00",
                            "2023-04-09 10:15:00",
                            "2023-04-09 11:00:00",
                            "2023-04-09 11:45:00",
                            "2023-04-09 12:30:00",
                            "2023-04-09 13:15:00",
                            "2023-04-09 14:00:00",
                            "2023-04-09 14:45:00",
                            "2023-04-09 15:30:00",
                            "2023-04-09 16:15:00",
                            "2023-04-10 09:30:00",
                            "2023-04-10 10:15:00",
                            "2023-04-10 11:00:00",
                            "2023-04-10 11:45:00",
                            "2023-04-10 12:30:00",
                            "2023-04-10 13:15:00",
                            "2023-04-10 14:00:00",
                            "2023-04-10 14:45:00",
                            "2023-04-10 15:30:00",
                            "2023-04-10 16:15:00",
                            "2023-01-22 10:00:00",
                            "2023-01-23 10:30:00",
                            "2023-02-27 11:30:00",
                            "2023-04-01 14:45:00",
                            "2023-02-06 13:15:00",
                            "2023-03-07 16:00:00",
                            "2023-04-06 09:45:00",
                        ],
                        "week_adj2": [
                            "2023-03-26 09:30:00",
                            "2023-03-26 10:15:00",
                            "2023-03-26 11:00:00",
                            "2023-03-26 11:45:00",
                            "2023-03-26 12:30:00",
                            "2023-03-26 13:15:00",
                            "2023-03-26 14:00:00",
                            "2023-03-26 14:45:00",
                            "2023-03-26 15:30:00",
                            "2023-03-26 16:15:00",
                            "2023-03-27 09:30:00",
                            "2023-03-27 10:15:00",
                            "2023-03-27 11:00:00",
                            "2023-03-27 11:45:00",
                            "2023-03-27 12:30:00",
                            "2023-03-27 13:15:00",
                            "2023-03-27 14:00:00",
                            "2023-03-27 14:45:00",
                            "2023-03-27 15:30:00",
                            "2023-03-27 16:15:00",
                            "2023-01-08 10:00:00",
                            "2023-01-09 10:30:00",
                            "2023-02-13 11:30:00",
                            "2023-03-18 14:45:00",
                            "2023-01-23 13:15:00",
                            "2023-02-21 16:00:00",
                            "2023-03-23 09:45:00",
                        ],
                        "week_adj3": [
                            "2023-04-16 10:30:00",
                            "2023-04-16 11:15:00",
                            "2023-04-16 12:00:00",
                            "2023-04-16 12:45:00",
                            "2023-04-16 13:30:00",
                            "2023-04-16 14:15:00",
                            "2023-04-16 15:00:00",
                            "2023-04-16 15:45:00",
                            "2023-04-16 16:30:00",
                            "2023-04-16 17:15:00",
                            "2023-04-17 10:30:00",
                            "2023-04-17 11:15:00",
                            "2023-04-17 12:00:00",
                            "2023-04-17 12:45:00",
                            "2023-04-17 13:30:00",
                            "2023-04-17 14:15:00",
                            "2023-04-17 15:00:00",
                            "2023-04-17 15:45:00",
                            "2023-04-17 16:30:00",
                            "2023-04-17 17:15:00",
                            "2023-01-29 11:00:00",
                            "2023-01-30 11:30:00",
                            "2023-03-06 12:30:00",
                            "2023-04-08 15:45:00",
                            "2023-02-13 14:15:00",
                            "2023-03-14 17:00:00",
                            "2023-04-13 10:45:00",
                        ],
                        "week_adj4": [
                            "2023-04-16 09:29:59",
                            "2023-04-16 10:14:59",
                            "2023-04-16 10:59:59",
                            "2023-04-16 11:44:59",
                            "2023-04-16 12:29:59",
                            "2023-04-16 13:14:59",
                            "2023-04-16 13:59:59",
                            "2023-04-16 14:44:59",
                            "2023-04-16 15:29:59",
                            "2023-04-16 16:14:59",
                            "2023-04-17 09:29:59",
                            "2023-04-17 10:14:59",
                            "2023-04-17 10:59:59",
                            "2023-04-17 11:44:59",
                            "2023-04-17 12:29:59",
                            "2023-04-17 13:14:59",
                            "2023-04-17 13:59:59",
                            "2023-04-17 14:44:59",
                            "2023-04-17 15:29:59",
                            "2023-04-17 16:14:59",
                            "2023-01-29 09:59:59",
                            "2023-01-30 10:29:59",
                            "2023-03-06 11:29:59",
                            "2023-04-08 14:44:59",
                            "2023-02-13 13:14:59",
                            "2023-03-14 15:59:59",
                            "2023-04-13 09:44:59",
                        ],
                        "week_adj5": [
                            "2023-04-17 09:30:00",
                            "2023-04-17 10:15:00",
                            "2023-04-17 11:00:00",
                            "2023-04-17 11:45:00",
                            "2023-04-17 12:30:00",
                            "2023-04-17 13:15:00",
                            "2023-04-17 14:00:00",
                            "2023-04-17 14:45:00",
                            "2023-04-17 15:30:00",
                            "2023-04-17 16:15:00",
                            "2023-04-18 09:30:00",
                            "2023-04-18 10:15:00",
                            "2023-04-18 11:00:00",
                            "2023-04-18 11:45:00",
                            "2023-04-18 12:30:00",
                            "2023-04-18 13:15:00",
                            "2023-04-18 14:00:00",
                            "2023-04-18 14:45:00",
                            "2023-04-18 15:30:00",
                            "2023-04-18 16:15:00",
                            "2023-01-30 10:00:00",
                            "2023-01-31 10:30:00",
                            "2023-03-07 11:30:00",
                            "2023-04-09 14:45:00",
                            "2023-02-14 13:15:00",
                            "2023-03-15 16:00:00",
                            "2023-04-14 09:45:00",
                        ],
                        "week_adj6": [
                            "2023-04-16 09:29:00",
                            "2023-04-16 10:14:00",
                            "2023-04-16 10:59:00",
                            "2023-04-16 11:44:00",
                            "2023-04-16 12:29:00",
                            "2023-04-16 13:14:00",
                            "2023-04-16 13:59:00",
                            "2023-04-16 14:44:00",
                            "2023-04-16 15:29:00",
                            "2023-04-16 16:14:00",
                            "2023-04-17 09:29:00",
                            "2023-04-17 10:14:00",
                            "2023-04-17 10:59:00",
                            "2023-04-17 11:44:00",
                            "2023-04-17 12:29:00",
                            "2023-04-17 13:14:00",
                            "2023-04-17 13:59:00",
                            "2023-04-17 14:44:00",
                            "2023-04-17 15:29:00",
                            "2023-04-17 16:14:00",
                            "2023-01-29 09:59:00",
                            "2023-01-30 10:29:00",
                            "2023-03-06 11:29:00",
                            "2023-04-08 14:44:00",
                            "2023-02-13 13:14:00",
                            "2023-03-14 15:59:00",
                            "2023-04-13 09:44:00",
                        ],
                        "week_adj7": [
                            "2023-05-16 09:30:00",
                            "2023-05-16 10:15:00",
                            "2023-05-16 11:00:00",
                            "2023-05-16 11:45:00",
                            "2023-05-16 12:30:00",
                            "2023-05-16 13:15:00",
                            "2023-05-16 14:00:00",
                            "2023-05-16 14:45:00",
                            "2023-05-16 15:30:00",
                            "2023-05-16 16:15:00",
                            "2023-05-17 09:30:00",
                            "2023-05-17 10:15:00",
                            "2023-05-17 11:00:00",
                            "2023-05-17 11:45:00",
                            "2023-05-17 12:30:00",
                            "2023-05-17 13:15:00",
                            "2023-05-17 14:00:00",
                            "2023-05-17 14:45:00",
                            "2023-05-17 15:30:00",
                            "2023-05-17 16:15:00",
                            "2023-03-01 10:00:00",
                            "2023-03-02 10:30:00",
                            "2023-04-03 11:30:00",
                            "2023-05-09 14:45:00",
                            "2023-03-16 13:15:00",
                            "2023-04-11 16:00:00",
                            "2023-05-14 09:45:00",
                        ],
                        "week_adj8": [
                            "2024-04-16 09:30:00",
                            "2024-04-16 10:15:00",
                            "2024-04-16 11:00:00",
                            "2024-04-16 11:45:00",
                            "2024-04-16 12:30:00",
                            "2024-04-16 13:15:00",
                            "2024-04-16 14:00:00",
                            "2024-04-16 14:45:00",
                            "2024-04-16 15:30:00",
                            "2024-04-16 16:15:00",
                            "2024-04-17 09:30:00",
                            "2024-04-17 10:15:00",
                            "2024-04-17 11:00:00",
                            "2024-04-17 11:45:00",
                            "2024-04-17 12:30:00",
                            "2024-04-17 13:15:00",
                            "2024-04-17 14:00:00",
                            "2024-04-17 14:45:00",
                            "2024-04-17 15:30:00",
                            "2024-04-17 16:15:00",
                            "2024-01-29 10:00:00",
                            "2024-01-30 10:30:00",
                            "2024-03-05 11:30:00",
                            "2024-04-08 14:45:00",
                            "2024-02-13 13:15:00",
                            "2024-03-13 16:00:00",
                            "2024-04-13 09:45:00",
                        ],
                    }
                ),
                "week_offset",
            ),
            id="week_offset",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_sliding_frame_relsize,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{txn:03d}" for txn in [44, 45, 48, 46, 49, 47, 50, 1]
                        ],
                        "w1": [1, 2, 3, 4, 5, 5, 5, 5],
                        "w2": [1, 2, 3, 1, 2, 1, 2, 1],
                        "w3": [56, 55, 54, 53, 52, 51, 50, 49],
                        "w4": [3, 2, 1, 2, 1, 2, 1, 5],
                        "w5": [0, 1, 2, 3, 4, 5, 6, 7],
                        "w6": [0, 1, 2, 0, 1, 0, 1, 0],
                        "w7": [6, 7, 8, 9, 9, 9, 9, 9],
                        "w8": [3, 3, 3, 2, 2, 2, 2, 5],
                    }
                ),
                "window_sliding_frame_relsize",
            ),
            id="window_sliding_frame_relsize",
        ),
        pytest.param(
            PyDoughPandasTest(
                window_sliding_frame_relsum,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{txn:03d}" for txn in [44, 45, 48, 46, 49, 47, 50, 1]
                        ],
                        "w1": [262, 187, 137, 197, 187, 195, 215, 190],
                        "w2": [200, 120, 40, 62, 2, 35, 30, 375],
                        "w3": [2798, 2718, 2638, 2598, 2538, 2536, 2531, 2501],
                        "w4": [200, 120, 40, 62, 2, 35, 30, 375],
                        "w5": [160, 200, 260, 262, 267, 297, 397, 447],
                        "w6": [160, 200, 200, 62, 62, 35, 35, 150],
                        "w7": [None, 80, 160, 200, 260, 262, 187, 137],
                        "w8": [None, 80, 160, None, 60, None, 5, None],
                    }
                ),
                "window_sliding_frame_relsum",
            ),
            id="window_sliding_frame_relsum",
        ),
    ],
)
def defog_custom_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests on custom queries using the defog.ai databases.
    Returns an instance of PyDoughPandasTest containing information about the
    test.
    """
    return request.param


def test_pipeline_until_relational_defog(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
    defog_graphs: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
):
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on
    custom questions using the defog.ai schemas.
    """
    file_path: str = get_plan_test_filename(defog_custom_pipeline_test_data.test_name)
    defog_custom_pipeline_test_data.run_relational_test(
        defog_graphs, file_path, update_tests
    )


@pytest.mark.execute
def test_pipeline_e2e_defog_custom(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on custom questions using the defog.ai
    schemas.
    """
    defog_custom_pipeline_test_data.run_e2e_test(defog_graphs, sqlite_defog_connection)


@pytest.mark.parametrize(
    "pydough_impl, graph_name, error_message",
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
        pytest.param(
            bad_round1,
            "Broker",
            "Unsupported argument 0.5 for ROUND.The precision argument should be an integer literal.",
            id="bad_round1",
        ),
        pytest.param(
            bad_round2,
            "Broker",
            re.escape(
                "Invalid operator invocation 'ROUND(high, -0.5, 2)': Expected between 1 and 2 arguments inclusive, received 3."
            ),
            id="bad_round2",
        ),
    ],
)
def test_defog_e2e_errors(
    pydough_impl: Callable[[], UnqualifiedNode],
    graph_name: str,
    error_message: str,
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised for defog database.
    """
    graph: GraphMetadata = defog_graphs(graph_name)
    run_e2e_error_test(
        pydough_impl, error_message, graph, database=sqlite_defog_connection
    )


@pytest.mark.execute
def test_pipeline_e2e_defog_simple_week(
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
    week_handling_config: PyDoughConfigs,
):
    """
    Test executing simple_week_sampler using the defog.ai schemas with different
    week configurations, comparing against expected results.
    """
    graph: GraphMetadata = defog_graphs("Broker")
    root: UnqualifiedNode = init_pydough_context(graph)(simple_week_sampler)()
    result: pd.DataFrame = to_df(
        root,
        metadata=graph,
        database=sqlite_defog_connection,
        config=week_handling_config,
    )

    # Generate expected DataFrame based on week_handling_config
    start_of_week = week_handling_config.start_of_week
    start_week_as_zero = week_handling_config.start_week_as_zero

    x_dt = pd.Timestamp(2025, 3, 10, 11, 0, 0)
    y_dt = pd.Timestamp(2025, 3, 14, 11, 0, 0)
    y_dt2 = pd.Timestamp(2025, 3, 15, 11, 0, 0)
    y_dt3 = pd.Timestamp(2025, 3, 16, 11, 0, 0)
    y_dt4 = pd.Timestamp(2025, 3, 17, 11, 0, 0)
    y_dt5 = pd.Timestamp(2025, 3, 18, 11, 0, 0)
    y_dt6 = pd.Timestamp(2025, 3, 19, 11, 0, 0)
    y_dt7 = pd.Timestamp(2025, 3, 20, 11, 0, 0)
    y_dt8 = pd.Timestamp(2025, 3, 21, 11, 0, 0)

    # Calculate weeks difference
    x_sow = get_start_of_week(x_dt, start_of_week)
    y_sow = get_start_of_week(y_dt, start_of_week)
    weeks_diff = (y_sow - x_sow).days // 7

    # Create lists to store calculated values
    dates = [y_dt, y_dt2, y_dt3, y_dt4, y_dt5, y_dt6, y_dt7, y_dt8]
    sows = []
    daynames = []
    dayofweeks = []

    # Calculate values for each date in a loop
    for dt in dates:
        # Calculate start of week
        sow = get_start_of_week(dt, start_of_week).strftime("%Y-%m-%d")
        sows.append(sow)

        # Get day name
        dayname = dt.day_name()
        daynames.append(dayname)

        # Calculate day of week
        dayofweek = get_day_of_week(dt, start_of_week, start_week_as_zero)
        dayofweeks.append(dayofweek)

    # Create dictionary for DataFrame
    data_dict = {"weeks_diff": [weeks_diff]}

    # Add start of week columns
    for i in range(len(dates)):
        data_dict[f"sow{i + 1}"] = [sows[i]]

    # Add day name columns
    for i in range(len(dates)):
        data_dict[f"dayname{i + 1}"] = [daynames[i]]

    # Add day of week columns
    for i in range(len(dates)):
        data_dict[f"dayofweek{i + 1}"] = [dayofweeks[i]]

    # Create DataFrame with expected results
    expected_df = pd.DataFrame(data_dict)
    pd.testing.assert_frame_equal(result, expected_df)


@pytest.mark.execute
def test_pipeline_e2e_defog_transaction_week(
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
    week_handling_config: PyDoughConfigs,
):
    """
    Test executing transaction_week_sampler using the defog.ai schemas with
    different week configurations, comparing against expected results.
    """
    graph: GraphMetadata = defog_graphs("Broker")
    root: UnqualifiedNode = init_pydough_context(graph)(transaction_week_sampler)()
    result: pd.DataFrame = to_df(
        root,
        metadata=graph,
        database=sqlite_defog_connection,
        config=week_handling_config,
    )

    to_sql(
        root,
        metadata=graph,
        database=sqlite_defog_connection,
        config=week_handling_config,
    )

    # Generate expected DataFrame based on week_handling_config
    start_of_week = week_handling_config.start_of_week
    start_week_as_zero = week_handling_config.start_week_as_zero

    # Sample dates from the result DataFrame
    date_times = result["date_time"].tolist()

    # Calculate expected values for each date
    expected_sows = []
    expected_daynames = []
    expected_dayofweeks = []

    for dt in date_times:
        dt = pd.to_datetime(dt)

        # Calculate start of week
        sow = get_start_of_week(dt, start_of_week).strftime("%Y-%m-%d")
        expected_sows.append(sow)

        # Get day name
        dayname = get_day_name(dt)
        expected_daynames.append(dayname)

        # Calculate day of week
        dayofweek = get_day_of_week(dt, start_of_week, start_week_as_zero)
        expected_dayofweeks.append(dayofweek)

    # Create DataFrame with expected results
    expected_df = pd.DataFrame(
        {
            "date_time": date_times,
            "sow": expected_sows,
            "dayname": expected_daynames,
            "dayofweek": expected_dayofweeks,
        }
    )
    pd.testing.assert_frame_equal(result, expected_df)
