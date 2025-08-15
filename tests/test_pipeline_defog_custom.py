"""
Integration tests for the PyDough workflow on custom queries using the defog.ai
schemas.
"""

import datetime
import re
from collections.abc import Callable

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df, to_sql
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.test_pydough_functions.bad_pydough_functions import (
    bad_get_part_1,
    bad_get_part_2,
    bad_get_part_3,
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
    agg_simplification_1,
    agg_simplification_2,
    cumulative_stock_analysis,
    exponentiation,
    find,
    get_part_multiple,
    get_part_single,
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
    str_count,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
            ),
            id="replace",
        ),
        pytest.param(
            PyDoughPandasTest(
                str_count,
                "Broker",
                # Answer
                lambda: pd.DataFrame(
                    {
                        "count_letter": [2],
                        "not_in_letter": [0],
                        "count_lastname": [1],
                        "count_sensitive_lastname": [0],
                        "count_empty": [0],
                        "all_empty": [0],
                        "first_empty": [0],
                        "count_numbers": [1],
                        "count_char_numbers": [2],
                        "no_occurence": [0],
                        "count_spaces": [1],
                        "space_arround": [3],
                        "count_special_chars": [1],
                        "no_overlapping": [2],
                        "no_overlapping_2": [1],
                        "entire_string_match": [1],
                        "longer_substring": [0],
                    }
                ),
                "str_count",
                skip_sql=True,
            ),
            id="str_count",
        ),
        pytest.param(
            PyDoughPandasTest(
                get_part_multiple,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "k": [1, 2, 3, 4],
                        "p1": ["john", "Smith", None, None],
                        "p2": ["doe", "Jane", None, None],
                        "p3": ["john", "smith@email", "com", None],
                        "p4": ["com", "smith@email", "bob", None],
                        "p5": ["555", "987", "8135", None],
                        "p6": ["4567", "987", "555", None],
                        "p7": ["9", "02", None, None],
                        "p8": ["01", "1", None, None],
                        "p9": ["john doe", None, None, None],
                        "p10": ["john doe", None, None, None],
                        "p11": ["john doe", None, None, None],
                        "p12": ["john doe", None, None, None],
                        "p13": ["john doe", None, None, None],
                        "p14": [None, None, None, None],
                        "p15": ["john", "Jane", "Bob", "Samantha"],
                        "p16": ["", None, None, None],
                        "p17": ["", "", "", None],
                        "p18": ["9", "", "", None],
                    }
                ),
                "get_part_multiple",
                skip_sql=True,
            ),
            id="get_part_multiple",
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
                skip_sql=True,
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
                skip_sql=True,
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
                skip_sql=True,
            ),
            id="window_sliding_frame_relsum",
        ),
        pytest.param(
            PyDoughPandasTest(
                agg_simplification_1,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "aug_exchange": [None, 4, 6, 8],
                        "su1": [3, 4, 10, 4],
                        "su2": [6, 8, 20, 8],
                        "su3": [-3, -4, -10, -4],
                        "su4": [-9, -12, -30, -12],
                        "su5": [0, 0, 0, 0],
                        "su6": [1.5, 2.0, 5.0, 2.0],
                        "su7": [0, 0, 0, 0],
                        "su8": [0, 4, 6, 8],
                        "co1": [3, 4, 10, 4],
                        "co2": [3, 4, 10, 4],
                        "co3": [3, 4, 10, 4],
                        "co4": [3, 4, 10, 4],
                        "co5": [3, 4, 10, 4],
                        "co6": [3, 4, 10, 4],
                        "co7": [0, 0, 0, 0],
                        "co8": [0, 4, 10, 4],
                        "nd1": [1, 1, 1, 1],
                        "nd2": [1, 1, 1, 1],
                        "nd3": [1, 1, 1, 1],
                        "nd4": [1, 1, 1, 1],
                        "nd5": [1, 1, 1, 1],
                        "nd6": [1, 1, 1, 1],
                        "nd7": [0, 0, 0, 0],
                        "nd8": [0, 1, 1, 1],
                        "av1": [1, 1, 1, 1],
                        "av2": [2, 2, 2, 2],
                        "av3": [-1, -1, -1, -1],
                        "av4": [-3, -3, -3, -3],
                        "av5": [0, 0, 0, 0],
                        "av6": [0.5, 0.5, 0.5, 0.5],
                        "av7": [None, None, None, None],
                        "av8": [None, 4, 6, 8],
                        "mi1": [1, 1, 1, 1],
                        "mi2": [2, 2, 2, 2],
                        "mi3": [-1, -1, -1, -1],
                        "mi4": [-3, -3, -3, -3],
                        "mi5": [0, 0, 0, 0],
                        "mi6": [0.5, 0.5, 0.5, 0.5],
                        "mi7": [None, None, None, None],
                        "mi8": [None, 4, 6, 8],
                        "ma1": [1, 1, 1, 1],
                        "ma2": [2, 2, 2, 2],
                        "ma3": [-1, -1, -1, -1],
                        "ma4": [-3, -3, -3, -3],
                        "ma5": [0, 0, 0, 0],
                        "ma6": [0.5, 0.5, 0.5, 0.5],
                        "ma7": [None, None, None, None],
                        "ma8": [None, 4, 6, 8],
                        "an1": [1, 1, 1, 1],
                        "an2": [2, 2, 2, 2],
                        "an3": [-1, -1, -1, -1],
                        "an4": [-3, -3, -3, -3],
                        "an5": [0, 0, 0, 0],
                        "an6": [0.5, 0.5, 0.5, 0.5],
                        "an7": [None, None, None, None],
                        "an8": [None, 4, 6, 8],
                        "me1": [1.0, 1.0, 1.0, 1.0],
                        "me2": [2.0, 2.0, 2.0, 2.0],
                        "me3": [-1.0, -1.0, -1.0, -1.0],
                        "me4": [-3.0, -3.0, -3.0, -3.0],
                        "me5": [0.0, 0.0, 0.0, 0.0],
                        "me6": [0.5, 0.5, 0.5, 0.5],
                        "me7": [None, None, None, None],
                        "me8": [None, 4.0, 6.0, 8.0],
                        "qu1": [1, 1, 1, 1],
                        "qu2": [2, 2, 2, 2],
                        "qu3": [-1, -1, -1, -1],
                        "qu4": [-3, -3, -3, -3],
                        "qu5": [0, 0, 0, 0],
                        "qu6": [0.5, 0.5, 0.5, 0.5],
                        "qu7": [None, None, None, None],
                        "qu8": [None, 4, 6, 8],
                    }
                ),
                "agg_simplification_1",
                order_sensitive=True,
            ),
            id="agg_simplification_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                agg_simplification_2,
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "state": ["CA", "FL", "NJ", "NY", "TX"],
                        "a1": [2, 1, 1, 1, 1],
                        "a2": [7, 3, 3, 4, 3],
                        "a3": [1, 0, 0, 3, 0],
                        "a4": [636307, 99303, 26403, 40008, 225000],
                        "a5": [
                            "555-123-4567",
                            "555-370-2648",
                            "555-246-1357",
                            "555-135-7902",
                            "555-246-8135",
                        ],
                        "a6": [
                            "555-864-2319",
                            "555-864-2319",
                            "555-987-6543",
                            "555-987-6543",
                            "555-753-1904",
                        ],
                        "a7": ["ca", "fl", "nj", "ny", "tx"],
                        "a8": ["ca", "fl", "nj", "ny", "tx"],
                        "a9": ["ca", "fl", "nj", "ny", "tx"],
                    }
                ),
                "agg_simplification_2",
                order_sensitive=True,
            ),
            id="agg_simplification_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                get_part_single,
                "Broker",
                lambda: pd.DataFrame({"last_name": ["Rodriguez"]}),
                "get_part_single",
            ),
            id="get_part_single",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = Broker.CALCULATE("
                " s00 = ABS(13),"  # -> 13
                " s01 = ABS(0),"  # -> 0
                " s02 = ABS(COUNT(customers)),"  # -> COUNT(customers)
                " s03 = ABS(COUNT(customers) + 5),"  # -> COUNT(customers) + 5
                " s04 = ABS(COUNT(customers) * 2),"  # -> COUNT(customers) * 2
                " s05 = ABS(COUNT(customers) / 8.0),"  # -> COUNT(customers) / 8.0
                " s06 = DEFAULT_TO(10, 0),"  # -> 10
                " s07 = DEFAULT_TO(COUNT(customers), 0),"  # -> COUNT(customers)
                " s08 = DEFAULT_TO(ABS(COUNT(customers) - 25), 0),"  # -> ABS(COUNT(customers) - 25)
                " s09 = DEFAULT_TO(COUNT(customers) + 1, 0),"  # -> COUNT(customers) + 1
                " s10 = DEFAULT_TO(COUNT(customers) - 3, 0),"  # -> COUNT(customers) - 3
                " s11 = DEFAULT_TO(COUNT(customers) * -1, 0),"  # -> COUNT(customers) * -1
                " s12 = DEFAULT_TO(COUNT(customers) / 2.5, 0),"  # -> COUNT(customers) / 2.5
                " s13 = DEFAULT_TO(COUNT(customers) > 10, False),"  # -> COUNT(customers) > 10
                " s14 = DEFAULT_TO(COUNT(customers) >= 10, False),"  # -> COUNT(customers) >= 10
                " s15 = DEFAULT_TO(COUNT(customers) == 20, False),"  # -> COUNT(customers) == 10
                " s16 = DEFAULT_TO(COUNT(customers) != 25, False),"  # -> COUNT(customers) != 20
                " s17 = DEFAULT_TO(COUNT(customers) < 25, False),"  # -> COUNT(customers) < 25
                " s18 = DEFAULT_TO(COUNT(customers) <= 25, False),"  # -> COUNT(customers) <= 25
                " s19 = COUNT(DEFAULT_TO(customers.name, '')),"  # -> COUNT(customers)
                " s20 = ABS(DEFAULT_TO(AVG(ABS(DEFAULT_TO(LENGTH(customers.name), 0))), 0)),"  # -> AVG(DEFAULT_TO(LENGTH(customers.name), ''))
                " s21 = PRESENT(COUNT(customers)),"  # -> True
                " s22 = PRESENT(1) >= 0,"  # -> True
                " s23 = ABSENT(1) >= 0,"  # -> True
                ")",
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "s00": [13],
                        "s01": [0],
                        "s02": [20],
                        "s03": [25],
                        "s04": [40],
                        "s05": [2.5],
                        "s06": [10],
                        "s07": [20],
                        "s08": [5],
                        "s09": [21],
                        "s10": [17],
                        "s11": [-20],
                        "s12": [8.0],
                        "s13": [1],
                        "s14": [1],
                        "s15": [1],
                        "s16": [1],
                        "s17": [1],
                        "s18": [1],
                        "s19": [20],
                        "s20": [12.3],
                        "s21": [1],
                        "s22": [1],
                        "s23": [1],
                    }
                ),
                "simplification_1",
            ),
            id="simplification_1",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = Broker.CALCULATE("
                " s00 = DEFAULT_TO(None, 0) == 0,"  # -> True
                " s01 = DEFAULT_TO(None, 0) != 0,"  # -> False
                " s02 = DEFAULT_TO(None, 0) >= 0,"  # -> True
                " s03 = DEFAULT_TO(None, 0) > 0,"  # -> False
                " s04 = DEFAULT_TO(None, 0) <= 0,"  # -> True
                " s05 = DEFAULT_TO(None, 0) < 0,"  # -> False
                " s06 = DEFAULT_TO(None, 0) == None,"  # -> None
                " s07 = DEFAULT_TO(None, 0) != None,"  # -> None
                " s08 = DEFAULT_TO(None, 0) >= None,"  # -> None
                " s09 = DEFAULT_TO(None, 0) > None,"  # -> None
                " s10 = DEFAULT_TO(None, 0) <= None,"  # -> None
                " s11 = DEFAULT_TO(None, 0) < None,"  # -> None
                " s12 = DEFAULT_TO(None, 'ab') == 'cd',"  # -> False
                " s13 = DEFAULT_TO(None, 'ab') != 'cd',"  # -> True
                " s14 = DEFAULT_TO(None, 'ab') >= 'cd',"  # -> False
                " s15 = DEFAULT_TO(None, 'ab') > 'cd',"  # -> False
                " s16 = DEFAULT_TO(None, 'ab') <= 'cd',"  # -> True
                " s17 = DEFAULT_TO(None, 'ab') < 'cd',"  # -> True
                " s18 = True | (COUNT(customers) > 10),"  # -> True
                " s19 = False & (COUNT(customers) > 10),"  # -> False
                " s20 = False | (LENGTH('foo') > 0),"  # -> True
                " s21 = False | (LENGTH('foo') < 0),"  # -> False
                " s22 = True & (LENGTH('foo') > 0),"  # -> True
                " s23 = True & (LENGTH('foo') < 0),"  # -> False
                " s24 = STARTSWITH('a', 'abc'),"  # -> False
                " s25 = STARTSWITH('abc', 'a'),"  # -> True
                " s26 = ENDSWITH('abc', 'c'),"  # -> True
                " s27 = ENDSWITH('abc', 'ab'),"  # -> False
                " s28 = CONTAINS('abc', 'b'),"  # -> True
                " s29 = CONTAINS('abc', 'B'),"  # -> False
                " s30 = LENGTH('alphabet'),"  # -> 8
                " s31 = LOWER('AlPhAbEt'),"  # -> 'alphabet'
                " s32 = UPPER('sOuP'),"  # -> 'SOUP'
                " s33 = True == True,"  # -> True
                " s34 = True != True,"  # -> False
                " s35 = True == False,"  # -> False
                " s36 = True != False,"  # -> True
                " s37 = SQRT(9),"  # -> 3.0
                " s38 = COUNT(customers) == None,"  # -> None
                " s39 = None >= COUNT(customers),"  # -> None
                " s40 = COUNT(customers) > None,"  # -> None
                " s41 = None < COUNT(customers),"  # -> None
                " s42 = None <= COUNT(customers),"  # -> None
                " s43 = None + COUNT(customers),"  # -> None
                " s44 = COUNT(customers) - None,"  # -> None
                " s45 = None * COUNT(customers),"  # -> None
                " s46 = COUNT(customers) / None,"  # -> None
                " s47 = ABS(DEFAULT_TO(LIKE(DEFAULT_TO(MAX(customers.name), ''), '%r%'), 1))"  # -> COALESCE(MAX(sbcustname), '') LIKE '%r%'
                ")",
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "s00": [1],
                        "s01": [0],
                        "s02": [1],
                        "s03": [0],
                        "s04": [1],
                        "s05": [0],
                        "s06": [None],
                        "s07": [None],
                        "s08": [None],
                        "s09": [None],
                        "s10": [None],
                        "s11": [None],
                        "s12": [0],
                        "s13": [1],
                        "s14": [0],
                        "s15": [0],
                        "s16": [1],
                        "s17": [1],
                        "s18": [1],
                        "s19": [0],
                        "s20": [1],
                        "s21": [0],
                        "s22": [1],
                        "s23": [0],
                        "s24": [0],
                        "s25": [1],
                        "s26": [1],
                        "s27": [0],
                        "s28": [1],
                        "s29": [0],
                        "s30": [8],
                        "s31": ["alphabet"],
                        "s32": ["SOUP"],
                        "s33": [1],
                        "s34": [0],
                        "s35": [0],
                        "s36": [1],
                        "s37": [3.0],
                        "s38": [None],
                        "s39": [None],
                        "s40": [None],
                        "s41": [None],
                        "s42": [None],
                        "s43": [None],
                        "s44": [None],
                        "s45": [None],
                        "s46": [None],
                        "s47": [1],
                    }
                ),
                "simplification_2",
            ),
            id="simplification_2",
        ),
        pytest.param(
            PyDoughPandasTest(
                "cust_info = customers.CALCULATE(p=DEFAULT_TO(INTEGER(postal_code), 0))"
                " .CALCULATE("
                " rank = RANKING(by=name.ASC()),"
                " rsum1 = DEFAULT_TO(RELSUM(ABS(p)), 0.1),"
                " rsum2 = DEFAULT_TO(RELSUM(ABS(p), by=name.ASC(), cumulative=True), 0.1),"
                " ravg1 = DEFAULT_TO(RELAVG(ABS(p)), 0.1),"
                " ravg2 = DEFAULT_TO(RELAVG(ABS(p), by=name.ASC(), frame=(None, -1)), 0.1),"
                " rcnt1 = DEFAULT_TO(RELCOUNT(INTEGER(postal_code)), 0.1),"
                " rcnt2 = DEFAULT_TO(RELCOUNT(INTEGER(postal_code), by=name.ASC(), cumulative=True), 0.1),"
                " rsiz1 = DEFAULT_TO(RELSIZE(), 0.1),"
                " rsiz2 = DEFAULT_TO(RELSIZE(by=name.ASC(), frame=(1, None)), 0.1),"
                ")\n"
                "result = Broker.CALCULATE("
                " s00 = MONOTONIC(1, 2, 3),"  # -> True
                " s01 = MONOTONIC(1, 1, 1),"  # -> True
                " s02 = MONOTONIC(1, 0, 3),"  # -> False
                " s03 = MONOTONIC(1, 4, 3),"  # -> False
                " s04 = MONOTONIC(1, 2, 1),"  # -> False
                " s05 = MONOTONIC(1, 0, 1),"  # -> False
                " s06 = MONOTONIC(1, LENGTH('foo'), COUNT(cust_info)),"  # -> 3 <= COUNT(*)
                " s07 = MONOTONIC(10, LENGTH('foo'), COUNT(cust_info)),"  # False
                " s08 = MONOTONIC(COUNT(cust_info), LENGTH('foobar'), 9),"  # -> COUNT(*) <= 6
                " s09 = MONOTONIC(COUNT(cust_info), LENGTH('foobar'), 5),"  # -> False
                " s10 = 13 * 7,"  # -> 91
                " s11 = 42 * LENGTH(''),"  # -> 0
                " s12 = 42 + LENGTH('fizzbuzz'),"  # -> 50
                " s13 = 50 - 15,"  # -> 35
                " s14 = 50 / 2,"  # -> 25
                " s15 = ABS(COUNT(cust_info) * -0.75),"  # -> not simplified
                " s16 = DEFAULT_TO(10, COUNT(cust_info)),"  # -> 10
                " s17 = DEFAULT_TO(None, None, None, COUNT(cust_info)),"  # -> COUNT(*)
                " s18 = DEFAULT_TO(None, None, COUNT(cust_info), None, -1),"  # -> COUNT(*)
                " s19 = STARTSWITH('', 'a'),"  # -> False
                " s20 = STARTSWITH('a', ''),"  # -> True
                " s21 = ENDSWITH('', 'a'),"  # -> False
                " s22 = ENDSWITH('a', ''),"  # -> True
                " s23 = CONTAINS('', 'a'),"  # -> False
                " s24 = CONTAINS('a', ''),"  # -> True
                " s25 = ABS(QUANTILE(ABS(INTEGER(cust_info.postal_code)), 0.25)),"  # -> QUANTILE(ABS(INTEGER(cust_info.postal_code)), 0.25)
                " s26 = ABS(MEDIAN(ABS(INTEGER(cust_info.postal_code)))),"  # -> MEDIAN(ABS(INTEGER(cust_info.postal_code)))
                " s27 = ABS(MIN(cust_info.rank)),"  # -> MIN(cust_info.rank)
                " s28 = ABS(MAX(cust_info.rank)),"  # -> MAX(cust_info.rank)
                " s29 = ABS(ANYTHING(cust_info.rsum1)),"  # -> ANYTHING(cust_info.rsum1)
                " s30 = ROUND(ABS(SUM(cust_info.rsum2)), 2),"  # -> ROUND(SUM(cust_info.rsum2), 2)
                " s31 = ABS(ANYTHING(cust_info.ravg1)),"  # -> ANYTHING(cust_info.ravg1)
                " s32 = ROUND(ABS(SUM(cust_info.ravg2)), 2),"  # -> ROUND(SUM(cust_info.ravg2), 2)
                " s33 = ABS(ANYTHING(cust_info.rcnt1)),"  # -> ANYTHING(cust_info.rcnt1)
                " s34 = ROUND(ABS(SUM(cust_info.rcnt2)), 2),"  # -> ROUND(SUM(cust_info.rcnt2), 2)
                " s35 = ABS(ANYTHING(cust_info.rsiz1)),"  # -> ANYTHING(cust_info.rsiz1)
                " s36 = ROUND(ABS(SUM(cust_info.rsiz2)), 2),"  # -> ROUND(SUM(cust_info.rsiz2), 2)
                ")",
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "s00": [1],
                        "s01": [1],
                        "s02": [0],
                        "s03": [0],
                        "s04": [0],
                        "s05": [0],
                        "s06": [1],
                        "s07": [0],
                        "s08": [0],
                        "s09": [0],
                        "s10": [91],
                        "s11": [0],
                        "s12": [50],
                        "s13": [35],
                        "s14": [25.0],
                        "s15": [15.0],
                        "s16": [10],
                        "s17": [20],
                        "s18": [20],
                        "s19": [0],
                        "s20": [1],
                        "s21": [0],
                        "s22": [1],
                        "s23": [0],
                        "s24": [1],
                        "s25": [10002],
                        "s26": [54050.5],
                        "s27": [1],
                        "s28": [20],
                        "s29": [1027021],
                        "s30": [9096414.0],
                        "s31": [51351.05],
                        "s32": [802375.94],
                        "s33": [20],
                        "s34": [210.0],
                        "s35": [20],
                        "s36": [190.0],
                    }
                ),
                "simplification_3",
            ),
            id="simplification_3",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " transactions"
                " .WHERE(YEAR(date_time) == 2023)"
                " .WHERE((RANKING(by=date_time.ASC()) == 1) | (RANKING(by=date_time.DESC()) == 1))"
                " .CALCULATE("
                " date_time,"
                " s00 = DATETIME(DATETIME(date_time, 'start of week'), '-8 weeks'),"  # -> DATETIME(date_time, 'start of week', '-8 weeks')
                " s01 = QUARTER(date_time) == 0,"  # KEEP_IF(False, PRESENT(date_time))
                " s02 = 1 == QUARTER(date_time),"  # ISIN(MONTH(date_time), [1,2,3])
                " s03 = QUARTER(date_time) == 2,"  # ISIN(MONTH(date_time), [4,5,6])
                " s04 = 3 == QUARTER(date_time),"  # ISIN(MONTH(date_time), [7,8,9])
                " s05 = QUARTER(date_time) == 4,"  # ISIN(MONTH(date_time), [10,11,12])
                " s06 = 5 == QUARTER(date_time),"  # KEEP_IF(False, PRESENT(date_time))
                " s07 = 1 > QUARTER(date_time),"  # KEEP_IF(False, PRESENT(date_time))
                " s08 = QUARTER(date_time) < 2,"  # MONTH(date_time) < 4
                " s09 = 3 > QUARTER(date_time),"  # MONTH(date_time) < 7
                " s10 = QUARTER(date_time) < 4,"  # MONTH(date_time) < 10
                " s11 = 5 > QUARTER(date_time),"  # KEEP_IF(True, PRESENT(date_time))
                " s12 = QUARTER(date_time) <= 0,"  # KEEP_IF(False, PRESENT(date_time))
                " s13 = 1 >= QUARTER(date_time),"  # MONTH(date_time) <= 3
                " s14 = QUARTER(date_time) <= 2,"  # MONTH(date_time) <= 6
                " s15 = 3 >= QUARTER(date_time),"  # MONTH(date_time) <= 9
                " s16 = QUARTER(date_time) <= 4,"  # KEEP_IF(True, PRESENT(date_time))
                " s17 = 0 < QUARTER(date_time),"  # KEEP_IF(True, PRESENT(date_time))
                " s18 = QUARTER(date_time) > 1,"  # MONTH(date_time) > 3
                " s19 = 2 < QUARTER(date_time),"  # MONTH(date_time) > 6
                " s20 = QUARTER(date_time) > 3,"  # MONTH(date_time) > 9
                " s21 = 4 < QUARTER(date_time),"  # KEEP_IF(False, PRESENT(date_time))
                " s22 = 1 <= QUARTER(date_time),"  # KEEP_IF(True, PRESENT(date_time))
                " s23 = QUARTER(date_time) >= 2,"  # MONTH(date_time) >= 4
                " s24 = 3 <= QUARTER(date_time),"  # MONTH(date_time) >= 7
                " s25 = QUARTER(date_time) >= 4,"  # MONTH(date_time) >= 10
                " s26 = 5 <= QUARTER(date_time),"  # KEEP_IF(False, PRESENT(date_time))
                " s27 = QUARTER(date_time) != 0,"  # KEEP_IF(True, PRESENT(date_time))
                " s28 = 1 != QUARTER(date_time),"  # NOT(ISIN(MONTH(date_time), [1,2,3]))
                " s29 = QUARTER(date_time) != 2,"  # NOT(ISIN(MONTH(date_time), [4,5,6]))
                " s30 = 3 != QUARTER(date_time),"  # NOT(ISIN(MONTH(date_time), [7,8,9]))
                " s31 = QUARTER(date_time) != 4,"  # NOT(ISIN(MONTH(date_time), [10,11,12]))
                " s32 = 5 != QUARTER(date_time),"  # KEEP_IF(True, PRESENT(date_time))
                " s33 = YEAR('2024-08-13 12:45:59'),"  # 2024
                " s34 = QUARTER('2024-08-13 12:45:59'),"  # 3
                " s35 = MONTH('2024-08-13 12:45:59'),"  # 8
                " s36 = DAY('2024-08-13 12:45:59'),"  # 13
                " s37 = HOUR('2024-08-13 12:45:59'),"  # 12
                " s38 = MINUTE('2024-08-13 12:45:59'),"  # 45
                " s39 = SECOND('2024-08-13 12:45:59'),"  # 59
                " s40 = YEAR(datetime.date(2020, 1, 31)),"  # 2024
                " s41 = QUARTER(datetime.date(2020, 1, 31)),"  # 1
                " s42 = MONTH(datetime.date(2020, 1, 31)),"  # 1
                " s43 = DAY(datetime.date(2020, 1, 31)),"  # 31
                " s44 = HOUR(datetime.date(2020, 1, 31)),"  # 0
                " s45 = MINUTE(datetime.date(2020, 1, 31)),"  # 0
                " s46 = SECOND(datetime.date(2020, 1, 31)),"  # 0
                " s47 = YEAR(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 2023
                " s48 = QUARTER(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 3
                " s49 = MONTH(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 7
                " s50 = DAY(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 4
                " s51 = HOUR(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 6
                " s52 = MINUTE(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 55
                " s53 = SECOND(datetime.datetime(2023, 7, 4, 6, 55, 0)),"  # 0
                " s54 = YEAR(pd.Timestamp('1999-12-31 23:59:58')),"  # 1999
                " s55 = QUARTER(pd.Timestamp('1999-12-31 23:59:58')),"  # 4
                " s56 = MONTH(pd.Timestamp('1999-12-31 23:59:58')),"  # 12
                " s57 = DAY(pd.Timestamp('1999-12-31 23:59:58')),"  # 31
                " s58 = HOUR(pd.Timestamp('1999-12-31 23:59:58')),"  # 23
                " s59 = MINUTE(pd.Timestamp('1999-12-31 23:59:58')),"  # 59
                " s60 = SECOND(pd.Timestamp('1999-12-31 23:59:58')),"  # 58
                " s61 = MONTH(date_time) == 0,"  # KEEP_IF(False, PRESENT(datetime))
                " s62 = MONTH(date_time) < 1,"  # KEEP_IF(False, PRESENT(datetime))
                " s63 = MONTH(date_time) <= 0,"  # KEEP_IF(False, PRESENT(datetime))
                " s64 = MONTH(date_time) != 0,"  # KEEP_IF(True, PRESENT(datetime))
                " s65 = MONTH(date_time) > 0,"  # KEEP_IF(True, PRESENT(datetime))
                " s66 = MONTH(date_time) >= 1,"  # KEEP_IF(True, PRESENT(datetime))
                " s67 = 0 == DAY(date_time),"  # KEEP_IF(False, PRESENT(datetime))
                " s68 = 1 > DAY(date_time),"  # KEEP_IF(False, PRESENT(datetime))
                " s69 = 0 >= DAY(date_time),"  # KEEP_IF(False, PRESENT(datetime))
                " s70 = 0 != DAY(date_time),"  # KEEP_IF(True, PRESENT(datetime))
                " s71 = 0 < DAY(date_time),"  # KEEP_IF(True, PRESENT(datetime))
                " s72 = 0 <= DAY(date_time),"  # KEEP_IF(True, PRESENT(datetime))
                " s73 = HOUR(date_time) == -1,"  # KEEP_IF(False, PRESENT(datetime))
                " s74 = 61 == MINUTE(date_time),"  # KEEP_IF(False, PRESENT(datetime))
                " s75 = -2 != SECOND(date_time),"  # KEEP_IF(True, PRESENT(datetime))
                " s76 = HOUR(date_time) != 62,"  # KEEP_IF(True, PRESENT(datetime))
                " s77 = MINUTE(date_time) < 0,"  # KEEP_IF(False, PRESENT(datetime))
                " s78 = SECOND(date_time) < 61,"  # KEEP_IF(True, PRESENT(datetime))
                " s79 = HOUR(date_time) <= -1,"  # KEEP_IF(False, PRESENT(datetime))
                " s80 = MINUTE(date_time) <= 60,"  # KEEP_IF(True, PRESENT(datetime))
                " s81 = SECOND(date_time) > -5,"  # KEEP_IF(True, PRESENT(datetime))
                " s82 = HOUR(date_time) > 60,"  # KEEP_IF(False, PRESENT(datetime))
                " s83 = MINUTE(date_time) >= 0,"  # KEEP_IF(True, PRESENT(datetime))
                " s84 = SECOND(date_time) >= 80,"  # KEEP_IF(False, PRESENT(datetime))
                "))",
                "Broker",
                lambda: pd.DataFrame(
                    {
                        "date_time": ["2023-01-15 10:00:00", "2023-04-03 16:15:00"],
                        "s00": ["2022-11-20", "2023-02-05"],
                        "s01": [0, 0],
                        "s02": [1, 0],
                        "s03": [0, 1],
                        "s04": [0, 0],
                        "s05": [0, 0],
                        "s06": [0, 0],
                        "s07": [0, 0],
                        "s08": [1, 0],
                        "s09": [1, 1],
                        "s10": [1, 1],
                        "s11": [1, 1],
                        "s12": [0, 0],
                        "s13": [1, 0],
                        "s14": [1, 1],
                        "s15": [1, 1],
                        "s16": [1, 1],
                        "s17": [1, 1],
                        "s18": [0, 1],
                        "s19": [0, 0],
                        "s20": [0, 0],
                        "s21": [0, 0],
                        "s22": [1, 1],
                        "s23": [0, 1],
                        "s24": [0, 0],
                        "s25": [0, 0],
                        "s26": [0, 0],
                        "s27": [1, 1],
                        "s28": [0, 1],
                        "s29": [1, 0],
                        "s30": [1, 1],
                        "s31": [1, 1],
                        "s32": [1, 1],
                        "s33": [2024, 2024],
                        "s34": [3, 3],
                        "s35": [8, 8],
                        "s36": [13, 13],
                        "s37": [12, 12],
                        "s38": [45, 45],
                        "s39": [59, 59],
                        "s40": [2020, 2020],
                        "s41": [1, 1],
                        "s42": [1, 1],
                        "s43": [31, 31],
                        "s44": [0, 0],
                        "s45": [0, 0],
                        "s46": [0, 0],
                        "s47": [2023, 2023],
                        "s48": [3, 3],
                        "s49": [7, 7],
                        "s50": [4, 4],
                        "s51": [6, 6],
                        "s52": [55, 55],
                        "s53": [0, 0],
                        "s54": [1999, 1999],
                        "s55": [4, 4],
                        "s56": [12, 12],
                        "s57": [31, 31],
                        "s58": [23, 23],
                        "s59": [59, 59],
                        "s60": [58, 58],
                        "s61": [0, 0],
                        "s62": [0, 0],
                        "s63": [0, 0],
                        "s64": [1, 1],
                        "s65": [1, 1],
                        "s66": [1, 1],
                        "s67": [0, 0],
                        "s68": [0, 0],
                        "s69": [0, 0],
                        "s70": [1, 1],
                        "s71": [1, 1],
                        "s72": [1, 1],
                        "s73": [0, 0],
                        "s74": [0, 0],
                        "s75": [1, 1],
                        "s76": [1, 1],
                        "s77": [0, 0],
                        "s78": [1, 1],
                        "s79": [0, 0],
                        "s80": [1, 1],
                        "s81": [1, 1],
                        "s82": [0, 0],
                        "s83": [1, 1],
                        "s84": [0, 0],
                    }
                ),
                "simplification_4",
                kwargs={"pd": pd, "datetime": datetime},
            ),
            id="simplification_4",
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


def test_pipeline_until_relational_defog_custom(
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


def test_pipeline_until_sql_defog_custom(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
    defog_graphs: graph_fetcher,
    empty_context_database: DatabaseContext,
    defog_config: PyDoughConfigs,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests that the PyDough queries from `defog_custom_pipeline_test_data`
    generate correct SQL text.
    """
    file_path: str = get_sql_test_filename(
        defog_custom_pipeline_test_data.test_name, empty_context_database.dialect
    )
    defog_custom_pipeline_test_data.run_sql_test(
        defog_graphs,
        file_path,
        update_tests,
        empty_context_database,
        config=defog_config,
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
        pytest.param(
            bad_get_part_1,
            "Broker",
            re.escape(
                "Invalid operator invocation 'GETPART(-1)': Expected 3 arguments, received 1"
            ),
            id="bad_get_part_1",
        ),
        pytest.param(
            bad_get_part_2,
            "Broker",
            re.escape(
                "Invalid operator invocation \"GETPART(name, ' ')\": Expected 3 arguments, received 2"
            ),
            id="bad_get_part_2",
        ),
        pytest.param(
            bad_get_part_3,
            "Broker",
            re.escape(
                "Invalid operator invocation 'GETPART(name, -1)': Expected 3 arguments, received 2"
            ),
            id="bad_get_part_3",
        ),
    ],
)
def test_defog_e2e_errors(
    pydough_impl: Callable[..., UnqualifiedNode],
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
