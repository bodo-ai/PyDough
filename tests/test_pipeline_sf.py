"""
Integration tests for the PyDough workflow on the TPC-H queries using Snowflake.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

from collections.abc import Callable
import pandas as pd
import pytest
import datetime
from tests.test_pipeline_defog_custom import get_start_of_week, get_day_of_week
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from pydough.configs import DayOfWeek, PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)

from tests.test_pydough_functions.simple_pydough_functions import week_offset

from tests.testing_utilities import (
    graph_fetcher,
    harmonize_types,
    PyDoughSQLComparisonTest,
)
from .test_pipeline_defog_custom import defog_custom_pipeline_test_data
from .test_pipeline_defog import defog_pipeline_test_data

from .testing_utilities import PyDoughPandasTest
from pydough import init_pydough_context, to_df, to_sql

# NOTE: this should move to test_pipeline_tpch_custom.py once the
# other dialects are supported
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


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q16,
                "TPCH",
                tpch_q16_output,
                "tpch_q16_params",
            ),
            id="tpch_q16_params",
        ),
    ],
)
def snowflake_params_tpch_q16_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H query 16. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_e2e_tpch_sf_conn(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sf_sample_graph: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with Snowflake as the executing database.
    Using the `connection` as keyword argument to the DatabaseContext.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sf_sample_graph,
        sf_conn_db_context("SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"),
        coerce_types=True,
    )


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_e2e_tpch_sf_params(
    snowflake_params_tpch_q16_data: PyDoughPandasTest,
    get_sf_sample_graph: graph_fetcher,
    sf_params_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with Snowflake as the executing database.
    Using the  `user`, `password`, `account`, `database`, `schema`, and `warehouse`
    as keyword arguments to the DatabaseContext.
    """
    snowflake_params_tpch_q16_data.run_e2e_test(
        get_sf_sample_graph, sf_params_tpch_db_context, coerce_types=True
    )


def simple_week_sampler():
    x_dt = datetime.datetime(2025, 3, 10, 11, 00, 0)
    y_dt = datetime.datetime(2025, 3, 14, 11, 00, 0)
    y_dt2 = datetime.datetime(2025, 3, 15, 11, 00, 0)
    y_dt3 = datetime.datetime(2025, 3, 16, 11, 00, 0)
    y_dt4 = datetime.datetime(2025, 3, 17, 11, 00, 0)
    y_dt5 = datetime.datetime(2025, 3, 18, 11, 00, 0)
    y_dt6 = datetime.datetime(2025, 3, 19, 11, 00, 0)
    y_dt7 = datetime.datetime(2025, 3, 20, 11, 00, 0)
    y_dt8 = datetime.datetime(2025, 3, 21, 11, 00, 0)
    return TPCH.CALCULATE(
        weeks_diff=DATEDIFF("weeks", x_dt, y_dt),
        sow1=DATETIME(y_dt, "start of week"),
        sow2=DATETIME(y_dt2, "start of week"),
        sow3=DATETIME(y_dt3, "start of week"),
        sow4=DATETIME(y_dt4, "start of week"),
        sow5=DATETIME(y_dt5, "start of week"),
        sow6=DATETIME(y_dt6, "start of week"),
        sow7=DATETIME(y_dt7, "start of week"),
        sow8=DATETIME(y_dt8, "start of week"),
        dayname1=DAYNAME(y_dt),
        dayname2=DAYNAME(y_dt2),
        dayname3=DAYNAME(y_dt3),
        dayname4=DAYNAME(y_dt4),
        dayname5=DAYNAME(y_dt5),
        dayname6=DAYNAME(y_dt6),
        dayname7=DAYNAME(y_dt7),
        dayname8=DAYNAME(y_dt8),
        dayofweek1=DAYOFWEEK(y_dt),
        dayofweek2=DAYOFWEEK(y_dt2),
        dayofweek3=DAYOFWEEK(y_dt3),
        dayofweek4=DAYOFWEEK(y_dt4),
        dayofweek5=DAYOFWEEK(y_dt5),
        dayofweek6=DAYOFWEEK(y_dt6),
        dayofweek7=DAYOFWEEK(y_dt7),
        dayofweek8=DAYOFWEEK(y_dt8),
    )


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_e2e_tpch_simple_week(
    get_sf_sample_graph: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
    week_handling_config: PyDoughConfigs,
):
    """
    Test executing simple_week_sampler using the tpch schemas with different
    week configurations, comparing against expected results.
    """
    graph: GraphMetadata = get_sf_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(simple_week_sampler)()
    result: pd.DataFrame = to_df(
        root,
        metadata=graph,
        database=sf_conn_db_context("SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"),
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
    data_dict = {"WEEKS_DIFF": [weeks_diff]}

    # Add start of week columns
    for i in range(len(dates)):
        data_dict[f"SOW{i + 1}"] = [sows[i]]

    # Add day name columns
    for i in range(len(dates)):
        data_dict[f"DAYNAME{i + 1}"] = [daynames[i]]

    # Add day of week columns
    for i in range(len(dates)):
        data_dict[f"DAYOFWEEK{i + 1}"] = [dayofweeks[i]]

    # Create DataFrame with expected results
    expected_df = pd.DataFrame(data_dict)
    for col_name in result.columns:
        result[col_name], expected_df[col_name] = harmonize_types(
            result[col_name], expected_df[col_name]
        )
    pd.testing.assert_frame_equal(result, expected_df, check_dtype=False)


@pytest.fixture
def defog_sf_test_data(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
) -> PyDoughPandasTest:
    """
    Modify reference solution data for some Defog queries.
    Return an instance of PyDoughPandasTest containing the modified data.
    """
    # Adjust the 3rd-to-last data point because Snowflake and SQLite
    # handle "+1 month" differently:
    # - Snowflake: if the next month overflows, it returns the end of next month
    # - SQLite: adds 30 days, which may move into the following month
    #
    # Example: "2023-01-30 + 1 month"
    #   SQLite: 2023-03-02 vs. Snowflake: 2023-02-28
    if defog_custom_pipeline_test_data.test_name == "week_offset":
        return PyDoughPandasTest(
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
                        "2023-03-14 13:15:00",
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
        )

    return defog_custom_pipeline_test_data


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_sf_e2e_defog_custom(
    defog_sf_test_data: PyDoughPandasTest,
    get_sf_defog_graphs: graph_fetcher,
    defog_config: PyDoughConfigs,
    sf_conn_db_context: DatabaseContext,
):
    """
    Test executing the defog analytical queries with Snowflake database.
    """
    defog_sf_test_data.run_e2e_test(
        get_sf_defog_graphs,
        sf_conn_db_context("DEFOG", defog_sf_test_data.graph_name),
        config=defog_config,
        coerce_types=True,
    )


@pytest.mark.snowflake
@pytest.mark.execute
def test_defog_e2e(
    defog_pipeline_test_data: PyDoughSQLComparisonTest,
    get_sf_defog_graphs: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
    defog_config: PyDoughConfigs,
    sqlite_defog_connection: DatabaseContext,
) -> None:
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on the defog.ai queries.
    NOTE: passing SQLite connection as reference database so that refsol
    is executed using SQLite.
    This is needed because refsol uses SQLite SQL syntax to obtain
    the correct results.
    """
    defog_pipeline_test_data.run_e2e_test(
        get_sf_defog_graphs,
        sf_conn_db_context("DEFOG", defog_pipeline_test_data.graph_name),
        defog_config,
        reference_database=sqlite_defog_connection,
        coerce_types=True,
    )


# NOTE: this should move and be part of tpch_custom_pipeline_test_data once the
# other dialects are supported
@pytest.fixture(
    params=[
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
def sf_user_generated_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for user generated collections on Snowflake. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.mark.snowflake
@pytest.mark.execute
def test_e2e_sf_user_generated_data(
    sf_user_generated_data: PyDoughPandasTest,
    get_sf_sample_graph: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with Snowflake as the executing database.
    Using the `connection` as keyword argument to the DatabaseContext.
    """
    sf_user_generated_data.run_e2e_test(
        get_sf_sample_graph,
        sf_conn_db_context("SNOWFLAKE_SAMPLE_DATA", "TPCH_SF1"),
        coerce_types=True,
    )


# TODO: delete this test once the other dialects are supported
# and moved to tpch_custom_pipeline_test_data
# It's needed here to access sf_user_generated_data fixture
# that has user-generated test cases.
def test_pipeline_until_relational_tpch_custom_sf(
    sf_user_generated_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on
    custom queries with the TPC-H graph.
    """
    file_path: str = get_plan_test_filename(sf_user_generated_data.test_name)
    sf_user_generated_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )
