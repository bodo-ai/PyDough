"""
Integration tests for the PyDough workflow on the TPC-H queries using Snowflake.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

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

from tests.test_pydough_functions.simple_pydough_functions import week_offset7

from tests.testing_utilities import (
    graph_fetcher,
    harmonize_types,
    PyDoughSQLComparisonTest,
)
from .test_pipeline_defog_custom import defog_custom_pipeline_test_data
from .test_pipeline_defog import defog_pipeline_test_data

from .testing_utilities import PyDoughPandasTest
from pydough import init_pydough_context, to_df, to_sql


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                week_offset7,
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
                    }
                ),
                "week_offset7",
            ),
            id="sf_week_offset_7",
        ),
    ],
)
def snowflake_params_week_offset_7_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H query 16. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


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


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_sf_e2e_defog_custom(
    defog_custom_pipeline_test_data: PyDoughPandasTest,
    get_sf_defog_graphs: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
):
    """
    Test executing the defog analytical queries with Snowflake database.
    """
    defog_custom_pipeline_test_data.run_e2e_test(
        get_sf_defog_graphs,
        sf_conn_db_context("DEFOG", defog_custom_pipeline_test_data.graph_name),
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


@pytest.mark.snowflake
@pytest.mark.execute
def test_pipeline_e2e_week_offset_7_sf(
    snowflake_params_week_offset_7_data: PyDoughPandasTest,
    get_sf_defog_graphs: graph_fetcher,
    sf_conn_db_context: DatabaseContext,
):
    """ """
    snowflake_params_week_offset_7_data.run_e2e_test(
        get_sf_defog_graphs,
        sf_conn_db_context("DEFOG", snowflake_params_week_offset_7_data.graph_name),
        coerce_types=True,
    )
