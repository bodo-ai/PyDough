"""
Integration tests for the PyDough workflow on the TPC-H queries using Oracle.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
)
from tests.test_pipeline_defog_custom import get_day_of_week, get_start_of_week
from tests.test_pydough_functions.simple_pydough_functions import (
    simple_week_sampler_tpch,
)
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)
from tests.testing_utilities import (
    PyDoughPandasTest,
    PyDoughSQLComparisonTest,
    graph_fetcher,
    harmonize_types,
)

from .conftest import tpch_custom_test_data_dialect_replacements
from .test_pipeline_custom_datasets import custom_datasets_test_data  # noqa
from .test_pipeline_defog import defog_pipeline_test_data  # noqa
from .test_pipeline_defog_custom import defog_custom_pipeline_test_data  # noqa
from .test_pipeline_tpch_custom import tpch_custom_pipeline_test_data  # noqa


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
def tpch_oracle_params_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H query 16. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],  # Fix this
):
    """
    Test executing the TPC-H queries from the original code generation on Oracle.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        oracle_conn_db_context("tpch"),
        coerce_types=True,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_tpch_16_params(
    tpch_oracle_params_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    oracle_params_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H 16 from the original code generation,
    with oracle as the executing database.
    Using the  `user`, `password`, `database`, and `host`
    as keyword arguments to the DatabaseContext.
    """
    tpch_oracle_params_test_data.run_e2e_test(
        get_sample_graph, oracle_params_tpch_db_context, coerce_types=True
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_custom_functions(
    custom_functions_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the custom functions test data using TPCH with Oracle
    """
    custom_functions_test_data.run_e2e_test(
        get_sample_graph, oracle_conn_db_context("tpch"), coerce_types=True
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_tpch_simple_week(
    get_sample_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
    week_handling_config: PyDoughConfigs,
):
    """
    Test executing simple_week_sampler using the TPCH schemas with different
    week configurations, comparing against expected results.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(simple_week_sampler_tpch)()
    result: pd.DataFrame = to_df(
        root,
        metadata=graph,
        database=oracle_conn_db_context("tpch"),
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

    for col_name in result.columns:
        result[col_name], expected_df[col_name] = harmonize_types(
            result[col_name], expected_df[col_name]
        )
    pd.testing.assert_frame_equal(
        result, expected_df, check_dtype=False, check_exact=False, atol=1e-8
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
    get_sample_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the TPC-H custom queries from the original code generation on
    Oracle.
    """
    tpch_custom_pipeline_test_data = tpch_custom_test_data_dialect_replacements(
        oracle_conn_db_context("tpch").dialect, tpch_custom_pipeline_test_data
    )

    tpch_custom_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        oracle_conn_db_context("tpch"),
        coerce_types=True,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_defog_custom(
    defog_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
    get_oracle_defog_graphs: graph_fetcher,
    defog_config: PyDoughConfigs,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the defog analytical queries with Oracle database.
    """
    defog_custom_pipeline_test_data.run_e2e_test(
        get_oracle_defog_graphs,
        oracle_conn_db_context(defog_custom_pipeline_test_data.graph_name.lower()),
        config=defog_config,
        coerce_types=True,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_defog(
    defog_pipeline_test_data: PyDoughSQLComparisonTest,  # noqa: F811
    get_oracle_defog_graphs: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
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
        get_oracle_defog_graphs,
        oracle_conn_db_context(defog_pipeline_test_data.graph_name.lower()),
        defog_config,
        reference_database=sqlite_defog_connection,
        coerce_types=True,
        rtol=1e4,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,  # noqa: F811
    get_oracle_custom_datasets_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_oracle_custom_datasets_graph,
        oracle_conn_db_context(custom_datasets_test_data.graph_name.lower()),
        coerce_types=True,
    )
