"""
Integration tests for the PyDough workflow on the TPC-H queries using MySQL.
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
    get_part_multiple,
    simple_week_sampler_tpch,
    week_offset,
)
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher, harmonize_types


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
def mysql_params_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H query 16. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                "result = customers.CALCULATE("
                "key,"
                "name[:],"
                "phone,"
                "next_digits=phone[3:6],"
                "country_code=phone[:3],"
                "name_without_first_char=name[1:],"
                "last_digit=phone[-1:],"
                "name_without_start_and_end_char=name[1:-1],"
                "phone_without_last_5_chars=phone[:-5],"
                "name_second_to_last_char=name[-2:-1],"
                "cust_number=name[-10:18]"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 6)),
                        "name": [
                            "Customer#000000001",
                            "Customer#000000002",
                            "Customer#000000003",
                            "Customer#000000004",
                            "Customer#000000005",
                        ],
                        "phone": [
                            "25-989-741-2988",
                            "23-768-687-3665",
                            "11-719-748-3364",
                            "14-128-190-5944",
                            "13-750-942-6364",
                        ],
                        "next_digits": [
                            "989",
                            "768",
                            "719",
                            "128",
                            "750",
                        ],
                        "country_code": ["25-", "23-", "11-", "14-", "13-"],
                        "name_without_first_char": [
                            "ustomer#000000001",
                            "ustomer#000000002",
                            "ustomer#000000003",
                            "ustomer#000000004",
                            "ustomer#000000005",
                        ],
                        "last_digit": ["8", "5", "4", "4", "4"],
                        "name_without_start_and_end_char": [
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                            "ustomer#00000000",
                        ],
                        "phone_without_last_5_chars": [
                            "25-989-741",
                            "23-768-687",
                            "11-719-748",
                            "14-128-190",
                            "13-750-942",
                        ],
                        "name_second_to_last_char": ["0", "0", "0", "0", "0"],
                        "cust_number": [
                            "#000000001",
                            "#000000002",
                            "#000000003",
                            "#000000004",
                            "#000000005",
                        ],
                    }
                ),
                "slicing_test",
            ),
            id="slicing_test",
        ),
        pytest.param(
            PyDoughPandasTest(
                """result = customers.CALCULATE(
                    key,
                    p1=GETPART(name, "#", key),
                    p2=GETPART(name, "0", key),
                    p3=GETPART(address, ",", key),
                    p4=GETPART(address, ",", -key),
                    p5=GETPART(phone, "-", key),
                    p6=GETPART(phone, "-", -key),
                    p7=GETPART(comment, " ", key),
                    p8=GETPART(comment, " ", -key),
                    p9=GETPART(address, "!", key),
                    p10=GETPART(market_segment, "O", -key),
                    p11=GETPART(name, "00000000", key),
                    p12=GETPART("^%1$$@@@##2$#&@@@*^%3$#", "@@@", -key),
                    p13=GETPART(name, "", key),
                    p14=GETPART("", " ", key),
                    p15=GETPART(name, "#", 0),
                    p16=GETPART(nation.name, nation.name, key),
                    p17=GETPART(GETPART(phone, "-", key), "7", 2)
                ).TOP_K(4, by=key.ASC())""",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "k": [1, 2, 3, 4],
                        "p1": ["Customer", "000000002", None, None],
                        "p2": ["Customer#", "", "", ""],
                        "p3": ["IVhzIApeRb ot", "NCwDVaWNe6tEgvwfmRchLXak", None, None],
                        "p4": ["E", "XSTf4", None, None],
                        "p5": ["25", "768", "748", "5944"],
                        "p6": ["2988", "687", "719", "14"],
                        "p7": ["to", "accounts.", "eat", "regular"],
                        "p8": ["e", "boldly:", "even", "ideas"],
                        "p9": ["IVhzIApeRb ot,c,E", None, None, None],
                        "p10": ["BUILDING", "M", "AUT", None],
                        "p11": ["Customer#", "2", None, None],
                        "p12": ["*^%3$#", "##2$#&", "^%1$$", None],
                        "p13": ["Customer#000000001", None, None, None],
                        "p14": [None, None, None, None],
                        "p15": ["Customer", "Customer", "Customer", "Customer"],
                        "p16": ["", "", None, None],
                        "p17": [None, "68", "48", None],
                    }
                ),
                "get_part_test",
            ),
            id="get_part_test",
        ),
    ],
)
def tpch_mysql_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


@pytest.fixture
def defog_mysql_test_data(defog_custom_pipeline_test_data) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the TPC-H queries. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
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

    if defog_custom_pipeline_test_data.test_name == "get_part_multiple":
        return PyDoughPandasTest(
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
                    "p16": ["", "", None, None],
                    "p17": ["", "", "", ""],
                    "p18": ["9", "", "", None],
                }
            ),
            "get_part_multiple",
            skip_sql=True,
        )
    return defog_custom_pipeline_test_data


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_e2e_mysql_conn(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    mysql_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the TPC-H queries from the original code generation on MySQL.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        mysql_conn_db_context("tpch"),
        coerce_types=True,
    )


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_e2e_mysql_params(
    mysql_params_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    mysql_params_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with MySQL as the executing database.
    Using the  `user`, `password`, `database`, and `host`
    as keyword arguments to the DatabaseContext.
    """
    mysql_params_data.run_e2e_test(
        get_sample_graph, mysql_params_tpch_db_context, coerce_types=True
    )


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_e2e_mysql_functions(
    tpch_mysql_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    mysql_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the TPC-H queries from the original code generation,
    with MySQL as the executing database.
    Using the  `user`, `password`, `database`, and `host`
    as keyword arguments to the DatabaseContext.
    """
    tpch_mysql_test_data.run_e2e_test(
        get_sample_graph, mysql_conn_db_context("tpch"), coerce_types=True
    )


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_e2e_tpch_simple_week(
    get_sample_graph: graph_fetcher,
    mysql_conn_db_context: Callable[[str], DatabaseContext],
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
        database=mysql_conn_db_context("tpch"),
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


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_mysql_e2e_defog_custom(
    defog_mysql_test_data: PyDoughPandasTest,
    get_mysql_defog_graphs: graph_fetcher,
    mysql_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the defog analytical queries with MySQL database.
    """
    defog_mysql_test_data.run_e2e_test(
        get_mysql_defog_graphs,
        mysql_conn_db_context("broker"),
        coerce_types=True,
        # display_sql=True,
    )
