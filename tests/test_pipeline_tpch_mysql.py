"""
Integration tests for the PyDough workflow on the TPC-H queries using MySQL.
"""

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)
from tests.testing_utilities import (
    graph_fetcher,
)

from .testing_utilities import PyDoughPandasTest


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


@pytest.mark.mysql
@pytest.mark.execute
def test_pipeline_e2e_mysql_conn(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    mysql_conn_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation on MySQL.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        mysql_conn_tpch_db_context,
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
    mysql_conn_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with MySQL as the executing database.
    Using the  `user`, `password`, `database`, and `host`
    as keyword arguments to the DatabaseContext.
    """
    tpch_mysql_test_data.run_e2e_test(
        get_sample_graph, mysql_conn_tpch_db_context, coerce_types=True
    )
