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
                "name,"
                "phone,"
                "country_code=phone[:3],"
                "name_without_first_char=name[1:],"
                "last_digit=phone[-1:],"
                "name_without_start_and_end_char=name[1:-1],"
                "phone_without_last_5_chars=phone[:-5],"
                "name_second_to_last_char=name[-2:-1],"
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
                        "country_code": ["25-", "23-", "11-", "14-", "13-"],
                        "name_without_first_char": [
                            "ustomer#000000001",
                            "ustomer#000000002",
                            "ustomer#000000003",
                            "ustomer#000000004",
                            "ustomer#000000005",
                        ],
                        "last_digit": ["", "", "", "", ""],
                        "name_without_start_and_end_char": ["", "", "", "", ""],
                        "phone_without_last_5_chars": ["", "", "", "", ""],
                        "name_second_to_last_char": ["1", "2", "3", "4", "5"],
                    }
                ),
                "slicing_test",
            ),
            id="slicing_test",
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
