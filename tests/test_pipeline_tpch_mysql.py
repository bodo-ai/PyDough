"""
Integration tests for the PyDough workflow on the TPC-H queries using MySQL.
"""

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
