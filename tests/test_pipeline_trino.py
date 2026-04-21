"""
Integration tests for the PyDough workflow on various queries using Trino.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pytest
from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q16_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q16,
)
import pandas as pd

from tests.test_pydough_functions.simple_pydough_functions import week_offset

from tests.testing_utilities import (
    graph_fetcher,
    PyDoughSQLComparisonTest,
)
from tests.test_pydough_functions.simple_pydough_functions import (
    week_offset,
)

from .conftest import tpch_custom_test_data_dialect_replacements

from .test_pipeline_defog_custom import defog_custom_pipeline_test_data
from .test_pipeline_defog import defog_pipeline_test_data
from .test_pipeline_custom_datasets import custom_datasets_test_data  # noqa
from .test_pipeline_tpch_custom import tpch_custom_pipeline_test_data  # noqa

from .testing_utilities import PyDoughPandasTest


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_tpch_trino_params(
    get_trino_graphs: graph_fetcher,
    trino_params_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation,
    with Trino as the executing database. Using `host`, `port`,
    `user`, `catalog`, `schema`, and `warehouse` as keyword arguments to the
    DatabaseContext. Only tests using TPC-H query 16, since the rest of the
    tests are already covered with the trino connection test.
    """
    test_data: PyDoughPandasTest = PyDoughPandasTest(
        impl_tpch_q16,
        "TPCH",
        tpch_q16_output,
        "tpch_q16_params",
    )
    test_data.run_e2e_test(
        get_trino_graphs,
        trino_params_tpch_db_context,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_trino_tpch_custom(
    tpch_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
    get_trino_graphs: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H custom queries from the original code generation on
    Trino.
    """
    tpch_custom_pipeline_test_data = tpch_custom_test_data_dialect_replacements(
        trino_conn_db_context.dialect,
        tpch_custom_pipeline_test_data,
    )

    tpch_custom_pipeline_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.mark.trino
@pytest.mark.execute
def test_defog_e2e(
    defog_pipeline_test_data: PyDoughSQLComparisonTest,
    get_trino_graphs: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
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
        get_trino_graphs,
        trino_conn_db_context,
        defog_config,
        reference_database=sqlite_defog_connection,
        coerce_types=True,
        atol=5e-3,
    )


@pytest.fixture
def defog_trino_test_data(
    defog_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
) -> PyDoughPandasTest:
    """
    Modify reference solution data for some Defog queries.
    Return an instance of PyDoughPandasTest containing the modified data.
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
    return defog_custom_pipeline_test_data


@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_trino_e2e_defog_custom(
    defog_trino_test_data: PyDoughPandasTest,
    get_trino_graphs: graph_fetcher,
    defog_config: PyDoughConfigs,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the defog analytical queries with Trino database.
    """
    defog_trino_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        config=defog_config,
        coerce_types=True,
        atol=5e-3,
        display_sql=True,
    )


@pytest.mark.skip(
    "Trino does not properly handle all of the weird quoting cases; investigate at a later time."
)
@pytest.mark.custom_datasets
@pytest.mark.trino
@pytest.mark.execute
def test_pipeline_e2e_trino_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,  # noqa: F811
    get_trino_graphs: graph_fetcher,
    trino_conn_db_context: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_trino_graphs,
        trino_conn_db_context,
        coerce_types=True,
        atol=5e-3,
    )
