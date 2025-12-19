"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import (
    graph_fetcher,
)

from .testing_utilities import PyDoughPandasTest


@pytest.mark.parametrize(
    "execute",
    [
        pytest.param(False, id="sql"),
        pytest.param(True, id="e2e", marks=pytest.mark.execute),
    ],
)
@pytest.mark.parametrize(
    "test_data, max_rows",
    [
        pytest.param(
            PyDoughPandasTest(
                "result = regions",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [0],
                        "name": ["AFRICA"],
                        "comment": [
                            "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to "
                        ],
                    }
                ),
                "regions_max1",
            ),
            1,
            id="regions_max1",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = regions",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [0, 1, 2, 3, 4],
                        "name": ["AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"],
                        "comment": [
                            "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to ",
                            "hs use ironic, even requests. s",
                            "ges. thinly even pinto beans ca",
                            "ly final courts cajole furiously final excuse",
                            "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl",
                        ],
                    }
                ),
                "regions_max100",
            ),
            100,
            id="regions_max100",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = nations.CALCULATE(key, name).TOP_K(3, by=name.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [0, 1],
                        "name": ["ALGERIA", "ARGENTINA"],
                    }
                ),
                "nations_top3_max2",
            ),
            2,
            id="nations_top3_max2",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = nations.CALCULATE(key, name).TOP_K(3, by=name.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [0, 1, 2],
                        "name": ["ALGERIA", "ARGENTINA", "BRAZIL"],
                    }
                ),
                "nations_top3_max6",
            ),
            6,
            id="nations_top3_max6",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = customers.CALCULATE(ck=key).TOP_K(10, by=account_balance.DESC()).orders.CALCULATE(ck, ok=key, tp=total_price).ORDER_BY(tp.DESC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "ck": [61453, 23828, 61453, 144232, 129934],
                        "ok": [4056323, 5349568, 5503299, 5343141, 1808867],
                        "tp": [424918.3, 322586.39, 308661.75, 307284.63, 306708.79],
                    }
                ),
                "richest_customers_orders_max5",
                order_sensitive=True,
            ),
            5,
            id="richest_customers_orders_max5",
        ),
    ],
)
def test_max_rows(
    test_data: PyDoughPandasTest,
    max_rows: int,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    execute: bool,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Test either SQL or runtime execution of custom queries on the TPC-H dataset
    with various max_rows settings.
    """
    if execute:
        test_data.run_e2e_test(
            get_sample_graph,
            sqlite_tpch_db_context,
            max_rows=max_rows,
        )
    else:
        file_path: str = get_sql_test_filename(
            test_data.test_name, sqlite_tpch_db_context.dialect
        )
        test_data.run_sql_test(
            get_sample_graph,
            file_path,
            update_tests,
            sqlite_tpch_db_context,
            max_rows=max_rows,
        )
