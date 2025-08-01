"""
Integration tests for the PyDough workflow on the TPC-H queries.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.test_pydough_functions.tpch_outputs import (
    tpch_q1_output,
    tpch_q2_output,
    tpch_q3_output,
    tpch_q4_output,
    tpch_q5_output,
    tpch_q6_output,
    tpch_q7_output,
    tpch_q8_output,
    tpch_q9_output,
    tpch_q10_output,
    tpch_q11_output,
    tpch_q12_output,
    tpch_q13_output,
    tpch_q14_output,
    tpch_q15_output,
    tpch_q16_output,
    tpch_q17_output,
    tpch_q18_output,
    tpch_q19_output,
    tpch_q20_output,
    tpch_q21_output,
    tpch_q22_output,
)
from tests.test_pydough_functions.tpch_test_functions import (
    impl_tpch_q1,
    impl_tpch_q2,
    impl_tpch_q3,
    impl_tpch_q4,
    impl_tpch_q5,
    impl_tpch_q6,
    impl_tpch_q7,
    impl_tpch_q8,
    impl_tpch_q9,
    impl_tpch_q10,
    impl_tpch_q11,
    impl_tpch_q12,
    impl_tpch_q13,
    impl_tpch_q14,
    impl_tpch_q15,
    impl_tpch_q16,
    impl_tpch_q17,
    impl_tpch_q18,
    impl_tpch_q19,
    impl_tpch_q20,
    impl_tpch_q21,
    impl_tpch_q22,
)
from tests.testing_utilities import (
    graph_fetcher,
)

from .testing_utilities import PyDoughPandasTest


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q1,
                "TPCH",
                tpch_q1_output,
                "tpch_q1",
            ),
            id="tpch_q1",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q2,
                "TPCH",
                tpch_q2_output,
                "tpch_q2",
            ),
            id="tpch_q2",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q3,
                "TPCH",
                tpch_q3_output,
                "tpch_q3",
            ),
            id="tpch_q3",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q4,
                "TPCH",
                tpch_q4_output,
                "tpch_q4",
            ),
            id="tpch_q4",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q5,
                "TPCH",
                tpch_q5_output,
                "tpch_q5",
            ),
            id="tpch_q5",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q6,
                "TPCH",
                tpch_q6_output,
                "tpch_q6",
            ),
            id="tpch_q6",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q7,
                "TPCH",
                tpch_q7_output,
                "tpch_q7",
            ),
            id="tpch_q7",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q8,
                "TPCH",
                tpch_q8_output,
                "tpch_q8",
            ),
            id="tpch_q8",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q9,
                "TPCH",
                tpch_q9_output,
                "tpch_q9",
            ),
            id="tpch_q9",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q10,
                "TPCH",
                tpch_q10_output,
                "tpch_q10",
            ),
            id="tpch_q10",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q11,
                "TPCH",
                tpch_q11_output,
                "tpch_q11",
            ),
            id="tpch_q11",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q12,
                "TPCH",
                tpch_q12_output,
                "tpch_q12",
            ),
            id="tpch_q12",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q13,
                "TPCH",
                tpch_q13_output,
                "tpch_q13",
            ),
            id="tpch_q13",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q14,
                "TPCH",
                tpch_q14_output,
                "tpch_q14",
            ),
            id="tpch_q14",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q15,
                "TPCH",
                tpch_q15_output,
                "tpch_q15",
            ),
            id="tpch_q15",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q16,
                "TPCH",
                tpch_q16_output,
                "tpch_q16",
            ),
            id="tpch_q16",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q17,
                "TPCH",
                tpch_q17_output,
                "tpch_q17",
            ),
            id="tpch_q17",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q18,
                "TPCH",
                tpch_q18_output,
                "tpch_q18",
            ),
            id="tpch_q18",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q19,
                "TPCH",
                tpch_q19_output,
                "tpch_q19",
            ),
            id="tpch_q19",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q20,
                "TPCH",
                tpch_q20_output,
                "tpch_q20",
            ),
            id="tpch_q20",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q21,
                "TPCH",
                tpch_q21_output,
                "tpch_q21",
            ),
            id="tpch_q21",
        ),
        pytest.param(
            PyDoughPandasTest(
                impl_tpch_q22,
                "TPCH",
                tpch_q22_output,
                "tpch_q22",
            ),
            id="tpch_q22",
        ),
        pytest.param(
            # Smoke test covering slicing, INTEGER, GETPART, SMALLEST, FIND,
            # JOIN_STRINGS, LPAD, RPAD, STRING, REPLACE, LOWER, UPPER,
            # LARGEST, STRIP, SQRT and ROUND.
            PyDoughPandasTest(
                "result = parts.CALCULATE("
                " key,"
                " a=INTEGER(JOIN_STRINGS('', brand[-2:], brand[7:], brand[-2:-1])),"
                " b=UPPER(SMALLEST(GETPART(name, ' ', 2), GETPART(name, ' ', -1))),"
                " c=STRIP(name[:2], 'o'),"
                " d=LPAD(STRING(size), 3, '0'),"
                " e=RPAD(STRING(size), 3, '0'),"
                " f=REPLACE(manufacturer, 'Manufacturer#', 'm'),"
                " g=REPLACE(LOWER(container), ' '),"
                " h=STRCOUNT(name, 'o') + (FIND(name, 'o') / 100.0),"
                " i=ROUND(SQRT(LARGEST(size, 10)), 3),"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": range(1, 6),
                        "a": [1331, 1331, 4224, 3443, 3223],
                        "b": ["LACE", "SADDLE", "CORNSILK", "CHOCOLATE", "BROWN"],
                        "c": ["g", "bl", "sp", "c", "f"],
                        "d": ["007", "001", "021", "014", "015"],
                        "e": ["700", "100", "210", "140", "150"],
                        "f": ["m1", "m1", "m4", "m3", "m3"],
                        "g": ["jumbopkg", "lgcase", "wrapcase", "meddrum", "smpkg"],
                        "h": [4.01, 1.23, 2.17, 5.01, 3.01],
                        "i": [3.162, 3.162, 4.583, 3.742, 3.873],
                    }
                ),
                "smoke_a",
            ),
            id="smoke_a",
        ),
        pytest.param(
            # Smoke test covering YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE,
            # SECOND, start of year, start of quarter, start of month, start of
            # week, start of day, start of hour, start of minute, ±years,
            # ±quarters, ±months, ±weeks, ±days, ±hours, ±minutes, ±seconds,
            # DAYNAME, DATEDIFF (year, month, day, week, hour, minute, second),
            # DAYOFWEEK, JOIN_STRINGS.
            PyDoughPandasTest(
                "result = orders.WHERE("
                " STARTSWITH(order_priority, '3')"
                " & ENDSWITH(clerk, '5')"
                " & CONTAINS(comment, 'fo')"
                ").CALCULATE("
                " key,"
                " a=JOIN_STRINGS('_', YEAR(order_date), QUARTER(order_date), MONTH(order_date), DAY(order_date)),"
                " b=JOIN_STRINGS(':', DAYNAME(order_date), DAYOFWEEK(order_date)),"
                " c=DATETIME(order_date, 'start of year', '+6 months', '-13 days'),"
                " d=DATETIME(order_date, 'start of quarter', '+1 year', '+25 hours'),"
                " e=DATETIME('2025-01-01 12:35:13', 'start of minute'),"
                " f=DATETIME('2025-01-01 12:35:13', 'start of hour', '+2 quarters', '+3 weeks'),"
                " g=DATETIME('2025-01-01 12:35:13', 'start of day'),"
                " h=JOIN_STRINGS(';', HOUR('2025-01-01 12:35:13'), MINUTE(DATETIME('2025-01-01 12:35:13', '+45 minutes')), SECOND(DATETIME('2025-01-01 12:35:13', '-7 seconds'))),"
                " i=DATEDIFF('years', '1993-05-25 12:45:36', order_date),"
                " j=DATEDIFF('quarters', '1993-05-25 12:45:36', order_date),"
                " k=DATEDIFF('months', '1993-05-25 12:45:36', order_date),"
                " l=DATEDIFF('weeks', '1993-05-25 12:45:36', order_date),"
                " m=DATEDIFF('days', '1993-05-25 12:45:36', order_date),"
                " n=DATEDIFF('hours', '1993-05-25 12:45:36', order_date),"
                " o=DATEDIFF('minutes', '1993-05-25 12:45:36', order_date),"
                " p=DATEDIFF('seconds', '1993-05-25 12:45:36', order_date),"
                " q=DATETIME(order_date, 'start of week'),"
                ").TOP_K(5, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [131, 834, 4677, 7430, 8065],
                        "a": [
                            "1994_2_6_8",
                            "1994_2_5_23",
                            "1998_1_2_21",
                            "1993_2_6_9",
                            "1992_4_10_22",
                        ],
                        "b": [
                            "Wednesday:3",
                            "Monday:1",
                            "Saturday:6",
                            "Wednesday:3",
                            "Thursday:4",
                        ],
                        "c": [
                            "1994-06-18",
                            "1994-06-18",
                            "1998-06-18",
                            "1993-06-18",
                            "1992-06-18",
                        ],
                        "d": [
                            "1995-04-02 01:00:00",
                            "1995-04-02 01:00:00",
                            "1999-01-02 01:00:00",
                            "1994-04-02 01:00:00",
                            "1993-10-02 01:00:00",
                        ],
                        "e": ["2025-01-01 12:35:00"] * 5,
                        "f": ["2025-07-22 12:00:00"] * 5,
                        "g": ["2025-01-01"] * 5,
                        "h": ["12;20;6"] * 5,
                        "i": [1, 1, 5, 0, -1],
                        "j": [4, 4, 19, 0, -2],
                        "k": [13, 12, 57, 1, -7],
                        "l": [54, 52, 247, 2, -31],
                        "m": [379, 363, 1733, 15, -215],
                        "n": [9084, 8700, 41580, 348, -5172],
                        "o": [544995, 521955, 2494755, 20835, -310365],
                        "p": [32699664, 31317264, 149685264, 1250064, -18621936],
                        "q": [
                            "1994-06-05",
                            "1994-05-22",
                            "1998-02-15",
                            "1993-06-06",
                            "1992-10-18",
                        ],
                    }
                ),
                "smoke_b",
            ),
            id="smoke_b",
        ),
        pytest.param(
            # Smoke test covering SUM, COUNT, NDISTINCT, AVG, MIN, MAX,
            # ANYTHING, VAR, STD, ABS, FLOOR, CEIL, KEEP_IF, DEFAULT_TO,
            # PRESENT, ABSENT, ROUND, MEDIAN and QUANTILE.
            PyDoughPandasTest(
                "result = TPCH.CALCULATE("
                " a=COUNT(customers),"
                " b=SUM(FLOOR(customers.account_balance)),"
                " c=SUM(CEIL(customers.account_balance)),"
                " d=NDISTINCT(customers.market_segment),"
                " e=ROUND(AVG(ABS(customers.account_balance)), 4),"
                " f=MIN(customers.account_balance),"
                " g=MAX(customers.account_balance),"
                " h=ANYTHING(customers.name[:1]),"
                " i=COUNT(KEEP_IF(customers.account_balance, customers.account_balance > 0)),"
                " j=CEIL(VAR(KEEP_IF(customers.account_balance, customers.account_balance > 0), type='population')),"
                " k=ROUND(VAR(KEEP_IF(customers.account_balance, customers.account_balance < 0), type='sample'), 4),"
                " l=FLOOR(STD(KEEP_IF(customers.account_balance, customers.account_balance < 0), type='population')),"
                " m=ROUND(STD(KEEP_IF(customers.account_balance, customers.account_balance > 0), type='sample'), 4),"
                " n=ROUND(AVG(DEFAULT_TO(KEEP_IF(customers.account_balance, customers.account_balance > 0), 0)), 2),"
                " o=SUM(PRESENT(KEEP_IF(customers.account_balance, customers.account_balance > 1000))),"
                " p=SUM(ABSENT(KEEP_IF(customers.account_balance, customers.account_balance > 1000))),"
                " q=QUANTILE(customers.account_balance, 0.2),"
                " r=MEDIAN(customers.account_balance),"
                ")",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "a": [150000],
                        "b": [674252474],
                        "c": [674401000],
                        "d": [5],
                        "e": [4586.6834],
                        "f": [-999.99],
                        "g": [9999.99],
                        "h": ["C"],
                        "i": [136308],
                        "j": [8322120],
                        "k": [83910.7228],
                        "l": [289],
                        "m": [2884.819],
                        "n": [4541.1],
                        "o": [122881],
                        "p": [27119],
                        "q": [1215.91],
                        "r": [4477.3],
                    }
                ),
                "smoke_c",
            ),
            id="smoke_c",
        ),
        pytest.param(
            # Smoke test covering RANKING, RELSUM, RELAVG, RELCOUNT, RELSIZE,
            # PERCENTILE, PREV, NEXT.
            PyDoughPandasTest(
                "result = nations.WHERE(region.name == 'ASIA').customers.CALCULATE("
                " key,"
                " a=RANKING(by=(account_balance.ASC(), key.ASC())),"
                " b=RANKING(by=(account_balance.ASC(), key.ASC()), per='nations'),"
                " c=RANKING(by=market_segment.ASC(), allow_ties=True),"
                " d=RANKING(by=market_segment.ASC(), allow_ties=True, dense=True),"
                " e=PERCENTILE(by=(account_balance.ASC(), key.ASC())),"
                " f=PERCENTILE(by=(account_balance.ASC(), key.ASC()), n_buckets=12, per='nations'),"
                " g=PREV(key, by=key.ASC()),"
                " h=PREV(key, n=2, default=-1, by=key.ASC(), per='nations'),"
                " i=NEXT(key, by=key.ASC()),"
                " j=NEXT(key, n=6000, by=key.ASC(), per='nations'),"
                " k=RELSUM(account_balance, per='nations'),"
                " l=RELSUM(account_balance, by=key.ASC(), cumulative=True),"
                " m=ROUND(RELAVG(account_balance), 2),"
                " n=ROUND(RELAVG(account_balance, per='nations', by=key.ASC(), frame=(None, -1)), 2),"
                " o=RELCOUNT(KEEP_IF(account_balance, account_balance > 0)),"
                " p=RELSIZE(),"
                ")"
                ".TOP_K(10, by=key.ASC())",
                "TPCH",
                lambda: pd.DataFrame(
                    {
                        "key": [7, 9, 19, 21, 25, 28, 36, 37, 38, 45],
                        "a": [
                            29000,
                            25596,
                            27275,
                            6657,
                            22305,
                            5509,
                            16404,
                            246,
                            20106,
                            30146,
                        ],
                        "b": [5803, 5081, 5465, 1367, 4416, 1119, 3265, 53, 3975, 6156],
                        "c": [
                            1,
                            12012,
                            18100,
                            24087,
                            12012,
                            12012,
                            5924,
                            12012,
                            18100,
                            1,
                        ],
                        "d": [1, 3, 4, 5, 3, 3, 2, 3, 4, 1],
                        "e": [97, 85, 91, 23, 74, 19, 55, 1, 67, 100],
                        "f": [12, 11, 11, 3, 9, 3, 7, 1, 9, 12],
                        "g": [None, 7, 9, 19, 21, 25, 28, 36, 37, 38],
                        "h": [-1, -1, -1, -1, -1, 9, -1, 21, -1, -1],
                        "i": [9, 19, 21, 25, 28, 36, 37, 38, 45, 51],
                        "j": [
                            149394,
                            149030,
                            149411,
                            149032,
                            None,
                            149036,
                            149751,
                            149062,
                            None,
                            146097,
                        ],
                        "k": [
                            26740212.13,
                            27293627.48,
                            26740212.13,
                            27293627.48,
                            26898468.71,
                            27293627.48,
                            27081997.67,
                            27293627.48,
                            26898468.71,
                            27930482.5,
                        ],
                        "l": [
                            9561.95,
                            17886.02,
                            26800.73,
                            28228.98,
                            35362.68,
                            36369.86,
                            41357.13,
                            40439.38,
                            46784.49,
                            56767.87,
                        ],
                        "m": [4504.02] * 10,
                        "n": [
                            None,
                            None,
                            9561.95,
                            8324.07,
                            None,
                            4876.16,
                            None,
                            3586.5,
                            7133.7,
                            None,
                        ],
                        "o": [27454] * 10,
                        "p": [30183] * 10,
                    }
                ),
                "smoke_d",
            ),
            id="smoke_d",
        ),
    ],
)
def tpch_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests for the 22 TPC-H queries, as well as some additional
    smoke tests to ensure various functions work as-intended. Returns an
    instance of PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on the
    22 TPC-H queries.
    """
    file_path: str = get_plan_test_filename(tpch_pipeline_test_data.test_name)
    tpch_pipeline_test_data.run_relational_test(
        get_sample_graph, file_path, update_tests
    )


def test_pipeline_until_sql_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    empty_context_database: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
) -> None:
    """
    Same as test_pipeline_until_relational_tpch, but for the generated SQL text.
    """
    file_path: str = get_sql_test_filename(
        tpch_pipeline_test_data.test_name, empty_context_database.dialect
    )
    tpch_pipeline_test_data.run_sql_test(
        get_sample_graph, file_path, update_tests, empty_context_database
    )


@pytest.mark.execute
def test_pipeline_e2e_tpch(
    tpch_pipeline_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    tpch_pipeline_test_data.run_e2e_test(
        get_sample_graph,
        sqlite_tpch_db_context,
    )
