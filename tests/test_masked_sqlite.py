"""
Integration tests for the PyDough workflow with custom questions on the custom
CRYPTBANK sqlite database.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import PyDoughPandasTest, graph_fetcher


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughPandasTest(
                "result = customers.TOP_K(3, by=key)",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3],
                        "first_name": ["alice", "bob", "carol"],
                        "last_name": ["johnson", "smith", "lee"],
                        "phone": ["555-123-4567", "555-234-5678", "555-345-6789"],
                        "email": [
                            "alice_j@example.org",
                            "bob.smith77@gmail.com",
                            "c.lee@outlook.com",
                        ],
                        "address": [
                            "123 Maple St;Portland;OR;97205",
                            "456 Oak Ave;Seattle;WA;98101",
                            "789 Pine Rd;Las Vegas;NV;89101",
                        ],
                        "birthday": ["1985-04-12", "1990-07-23", "1982-11-05"],
                    }
                ),
                "basic_scan_topk",
                order_sensitive=True,
            ),
            id="basic_scan_topk",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = transactions.CALCULATE(key, time_stamp).TOP_K(5, by=time_stamp.DESC())",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [167, 15, 147, 263, 36],
                        "time_stamp": [
                            "2025-08-04 06:13:08",
                            "2025-08-03 02:23:25",
                            "2025-07-27 17:55:45",
                            "2025-07-13 14:31:46",
                            "2025-07-13 09:11:12",
                        ],
                    }
                ),
                "partially_encrypted_scan_topk",
                order_sensitive=True,
            ),
            id="partially_encrypted_scan_topk",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " branches"
                " .CALCULATE(source_branch_key=key)"
                " .CALCULATE("
                "  branch_key=key,"
                "  n_local_cust=COUNT(same_state_customers),"
                "  n_local_cust_local_acct=COUNT(same_state_customers.accounts_held.WHERE(branch_key == source_branch_key)),"
                "))",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": list(range(1, 9)),
                        "n_local_cust": [6, 6, 4, 4, 3, 4, 3, 3],
                        "n_local_cust_local_acct": [10, 9, 6, 3, 3, 11, 3, 3],
                    }
                ),
                "general_join_01",
            ),
            id="general_join_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = CRYPTBANK.CALCULATE(n=COUNT("
                " accounts"
                " .CALCULATE(source_branch_key=branch_key)"
                " .WHERE(HAS(account_holder.same_state_branches.WHERE(key == source_branch_key))),"
                "))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [48]}),
                "general_join_02",
            ),
            id="general_join_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name == 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "filter_count_01",
            ),
            id="filter_count_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name != 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "filter_count_02",
            ),
            id="filter_count_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [6]}),
                "filter_count_03",
            ),
            id="filter_count_03",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [14]}),
                "filter_count_04",
            ),
            id="filter_count_04",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(STARTSWITH(phone_number, '555-8'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "filter_count_05",
            ),
            id="filter_count_05",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ENDSWITH(email, 'gmail.com'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "filter_count_06",
            ),
            id="filter_count_06",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(YEAR(birthday) == 1978)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "filter_count_07",
            ),
            id="filter_count_07",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday == '1985-04-12')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "filter_count_08",
            ),
            id="filter_count_08",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(MONOTONIC(8000, amount, 9000))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [31]}),
                "filter_count_09",
            ),
            id="filter_count_09",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(MONOTONIC('2021-05-10 12:00:00', time_stamp, '2021-05-20 12:00:00'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "filter_count_10",
            ),
            id="filter_count_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(sender_account.account_holder.first_name == 'alice')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "filter_count_11",
            ),
            id="filter_count_11",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(YEAR(time_stamp) == YEAR(sender_account.creation_timestamp))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [9]}),
                "filter_count_12",
            ),
            id="filter_count_12",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(time_stamp < DATETIME(receiver_account.creation_timestamp, '+2 years'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [64]}),
                "filter_count_13",
            ),
            id="filter_count_13",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ENDSWITH(first_name, 'e') | ENDSWITH(last_name, 'e'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [8]}),
                "filter_count_14",
            ),
            id="filter_count_14",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(HAS(accounts_held.WHERE(account_type == 'retirement')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [11]}),
                "filter_count_15",
            ),
            id="filter_count_15",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(HAS(accounts_held.WHERE((account_type != 'savings') & (account_type != 'checking'))))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [12]}),
                "filter_count_16",
            ),
            id="filter_count_16",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(first_name[1:2] == 'a')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "filter_count_17",
            ),
            id="filter_count_17",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE((YEAR(time_stamp) == 2022) & (MONTH(time_stamp) == 6))\n"
                "result = CRYPTBANK.CALCULATE(n=ROUND(AVG(selected_transactions.amount), 2))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3858.9]}),
                "agg_01",
            ),
            id="agg_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "acc_typs = accounts.PARTITION(name='account_types', by=account_type)\n"
                "result = acc_typs.CALCULATE(account_type, n=COUNT(accounts), avg_bal=ROUND(AVG(accounts.balance), 2))\n",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "account_type": [
                            "business",
                            "checking",
                            "mma",
                            "retirement",
                            "savings",
                        ],
                        "n": [3, 20, 2, 11, 20],
                        "avg_bal": [4600.0, 1283.99, 4700.25, 20181.82, 6715.33],
                    }
                ),
                "agg_02",
            ),
            id="agg_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "acc_typs = accounts.PARTITION(name='account_types', by=account_type)\n"
                "result = acc_typs.accounts.BEST(per='account_types', by=balance.DESC()).CALCULATE(account_type, balance)\n",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "account_type": [
                            "business",
                            "checking",
                            "mma",
                            "retirement",
                            "savings",
                        ],
                        "balance": [4800.0, 3200.0, 5500.0, 25000.0, 10500.0],
                    }
                ),
                "agg_03",
            ),
            id="agg_03",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = branches.CALCULATE("
                " branch_key=key,"
                " pct_total_wealth=ROUND(SUM(accounts_managed.balance) / RELSUM(SUM(accounts_managed.balance)), 2)"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": range(1, 9),
                        "pct_total_wealth": [
                            0.25,
                            0.22,
                            0.12,
                            0.06,
                            0.07,
                            0.23,
                            0.01,
                            0.05,
                        ],
                    }
                ),
                "agg_04",
            ),
            id="agg_04",
        ),
    ],
)
def cryptbank_pipeline_test_data(request) -> PyDoughPandasTest:
    """
    Test data for e2e tests using cryptbank test data. Returns an instance of
    PyDoughPandasTest containing information about the test.
    """
    return request.param


def test_pipeline_until_relational_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(cryptbank_pipeline_test_data.test_name)
    cryptbank_pipeline_test_data.run_relational_test(
        masked_graphs, file_path, update_tests
    )


def test_pipeline_until_sql_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
):
    """
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
    into SQL text.
    """
    file_path: str = get_sql_test_filename(
        cryptbank_pipeline_test_data.test_name, sqlite_tpch_db_context.dialect
    )
    cryptbank_pipeline_test_data.run_sql_test(
        masked_graphs,
        file_path,
        update_tests,
        sqlite_tpch_db_context,
    )


@pytest.mark.execute
def test_pipeline_e2e_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_cryptbank_connection: DatabaseContext,
):
    """
    Test executing the the custom queries with the custom cryptbank dataset
    against the refsol DataFrame.
    """
    cryptbank_pipeline_test_data.run_e2e_test(
        masked_graphs,
        sqlite_cryptbank_connection,
    )
