"""
Integration tests for the PyDough workflow with custom questions on the custom
CRYPTBANK sqlite database.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.mask_server import MaskServerInfo
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
                "cryptbank_basic_scan_topk",
                order_sensitive=True,
            ),
            id="cryptbank_basic_scan_topk",
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
                "cryptbank_partially_encrypted_scan_topk",
                order_sensitive=True,
            ),
            id="cryptbank_partially_encrypted_scan_topk",
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
                        "n_local_cust": [5, 5, 4, 4, 3, 4, 2, 2],
                        "n_local_cust_local_acct": [9, 7, 6, 3, 3, 11, 2, 2],
                    }
                ),
                "cryptbank_general_join_01",
            ),
            id="cryptbank_general_join_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = CRYPTBANK.CALCULATE(n=COUNT("
                " accounts"
                " .CALCULATE(source_branch_key=branch_key)"
                " .WHERE(HAS(account_holder.same_state_branches.WHERE(key == source_branch_key))),"
                "))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [43]}),
                "cryptbank_general_join_02",
            ),
            id="cryptbank_general_join_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name == 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "cryptbank_filter_count_01",
            ),
            id="cryptbank_filter_count_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(last_name != 'lee')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "cryptbank_filter_count_02",
            ),
            id="cryptbank_filter_count_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [6]}),
                "cryptbank_filter_count_03",
            ),
            id="cryptbank_filter_count_03",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [14]}),
                "cryptbank_filter_count_04",
            ),
            id="cryptbank_filter_count_04",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(STARTSWITH(phone_number, '555-8'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "cryptbank_filter_count_05",
            ),
            id="cryptbank_filter_count_05",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ENDSWITH(email, 'gmail.com'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_06",
            ),
            id="cryptbank_filter_count_06",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(YEAR(birthday) == 1978)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "cryptbank_filter_count_07",
            ),
            id="cryptbank_filter_count_07",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday == '1985-04-12')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "cryptbank_filter_count_08",
            ),
            id="cryptbank_filter_count_08",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(MONOTONIC(8000, amount, 9000))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [31]}),
                "cryptbank_filter_count_09",
            ),
            id="cryptbank_filter_count_09",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(MONOTONIC('2021-05-10 12:00:00', time_stamp, '2021-05-20 12:00:00'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "cryptbank_filter_count_10",
            ),
            id="cryptbank_filter_count_10",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(sender_account.account_holder.first_name == 'alice')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "cryptbank_filter_count_11",
            ),
            id="cryptbank_filter_count_11",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(YEAR(time_stamp) == YEAR(sender_account.creation_timestamp))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [9]}),
                "cryptbank_filter_count_12",
            ),
            id="cryptbank_filter_count_12",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(time_stamp < DATETIME(receiver_account.creation_timestamp, '+2 years'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [64]}),
                "cryptbank_filter_count_13",
            ),
            id="cryptbank_filter_count_13",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ENDSWITH(first_name, 'e') | ENDSWITH(last_name, 'e'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [8]}),
                "cryptbank_filter_count_14",
            ),
            id="cryptbank_filter_count_14",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(HAS(accounts_held.WHERE(account_type == 'retirement')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [11]}),
                "cryptbank_filter_count_15",
            ),
            id="cryptbank_filter_count_15",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(HAS(accounts_held.WHERE((account_type != 'savings') & (account_type != 'checking'))))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [12]}),
                "cryptbank_filter_count_16",
            ),
            id="cryptbank_filter_count_16",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(first_name[1:2] == 'a')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_17",
            ),
            id="cryptbank_filter_count_17",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(LIKE(email, '%.%@%mail%'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [8]}),
                "cryptbank_filter_count_18",
            ),
            id="cryptbank_filter_count_18",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(email, 'mail'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [12]}),
                "cryptbank_filter_count_19",
            ),
            id="cryptbank_filter_count_19",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday > '1991-11-15')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_20",
            ),
            id="cryptbank_filter_count_20",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday >= '1991-11-15')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_21",
            ),
            id="cryptbank_filter_count_21",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday < '1991-11-15')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [13]}),
                "cryptbank_filter_count_22",
            ),
            id="cryptbank_filter_count_22",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday <= '1991-11-15')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [14]}),
                "cryptbank_filter_count_23",
            ),
            id="cryptbank_filter_count_23",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(birthday == '1991-11-15')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "cryptbank_filter_count_24",
            ),
            id="cryptbank_filter_count_24",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ABSENT(birthday) | (birthday != '1991-11-15'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [19]}),
                "cryptbank_filter_count_25",
            ),
            id="cryptbank_filter_count_25",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(phone_number == '555-123-456')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [0]}),
                "cryptbank_filter_count_26",
            ),
            id="cryptbank_filter_count_26",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE("
                " ("
                "  PRESENT(address) &"
                "  PRESENT(birthday) &"
                "  (last_name != 'lopez') &"
                "  (ENDSWITH(first_name, 'a') | ENDSWITH(first_name, 'e') | ENDSWITH(first_name, 's'))"
                ") | (ABSENT(birthday) & ENDSWITH(phone_number, '5'))"
                ")\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [6]}),
                "cryptbank_filter_count_27",
            ),
            id="cryptbank_filter_count_27",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE("
                + " & ".join(
                    [
                        "((account_type == 'retirement') | (account_type == 'savings'))",
                        "(balance >= 5000)",
                        "(CONTAINS(account_holder.email, 'outlook') | CONTAINS(account_holder.email, 'gmail'))",
                        "(YEAR(creation_timestamp) < 2020)",
                    ]
                )
                + ")\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [12]}),
                "cryptbank_filter_count_28",
            ),
            id="cryptbank_filter_count_28",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE((YEAR(time_stamp) == 2022) & (MONTH(time_stamp) == 6))\n"
                "result = CRYPTBANK.CALCULATE(n=ROUND(AVG(selected_transactions.amount), 2))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3858.9]}),
                "cryptbank_agg_01",
            ),
            id="cryptbank_agg_01",
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
                "cryptbank_agg_02",
            ),
            id="cryptbank_agg_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "acc_typs = accounts.CALCULATE(name=JOIN_STRINGS(' ', account_holder.first_name, account_holder.last_name)).PARTITION(name='account_types', by=account_type)\n"
                "result = acc_typs.accounts.BEST(per='account_types', by=balance.DESC()).CALCULATE(account_type, balance, name)\n",
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
                        "name": [
                            "grace davis",
                            "peter thomas",
                            "carol lee",
                            "james martinez",
                            "robert moore",
                        ],
                    }
                ),
                "cryptbank_agg_03",
            ),
            id="cryptbank_agg_03",
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
                "cryptbank_agg_04",
            ),
            id="cryptbank_agg_04",
        ),
        pytest.param(
            PyDoughPandasTest(
                "account_info = accounts.CALCULATE("
                " first_transaction_sent=MIN(transactions_sent.time_stamp),"
                ")\n"
                "result = CRYPTBANK.CALCULATE(avg_secs=ROUND(AVG(DATEDIFF('seconds', account_info.creation_timestamp, account_info.first_transaction_sent)), 2))",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "avg_secs": [67095338.32],
                    }
                ),
                "cryptbank_agg_05",
            ),
            id="cryptbank_agg_05",
        ),
        pytest.param(
            PyDoughPandasTest(
                "first_sent = accounts_held.transactions_sent.WHERE(receiver_account.branch.address[-5:] == '94105').BEST(per='accounts_held', by=time_stamp.ASC())\n"
                "result = ("
                " customers"
                " .CALCULATE("
                "  key,"
                "  name=JOIN_STRINGS(' ', first_name, last_name),"
                "  first_sends=SUM(first_sent.amount),"
                " )"
                " .TOP_K(3, by=(first_sends.DESC(), key.ASC()))"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [7, 3, 10],
                        "name": ["grace davis", "carol lee", "james martinez"],
                        "first_sends": [16033.18, 15871.02, 11554.16],
                    }
                ),
                "cryptbank_analysis_01",
                order_sensitive=True,
            ),
            id="cryptbank_analysis_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "first_received = accounts_held.transactions_received.WHERE(sender_account.branch.address[-5:] == '94105').BEST(per='accounts_held', by=time_stamp.ASC())\n"
                "result = ("
                " customers"
                " .CALCULATE("
                "  key,"
                "  name=JOIN_STRINGS(' ', first_name, last_name),"
                "  first_recvs=SUM(first_received.amount),"
                " )"
                " .TOP_K(3, by=(first_recvs.DESC(), key.ASC()))"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [3, 9, 2],
                        "name": ["carol lee", "isabel rodriguez", "bob smith"],
                        "first_recvs": [22802.6, 15400.64, 14990.64],
                    }
                ),
                "cryptbank_analysis_02",
                order_sensitive=True,
            ),
            id="cryptbank_analysis_02",
        ),
        pytest.param(
            PyDoughPandasTest(
                "first_sent = accounts_held.transactions_sent.WHERE(receiver_account.branch.address[-5:] == '94105').BEST(per='accounts_held', by=time_stamp.ASC())\n"
                "first_received = accounts_held.transactions_received.WHERE(sender_account.branch.address[-5:] == '94105').BEST(per='accounts_held', by=time_stamp.ASC())\n"
                "result = ("
                " customers"
                " .CALCULATE("
                "  key,"
                "  name=JOIN_STRINGS(' ', first_name, last_name),"
                "  first_sends=SUM(first_sent.amount),"
                "  first_recvs=SUM(first_received.amount),"
                " )"
                " .TOP_K(3, by=((first_sends+first_recvs).DESC(), key.ASC()))"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [3, 7, 9],
                        "name": ["carol lee", "grace davis", "isabel rodriguez"],
                        "first_sends": [15871.02, 16033.18, 8675.95],
                        "first_recvs": [22802.6, 8976.13, 15400.64],
                    }
                ),
                "cryptbank_analysis_03",
                order_sensitive=True,
            ),
            id="cryptbank_analysis_03",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customer = account_holder.WHERE(MONOTONIC(1980, YEAR(birthday), 1985))\n"
                "selected_transactions = transactions_sent.WHERE(amount > 9000.0)\n"
                "result = ("
                " accounts"
                " .WHERE(HAS(selected_customer) & HAS(selected_transactions))"
                " .CALCULATE("
                "  key,"
                "  cust_name=JOIN_STRINGS(' ', selected_customer.first_name, selected_customer.last_name),"
                "  n_trans=COUNT(selected_transactions),"
                " )"
                " .ORDER_BY(key.ASC())"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "key": [3, 6, 31, 33, 40, 55, 56],
                        "cust_name": [
                            "alice johnson",
                            "carol lee",
                            "luke lopez",
                            "luke lopez",
                            "olivia anderson",
                            "thomas lee",
                            "thomas lee",
                        ],
                        "n_trans": [1, 1, 2, 1, 2, 1, 1],
                    }
                ),
                "cryptbank_analysis_04",
                order_sensitive=True,
            ),
            id="cryptbank_analysis_04",
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
    enable_mask_rewrites: str,
    mock_server_info: MaskServerInfo,
) -> None:
    """
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(
        f"{cryptbank_pipeline_test_data.test_name}_{enable_mask_rewrites}"
    )
    cryptbank_pipeline_test_data.run_relational_test(
        masked_graphs, file_path, update_tests, mask_server=mock_server_info
    )


def test_pipeline_until_sql_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_tpch_db_context: DatabaseContext,
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
    enable_mask_rewrites: str,
    mock_server_info: MaskServerInfo,
):
    """
    Tests the conversion of the PyDough queries on the custom cryptbank dataset
    into SQL text.
    """
    file_path: str = get_sql_test_filename(
        f"{cryptbank_pipeline_test_data.test_name}_{enable_mask_rewrites}",
        sqlite_tpch_db_context.dialect,
    )
    cryptbank_pipeline_test_data.run_sql_test(
        masked_graphs,
        file_path,
        update_tests,
        sqlite_tpch_db_context,
        mask_server=mock_server_info,
    )


@pytest.mark.execute
def test_pipeline_e2e_cryptbank(
    cryptbank_pipeline_test_data: PyDoughPandasTest,
    masked_graphs: graph_fetcher,
    sqlite_cryptbank_connection: DatabaseContext,
    enable_mask_rewrites: str,
    mock_server_info: MaskServerInfo,
):
    """
    Test executing the the custom queries with the custom cryptbank dataset
    against the refsol DataFrame.
    """
    cryptbank_pipeline_test_data.run_e2e_test(
        masked_graphs,
        sqlite_cryptbank_connection,
        mask_server=mock_server_info,
    )
