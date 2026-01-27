"""
Integration tests for the PyDough workflow with custom questions on the custom
CRYPTBANK sqlite database.
"""

import datetime
import io
from collections.abc import Callable
from contextlib import redirect_stdout

import pandas as pd
import pytest

from pydough import to_sql
from pydough.database_connectors import DatabaseContext, DatabaseDialect
from pydough.mask_server import MaskServerInfo
from pydough.metadata import GraphMetadata
from pydough.unqualified import UnqualifiedNode
from tests.testing_utilities import (
    PyDoughPandasTest,
    extract_batch_requests_from_logs,
    graph_fetcher,
    transform_and_exec_pydough,
)


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
                "selected_customers = customers.WHERE(birthday <= '1925-01-01')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [0]}),
                "cryptbank_filter_count_29",
            ),
            id="cryptbank_filter_count_29",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE("
                " ISIN(YEAR(birthday) - 2, (1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993))"
                " & ISIN(MONTH(birthday) + 1, (2, 4, 6, 8, 10, 12))"
                ")\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_30",
            ),
            id="cryptbank_filter_count_30",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(birthday, [datetime.date(1991, 11, 15), datetime.date(1978, 2, 11), datetime.date(2005, 3, 14), datetime.date(1985, 4, 12)]))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "cryptbank_filter_count_31",
                kwargs={"datetime": datetime, "pd": pd},
            ),
            id="cryptbank_filter_count_31",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(MONOTONIC(pd.Timestamp('2020-03-28 09:20:00'), creation_timestamp, datetime.datetime(2020, 9, 20, 8, 30, 0)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_32",
                kwargs={"datetime": datetime, "pd": pd},
            ),
            id="cryptbank_filter_count_32",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(QUARTER(creation_timestamp) == 1)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [11]}),
                "cryptbank_filter_count_33",
            ),
            id="cryptbank_filter_count_33",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(QUARTER(creation_timestamp) == DAY(creation_timestamp))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "cryptbank_filter_count_34",
            ),
            id="cryptbank_filter_count_34",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE((HOUR(creation_timestamp) < 10) & (MINUTE(creation_timestamp) < 20))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [6]}),
                "cryptbank_filter_count_35",
            ),
            id="cryptbank_filter_count_35",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_transactions = transactions.WHERE(SECOND(time_stamp) == 23)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_transactions))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "cryptbank_filter_count_36",
            ),
            id="cryptbank_filter_count_36",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(MONOTONIC(200, ABS(balance - 7250), 600))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "cryptbank_filter_count_37",
            ),
            id="cryptbank_filter_count_37",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(LARGEST(HOUR(creation_timestamp), MINUTE(creation_timestamp), SECOND(creation_timestamp)) == 10)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "cryptbank_filter_count_38",
            ),
            id="cryptbank_filter_count_38",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_accounts = accounts.WHERE(SMALLEST(HOUR(creation_timestamp), MINUTE(creation_timestamp)) == 15)\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_39",
            ),
            id="cryptbank_filter_count_39",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('', '1-', phone_number), '1-5'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [20]}),
                "cryptbank_filter_count_40",
            ),
            id="cryptbank_filter_count_40",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('-', '1', phone_number), '1-5'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [20]}),
                "cryptbank_filter_count_41",
            ),
            id="cryptbank_filter_count_41",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('-', '1', phone_number, '1'), '5-1'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_42",
            ),
            id="cryptbank_filter_count_42",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(JOIN_STRINGS(' ', first_name, last_name) == 'olivia anderson')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "cryptbank_filter_count_43",
            ),
            id="cryptbank_filter_count_43",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(DEFAULT_TO(YEAR(birthday), 1990), (1990, 1991)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_44",
            ),
            id="cryptbank_filter_count_44",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(DEFAULT_TO(YEAR(birthday), 1990), (1990, 2005)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [3]}),
                "cryptbank_filter_count_45",
            ),
            id="cryptbank_filter_count_45",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(DEFAULT_TO(YEAR(birthday), 2005), (2005, 2006)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [2]}),
                "cryptbank_filter_count_46",
            ),
            id="cryptbank_filter_count_46",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(DEFAULT_TO(YEAR(birthday), 1990), (1990, 1991)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [16]}),
                "cryptbank_filter_count_47",
            ),
            id="cryptbank_filter_count_47",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(DEFAULT_TO(YEAR(birthday), 1990), (1990, 2005)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [17]}),
                "cryptbank_filter_count_48",
            ),
            id="cryptbank_filter_count_48",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(DEFAULT_TO(YEAR(birthday), 2005), (2005, 2006)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [18]}),
                "cryptbank_filter_count_49",
            ),
            id="cryptbank_filter_count_49",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(~ISIN(DEFAULT_TO(YEAR(birthday), 2005), (2005, 2006)))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [18]}),
                "cryptbank_filter_count_50",
            ),
            id="cryptbank_filter_count_50",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(IFF(ISIN(first_name[:1], ('q', 'r', 's')), first_name, last_name), 'ee'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [4]}),
                "cryptbank_filter_count_51",
            ),
            id="cryptbank_filter_count_51",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(last_name, JOIN_STRINGS('', 'e', IFF(ISIN(last_name[:1], ('q', 'r', 's')), 'z', 'e'))))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_52",
            ),
            id="cryptbank_filter_count_52",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(first_name[0:1] == 'i')\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [1]}),
                "cryptbank_filter_count_53",
            ),
            id="cryptbank_filter_count_53",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(first_name[-1:], list('aeiou')))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [7]}),
                "cryptbank_filter_count_54",
            ),
            id="cryptbank_filter_count_54",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(first_name[1:3], ['ar', 'li', 'ra', 'to', 'am']))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [8]}),
                "cryptbank_filter_count_55",
            ),
            id="cryptbank_filter_count_55",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(ISIN(first_name[-2:-1], ['a', 'c', 'l']))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_56",
            ),
            id="cryptbank_filter_count_56",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(first_name[:-1], 'e'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [8]}),
                "cryptbank_filter_count_57",
            ),
            id="cryptbank_filter_count_57",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS(first_name[1:-1], 'e'))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [7]}),
                "cryptbank_filter_count_58",
            ),
            id="cryptbank_filter_count_58",
        ),
        pytest.param(
            PyDoughPandasTest(
                "selected_customers = customers.WHERE(CONTAINS('SLICE', UPPER(first_name[:1])))\n"
                "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n": [5]}),
                "cryptbank_filter_count_59",
            ),
            id="cryptbank_filter_count_59",
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
                "result = CRYPTBANK.CALCULATE(n_neg=SUM(transactions.amount < 0), n_positive=SUM(transactions.amount > 0))",
                "CRYPTBANK",
                lambda: pd.DataFrame({"n_neg": [0], "n_positive": [300]}),
                "cryptbank_agg_06",
            ),
            id="cryptbank_agg_06",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = CRYPTBANK.CALCULATE("
                " n_yr=SUM(DATETIME(transactions.time_stamp, 'start of year') == '2023-01-01'),"
                " n_qu=SUM(DATETIME(transactions.time_stamp, 'start of quarter') == '2023-04-01'),"
                " n_mo=SUM(DATETIME(transactions.time_stamp, 'start of month') == '2023-06-01'),"
                " n_we=SUM(DATETIME(transactions.time_stamp, 'start of week') == '2023-05-28'),"
                " n_da=SUM(DATETIME(transactions.time_stamp, 'start of day') == '2023-06-02'),"
                " n_ho=SUM(DATETIME(transactions.time_stamp, 'start of hour') == '2023-06-02 04:00:00'),"
                " n_mi=SUM(DATETIME(transactions.time_stamp, 'start of minute') == '2023-06-02 04:55:00'),"
                " n_se=SUM(DATETIME(transactions.time_stamp, 'start of second') == '2023-06-02 04:55:31'),"
                " n_cts=SUM(transactions.time_stamp == DATETIME('now', 'start of day')),"
                " n_dts=SUM(transactions.time_stamp == DATETIME(JOIN_STRINGS('-', '2025', '12', '31'))),"
                " n_nst=SUM(DATETIME(transactions.time_stamp, 'start of week', '+3 days') == '2023-05-31'),"
                " n_ayr=SUM(DATETIME(transactions.time_stamp, '+1 Y') == '2020-11-11 18:00:52'),"
                " n_aqu=SUM(DATETIME(transactions.time_stamp, '+2 q') == '2020-05-11 18:00:52'),"
                " n_amo=SUM(DATETIME(transactions.time_stamp, '-5 Mm') == '2019-06-11 18:00:52'),"
                " n_awe=SUM(DATETIME(transactions.time_stamp, 'start of day', '+1 week') == '2023-06-09'),"
                " n_ada=SUM(DATETIME(transactions.time_stamp, '+10 DAYS') == '2019-11-21 18:00:52'),"
                " n_aho=SUM(DATETIME(transactions.time_stamp, '+1000 hour') == '2019-12-23 10:00:52'),"
                " n_ami=SUM(DATETIME(transactions.time_stamp, '+10000 minute') == '2019-11-18 16:40:52'),"
                " n_ase=SUM(DATETIME(transactions.time_stamp, '-1000000 s') == '2019-10-31 04:14:12'),"
                " n_ldm=SUM(DATETIME(transactions.time_stamp, 'start of month', '-1 day') == '2019-10-31'),"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "n_yr": [61],
                        "n_qu": [17],
                        "n_mo": [8],
                        "n_we": [2],
                        "n_da": [2],
                        "n_ho": [2],
                        "n_mi": [2],
                        "n_se": [1],
                        "n_cts": [0],
                        "n_dts": [0],
                        "n_nst": [2],
                        "n_ayr": [1],
                        "n_aqu": [1],
                        "n_amo": [1],
                        "n_awe": [2],
                        "n_ada": [1],
                        "n_aho": [1],
                        "n_ami": [1],
                        "n_ase": [1],
                        "n_ldm": [4],
                    }
                ),
                "cryptbank_agg_07",
            ),
            id="cryptbank_agg_07",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " accounts"
                " .CALCULATE(partkey=(account_type == 'retirement') | (account_type == 'savings'))"
                " .PARTITION(name='actyp', by=partkey)"
                " .accounts"
                " .BEST(per='actyp', by=balance.DESC())"
                " .CALCULATE(account_type, key, balance)"
                " .ORDER_BY(account_type.ASC())"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "account_type": ["mma", "retirement"],
                        "key": [8, 28],
                        "balance": [5500.0, 25000.0],
                    }
                ),
                "cryptbank_window_01",
            ),
            id="cryptbank_window_01",
        ),
        pytest.param(
            PyDoughPandasTest(
                "result = ("
                " branches"
                " .WHERE(CONTAINS(address, ';CA;'))"
                " .CALCULATE(branch_name=name)"
                " .accounts_managed"
                " .BEST(per='branches', by=((YEAR(creation_timestamp) == 2021).ASC(), key.ASC()))"
                " .CALCULATE(branch_name, key, creation_timestamp)"
                " .ORDER_BY(branch_name.ASC())"
                ")",
                "CRYPTBANK",
                lambda: pd.DataFrame(
                    {
                        "branch_name": [
                            "Downtown Los Angeles Branch",
                            "San Francisco Financial Branch",
                        ],
                        "key": [14, 8],
                        "creation_timestamp": [
                            "2016-05-12 14:00:00",
                            "2018-07-19 14:10:00",
                        ],
                    }
                ),
                "cryptbank_window_02",
            ),
            id="cryptbank_window_02",
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
    # Capture stdout to avoid polluting the console with logging calls
    with redirect_stdout(io.StringIO()):
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
    # Capture stdout to avoid polluting the console with logging calls
    with redirect_stdout(io.StringIO()):
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
    # Capture stdout to avoid polluting the console with logging calls
    with redirect_stdout(io.StringIO()):
        cryptbank_pipeline_test_data.run_e2e_test(
            masked_graphs,
            sqlite_cryptbank_connection,
            mask_server=mock_server_info,
        )


@pytest.mark.parametrize(
    ["pydough_code", "batch_requests"],
    [
        pytest.param(
            "selected_customers = customers.WHERE(last_name == 'lee')\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {"CRBNK/CUSTOMERS/c_lname: ['EQUAL', 2, '__col__', 'lee']", "DRY_RUN"},
                {"CRBNK/CUSTOMERS/c_lname: ['EQUAL', 2, '__col__', 'lee']"},
            ],
            id="cryptbank_filter_count_01",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_lname: ['IN', 4, '__col__', 'lee', 'smith', 'rodriguez']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_lname: ['IN', 4, '__col__', 'lee', 'smith', 'rodriguez']"
                },
            ],
            id="cryptbank_filter_count_03",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(~ISIN(last_name, ('lee', 'smith', 'rodriguez')))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_lname: ['IN', 4, '__col__', 'lee', 'smith', 'rodriguez']",
                    "CRBNK/CUSTOMERS/c_lname: ['NOT', 1, 'IN', 4, '__col__', 'lee', 'smith', 'rodriguez']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_lname: ['NOT', 1, 'IN', 4, '__col__', 'lee', 'smith', 'rodriguez']",
                },
            ],
            id="cryptbank_filter_count_04",
        ),
        pytest.param(
            "selected_customers = customers.WHERE("
            " ("
            "  PRESENT(address) &"
            "  PRESENT(birthday) &"
            "  (last_name != 'lopez') &"
            "  (ENDSWITH(first_name, 'a') | ENDSWITH(first_name, 'e') | ENDSWITH(first_name, 's'))"
            ") | (ABSENT(birthday) & ENDSWITH(phone_number, '5'))"
            ")\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['ENDSWITH', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['ENDSWITH', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['ENDSWITH', 2, '__col__', 's']",
                    "CRBNK/CUSTOMERS/c_fname: ['OR', 2, 'ENDSWITH', 2, '__col__', 'a', 'ENDSWITH', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['OR', 2, 'OR', 2, 'ENDSWITH', 2, '__col__', 'a', 'ENDSWITH', 2, '__col__', 'e', 'ENDSWITH', 2, '__col__', 's']",
                    "CRBNK/CUSTOMERS/c_lname: ['NOT_EQUAL', 2, '__col__', 'lopez']",
                    "CRBNK/CUSTOMERS/c_phone: ['ENDSWITH', 2, '__col__', '5']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['ENDSWITH', 2, '__col__', 's']",
                    "CRBNK/CUSTOMERS/c_fname: ['OR', 2, 'ENDSWITH', 2, '__col__', 'a', 'ENDSWITH', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_lname: ['NOT_EQUAL', 2, '__col__', 'lopez']",
                    "CRBNK/CUSTOMERS/c_phone: ['ENDSWITH', 2, '__col__', '5']",
                },
            ],
            id="cryptbank_filter_count_27",
        ),
        pytest.param(
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
            [
                {
                    "CRBNK/ACCOUNTS/a_balance: ['GTE', 2, '__col__', 5000]",
                    "CRBNK/ACCOUNTS/a_open_ts: ['LT', 2, 'YEAR', 1, '__col__', 2020]",
                    "CRBNK/ACCOUNTS/a_type: ['EQUAL', 2, '__col__', 'retirement']",
                    "CRBNK/ACCOUNTS/a_type: ['EQUAL', 2, '__col__', 'savings']",
                    "CRBNK/CUSTOMERS/c_email: ['CONTAINS', 2, '__col__', 'gmail']",
                    "CRBNK/CUSTOMERS/c_email: ['CONTAINS', 2, '__col__', 'outlook']",
                    "CRBNK/ACCOUNTS/a_type: ['OR', 2, 'EQUAL', 2, '__col__', 'retirement', 'EQUAL', 2, '__col__', 'savings']",
                    "CRBNK/CUSTOMERS/c_email: ['OR', 2, 'CONTAINS', 2, '__col__', 'outlook', 'CONTAINS', 2, '__col__', 'gmail']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/ACCOUNTS/a_type: ['OR', 2, 'EQUAL', 2, '__col__', 'retirement', 'EQUAL', 2, '__col__', 'savings']",
                },
            ],
            id="cryptbank_filter_count_28",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(birthday <= '1925-01-01')\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['LTE', 2, '__col__', '1925-01-01']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['LTE', 2, '__col__', '1925-01-01']",
                },
            ],
            id="cryptbank_filter_count_29",
        ),
        pytest.param(
            "selected_customers = customers.WHERE("
            " ISIN(YEAR(birthday) - 2, (1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993))"
            " & ISIN(MONTH(birthday) + 1, (2, 4, 6, 8, 10, 12))"
            ")\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['AND', 2, 'IN', 7, 'ADD', 2, 'MONTH', 1, '__col__', 1, 2, 4, 6, 8, 10, 12, 'IN', 11, 'SUB', 2, 'YEAR', 1, '__col__', 2, 1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993]",
                    "CRBNK/CUSTOMERS/c_birthday: ['IN', 11, 'SUB', 2, 'YEAR', 1, '__col__', 2, 1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993]",
                    "CRBNK/CUSTOMERS/c_birthday: ['IN', 7, 'ADD', 2, 'MONTH', 1, '__col__', 1, 2, 4, 6, 8, 10, 12]",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['AND', 2, 'IN', 7, 'ADD', 2, 'MONTH', 1, '__col__', 1, 2, 4, 6, 8, 10, 12, 'IN', 11, 'SUB', 2, 'YEAR', 1, '__col__', 2, 1975, 1977, 1979, 1981, 1983, 1985, 1987, 1989, 1991, 1993]",
                },
            ],
            id="cryptbank_filter_count_30",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(ISIN(birthday, [datetime.date(1991, 11, 15), datetime.date(1978, 2, 11), datetime.date(2005, 3, 14), datetime.date(1985, 4, 12)]))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['IN', 5, '__col__', '1991-11-15', '1978-02-11', '2005-03-14', '1985-04-12']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_birthday: ['IN', 5, '__col__', '1991-11-15', '1978-02-11', '2005-03-14', '1985-04-12']",
                },
            ],
            id="cryptbank_filter_count_31",
        ),
        pytest.param(
            "selected_accounts = accounts.WHERE(MONOTONIC(pd.Timestamp('2020-03-28 09:20:00'), creation_timestamp, datetime.datetime(2020, 9, 20, 8, 30, 0)))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
            [
                {
                    "CRBNK/ACCOUNTS/a_open_ts: ['AND', 2, 'LTE', 2, '2020-03-28 09:20:00', '__col__', 'LTE', 2, '__col__', '2020-09-20 08:30:00']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/ACCOUNTS/a_open_ts: ['AND', 2, 'LTE', 2, '2020-03-28 09:20:00', '__col__', 'LTE', 2, '__col__', '2020-09-20 08:30:00']",
                },
            ],
            id="cryptbank_filter_count_32",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE(n_neg=SUM(transactions.amount < 0), n_positive=SUM(transactions.amount > 0))",
            [
                {
                    "CRBNK/TRANSACTIONS/t_amount: ['LT', 2, '__col__', 0]",
                    "CRBNK/TRANSACTIONS/t_amount: ['GT', 2, '__col__', 0]",
                    "DRY_RUN",
                },
                {
                    "CRBNK/TRANSACTIONS/t_amount: ['LT', 2, '__col__', 0]",
                    "CRBNK/TRANSACTIONS/t_amount: ['GT', 2, '__col__', 0]",
                },
            ],
            id="cryptbank_agg_06",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            " n_yr=SUM(DATETIME(transactions.time_stamp, 'start of year') == '2023-01-01'),"
            " n_qu=SUM(DATETIME(transactions.time_stamp, 'start of quarter') == '2023-04-01'),"
            " n_mo=SUM(DATETIME(transactions.time_stamp, 'start of month') == '2023-06-01'),"
            " n_we=SUM(DATETIME(transactions.time_stamp, 'start of week') == '2023-05-28'),"
            " n_da=SUM(DATETIME(transactions.time_stamp, 'start of day') == '2023-06-02'),"
            " n_ho=SUM(DATETIME(transactions.time_stamp, 'start of hour') == '2023-06-02 04:00:00'),"
            " n_mi=SUM(DATETIME(transactions.time_stamp, 'start of minute') == '2023-06-02 04:55:00'),"
            " n_se=SUM(DATETIME(transactions.time_stamp, 'start of second') == '2023-06-02 04:55:31'),"
            " n_cts=SUM(transactions.time_stamp == DATETIME('now', 'start of day')),"
            " n_dts=SUM(transactions.time_stamp == DATETIME(JOIN_STRINGS('-', '2025', '12', '31'))),"
            " n_nst=SUM(DATETIME(transactions.time_stamp, 'start of week', '+3 days') == '2023-05-31'),"
            " n_ayr=SUM(DATETIME(transactions.time_stamp, '+1 Y') == '2020-11-11 18:00:52'),"
            " n_aqu=SUM(DATETIME(transactions.time_stamp, '+2 q') == '2020-05-11 18:00:52'),"
            " n_amo=SUM(DATETIME(transactions.time_stamp, '-5 Mm') == '2019-06-11 18:00:52'),"
            " n_awe=SUM(DATETIME(transactions.time_stamp, 'start of day', '+1 week') == '2023-06-09'),"
            " n_ada=SUM(DATETIME(transactions.time_stamp, '+10 DAYS') == '2019-11-21 18:00:52'),"
            " n_aho=SUM(DATETIME(transactions.time_stamp, '+1000 hour') == '2019-12-23 10:00:52'),"
            " n_ami=SUM(DATETIME(transactions.time_stamp, '+10000 minute') == '2019-11-18 16:40:52'),"
            " n_ase=SUM(DATETIME(transactions.time_stamp, '-1000000 s') == '2019-10-31 04:14:12'),"
            " n_ldm=SUM(DATETIME(transactions.time_stamp, 'start of month', '-1 day') == '2019-10-31'),"
            ")",
            [
                {
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'day', '__col__', '2023-06-02']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'hour', '__col__', '2023-06-02 04:00:00']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'minute', '__col__', '2023-06-02 04:55:00']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'month', '__col__', '2023-06-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'quarter', '__col__', '2023-04-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'second', '__col__', '2023-06-02 04:55:31']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'year', '__col__', '2023-01-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 1, 'years', '__col__', '2020-11-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 2, 'quarters', '__col__', '2020-05-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -5, 'months', '__col__', '2019-06-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 10, 'days', '__col__', '2019-11-21 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 1000, 'hours', '__col__', '2019-12-23 10:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 10000, 'minutes', '__col__', '2019-11-18 16:40:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -1000000, 'seconds', '__col__', '2019-10-31 04:14:12']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -1, 'days', 'DATETRUNC', 2, 'month', '__col__', '2019-10-31']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'day', '__col__', '2023-06-02']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'hour', '__col__', '2023-06-02 04:00:00']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'minute', '__col__', '2023-06-02 04:55:00']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'month', '__col__', '2023-06-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'quarter', '__col__', '2023-04-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'second', '__col__', '2023-06-02 04:55:31']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATETRUNC', 2, 'year', '__col__', '2023-01-01']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 1, 'years', '__col__', '2020-11-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 2, 'quarters', '__col__', '2020-05-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -5, 'months', '__col__', '2019-06-11 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 10, 'days', '__col__', '2019-11-21 18:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 1000, 'hours', '__col__', '2019-12-23 10:00:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, 10000, 'minutes', '__col__', '2019-11-18 16:40:52']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -1000000, 'seconds', '__col__', '2019-10-31 04:14:12']",
                    "CRBNK/TRANSACTIONS/t_ts: ['EQUAL', 2, 'DATEADD', 3, -1, 'days', 'DATETRUNC', 2, 'month', '__col__', '2019-10-31']",
                },
            ],
            id="cryptbank_agg_07",
        ),
        pytest.param(
            "selected_accounts = accounts.WHERE(QUARTER(creation_timestamp) == DAY(creation_timestamp))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_accounts))",
            [
                {
                    "CRBNK/ACCOUNTS/a_open_ts: ['EQUAL', 2, 'QUARTER', 1, '__col__', 'DAY', 1, '__col__']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/ACCOUNTS/a_open_ts: ['EQUAL', 2, 'QUARTER', 1, '__col__', 'DAY', 1, '__col__']"
                },
            ],
            id="cryptbank_filter_count_34",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('', '1-', phone_number), '1-5'))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 2, '1-', '__col__', '1-5']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 2, '1-', '__col__', '1-5']"
                },
            ],
            id="cryptbank_filter_count_40",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('-', '1', phone_number), '1-5'))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 3, '1', '-', '__col__', '1-5']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 3, '1', '-', '__col__', '1-5']"
                },
            ],
            id="cryptbank_filter_count_41",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(CONTAINS(JOIN_STRINGS('-', '1', phone_number, '1'), '5-1'))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 5, '1', '-', '__col__', '-', '1', '5-1']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_phone: ['CONTAINS', 2, 'CONCAT', 5, '1', '-', '__col__', '-', '1', '5-1']"
                },
            ],
            id="cryptbank_filter_count_42",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(JOIN_STRINGS(' ', first_name, last_name) == 'olivia anderson')\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [],
            id="cryptbank_filter_count_43",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(CONTAINS('SLICE', UPPER(first_name[:1])))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, 'SLICE', 'UPPER', 1, 'SLICE', 3, '__col__', 0, 1]",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, 'SLICE', 'UPPER', 1, 'SLICE', 3, '__col__', 0, 1]"
                },
            ],
            id="cryptbank_filter_count_59",
        ),
        pytest.param(
            "selected_customers = customers.WHERE(ISIN(first_name, ['Datediff', 'YEAR', 'IN', 'NOT IN', 'NEQ', 'NOT_EQUAL', 'lower']))\n"
            "result = CRYPTBANK.CALCULATE(n=COUNT(selected_customers))",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['IN', 8, '__col__', 'Datediff', 'YEAR', 'IN', 'NOT IN', 'NEQ', 'NOT_EQUAL', 'lower']",
                    "DRY_RUN",
                },
            ],
            id="cryptbank_quote_list",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "CONTAINS(first_name, 'a')",
                        "CONTAINS(first_name, 'e')",
                        "CONTAINS(first_name, 'i')",
                        "CONTAINS(first_name, 'o')",
                        "CONTAINS(first_name, 'u')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                },
            ],
            id="cryptbank_multi_fcount_01",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'e')",
                        "CONTAINS(first_name, 'e') & CONTAINS(first_name, 'i')",
                        "CONTAINS(first_name, 'i') & CONTAINS(first_name, 'o')",
                        "CONTAINS(first_name, 'o') & CONTAINS(first_name, 'u')",
                        "CONTAINS(first_name, 'u') & CONTAINS(first_name, 'a')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', "
                    "'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', "
                    "'CONTAINS', 2, '__col__', 'u']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'e', "
                    "'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'i', "
                    "'CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'o', "
                    "'CONTAINS', 2, '__col__', 'u']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'i', 'CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'o', 'CONTAINS', 2, '__col__', 'u']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                },
            ],
            id="cryptbank_multi_fcount_02",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'e')",
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'i')",
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'o')",
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'u')",
                        "CONTAINS(first_name, 'a')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                },
            ],
            id="cryptbank_multi_fcount_03",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'e')",
                        "CONTAINS(first_name, 'e') & CONTAINS(first_name, 'i')",
                        "CONTAINS(first_name, 'i') & CONTAINS(first_name, 'u')",
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'i')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'i', 'CONTAINS', 2, '__col__', 'u']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                },
            ],
            id="cryptbank_multi_fcount_04",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "CONTAINS(first_name, 'a') & CONTAINS(first_name, 'e') & CONTAINS(first_name, 'i')",
                        "CONTAINS(first_name, 'e') & CONTAINS(first_name, 'i') & CONTAINS(first_name, 'o')",
                        "CONTAINS(first_name, 'i') & CONTAINS(first_name, 'o') & CONTAINS(first_name, 'u')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 3, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 3, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i', 'CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 3, 'CONTAINS', 2, '__col__', 'i', 'CONTAINS', 2, '__col__', 'o', 'CONTAINS', 2, '__col__', 'u']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 3, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 3, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i', 'CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'u']",
                },
            ],
            id="cryptbank_multi_fcount_05",
        ),
        pytest.param(
            "result = CRYPTBANK.CALCULATE("
            + ", ".join(
                f"n{idx}=COUNT(customers.WHERE({cond}))"
                for idx, cond in enumerate(
                    [
                        "~(CONTAINS(first_name, 'a') & CONTAINS(first_name, 'e')) & CONTAINS(first_name, 'i')",
                        "~(CONTAINS(first_name, 'e') & CONTAINS(first_name, 'i')) & CONTAINS(first_name, 'o')",
                    ]
                )
            )
            + ")",
            [
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'i', 'NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'o', 'NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'a']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'i']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                    "DRY_RUN",
                },
                {
                    "CRBNK/CUSTOMERS/c_fname: ['AND', 2, 'CONTAINS', 2, '__col__', 'i', 'NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'a', 'CONTAINS', 2, '__col__', 'e']",
                    "CRBNK/CUSTOMERS/c_fname: ['CONTAINS', 2, '__col__', 'o']",
                    "CRBNK/CUSTOMERS/c_fname: ['NOT', 1, 'AND', 2, 'CONTAINS', 2, '__col__', 'e', 'CONTAINS', 2, '__col__', 'i']",
                },
            ],
            id="cryptbank_multi_fcount_06",
        ),
    ],
)
def test_cryptbank_mask_server_logging(
    pydough_code: str,
    batch_requests: list[set[str]],
    masked_graphs: graph_fetcher,
    enable_mask_rewrites: str,
    mock_server_info: MaskServerInfo,
    caplog,
):
    """
    Tests whether, during the conversion of the PyDough queries on the custom
    cryptbank dataset into SQL text, the correct logging calls are made
    regarding batches sent to the mask server. This is to ensure that the calls
    are being batched as expected, the right calls are being sent to the server,
    and expressions that are non-predicates are not being sent, even if they are
    a valid sub-expression of a predicate that can be sent.
    """
    # Obtain the graph and the unqualified node
    graph: GraphMetadata = masked_graphs("CRYPTBANK")
    root: UnqualifiedNode = transform_and_exec_pydough(
        pydough_code,
        masked_graphs("CRYPTBANK"),
        {"datetime": datetime, "pd": pd},
    )

    # Convert the PyDough code to SQL text, while capturing
    # stdout to avoid polluting the console with logging calls
    with redirect_stdout(io.StringIO()):
        to_sql(root, metadata=graph, mask_server=mock_server_info)

    # Retrieve the output from the captured logger output
    batch_requests_made: list[set[str]] = extract_batch_requests_from_logs(caplog.text)

    # If in raw mode, make sure no requests were made. Otherwise, compare the
    # expected batch requests to those made.
    if enable_mask_rewrites == "raw":
        assert batch_requests_made == [], (
            "Expected no batch requests to be made in 'raw' mode."
        )
    else:
        assert batch_requests_made == batch_requests, (
            "The batch requests made do not match the expected batch requests."
        )
