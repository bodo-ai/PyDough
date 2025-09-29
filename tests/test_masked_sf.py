import datetime
from collections.abc import Callable

import pandas as pd
import pytest

from pydough.database_connectors import DatabaseContext, DatabaseDialect
from tests.testing_utilities import graph_fetcher

from .testing_sf_masked_utilities import (
    PyDoughSnowflakeMaskedTest,
    get_sf_masked_graphs,  # noqa: F401
    sf_masked_context,  # noqa: F401
)


@pytest.fixture(
    params=[
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "result = ("
                "    customers"
                "    .CALCULATE(first_name, last_name, city, zipcode, date_of_birth)"
                "    .TOP_K(5, by=last_name.ASC())"
                ")",
                "FSI",
                "fsi_scan_topk",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            "FIRST_NAME": [
                                "Gabriel",
                                "Bryan",
                                "Elizabeth",
                                "Andrew",
                                "Eugene",
                            ],
                            "LAST_NAME": [
                                "Adams",
                                "Alvarado",
                                "Anderson",
                                "Andrews",
                                "Archer",
                            ],
                            "CITY": [
                                "Rileyside",
                                "South Johnhaven",
                                "Lake Jamesland",
                                "Port David",
                                "South Michael",
                            ],
                            "ZIPCODE": [34186, 34508, 36574, 41540, 95081],
                            "DATE_OF_BIRTH": [
                                "1960-03-01",
                                "1957-02-09",
                                "1961-08-20",
                                "1994-10-07",
                                "1987-07-25",
                            ],
                        }
                    ),
                },
            ),
            id="fsi_scan_topk",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_customers = customers.WHERE(ISIN(last_name, ('Barnes', 'Hernandez', 'Moore')))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_customers))",
                "FSI",
                "fsi_customers_filter_isin",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [8]}),
                },
            ),
            id="fsi_customers_filter_isin",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_customers = customers.WHERE(~ISIN(last_name, ('Barnes', 'Hernandez', 'Moore')))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_customers))",
                "FSI",
                "fsi_customers_filter_not_isin",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [192]}),
                },
            ),
            id="fsi_customers_filter_not_isin",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE( loyalty_tier == 'Platinum')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_equals",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [117]}),
                },
            ),
            id="retail_members_filter_equals",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE( loyalty_tier != 'Platinum')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_not_equals",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [383]}),
                },
            ),
            id="retail_members_filter_not_equals",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients = patients.WHERE(STARTSWITH(phone_number, '001'))\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_patients_filter_startswith",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [166]}),
                },
            ),
            id="health_patients_filter_startswith",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients = patients.WHERE(ENDSWITH(email, 'gmail.com'))\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_patients_filter_endswith",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [325]}),
                },
            ),
            id="health_patients_filter_endswith",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients = claims.WHERE(YEAR(claim_date) > 2020)\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_claims_filter_year",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [6000]}),
                },
            ),
            id="health_claims_filter_year",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients = claims.WHERE(MONTH(claim_date) == 12)\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_claims_filter_month",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [502]}),
                },
            ),
            id="health_claims_filter_month",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients = claims.WHERE(DAY(claim_date) == 31)\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_claims_filter_day",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [117]}),
                },
            ),
            id="health_claims_filter_day",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_accounts = accounts.WHERE(MONOTONIC(8000, balance, 9000))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_accounts))",
                "FSI",
                "health_accounts_filter_monotonic",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [7]}),
                },
            ),
            id="health_accounts_filter_monotonic",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members= loyalty_members.WHERE(join_date > '2025-01-01')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_datetime",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [53]}),
                },
            ),
            id="retail_members_filter_datetime",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(join_date > '2026-01-01')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_never",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [0]}),
                },
            ),
            id="retail_members_filter_never",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(ENDSWITH(first_name, 'e') | ENDSWITH(last_name, 'e'))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_name_endswith",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [87]}),
                },
            ),
            id="retail_members_filter_name_endswith",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(first_name[1:2] == 'a')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_slice",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [144]}),
                },
            ),
            id="retail_members_filter_slice",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(LIKE(email, '%.%@%mail%'))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_email_like",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [339]}),
                },
            ),
            id="retail_members_filter_email_like",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(~CONTAINS(email, 'mail'))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_email_contains",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [161]}),
                },
            ),
            id="retail_members_filter_email_contains",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_patients= patients.WHERE(ABSENT(date_of_birth) | (date_of_birth > '2003-06-29'))\n"
                "result = HEALTH.CALCULATE(n=COUNT(selected_patients))",
                "HEALTH",
                "health_patients_filter_absent",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [58]}),
                },
            ),
            id="health_patients_filter_absent",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_transactions = transactions.WHERE((YEAR(transaction_date) == 2025) & (MONTH(transaction_date) == 7))\n"
                "result = RETAIL.CALCULATE(n=ROUND(AVG(selected_transactions.total_amount), 2))",
                "RETAIL",
                "retail_transactions_filter",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [252.9]}),
                },
            ),
            id="retail_transactions_filter",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "acc_typs = accounts.PARTITION(name='account_types', by=account_type)\n"
                "result = acc_typs.CALCULATE(account_type, n=COUNT(accounts), avg_bal=ROUND(AVG(accounts.balance), 2))\n",
                "FSI",
                "fsi_accounts_partition_agg",
                answers={
                    "NONE": None,
                    "PARTIAL": pd.DataFrame(
                        {
                            "account_type": ["Ch**king", "Sa*ings"],
                            "n": [153, 144],
                            "avg_bal": [14868.66, 15613.95],
                        }
                    ),
                    "FULL": pd.DataFrame(
                        {
                            "account_type": ["Checking", "Savings"],
                            "n": [153, 144],
                            "avg_bal": [14868.66, 15613.95],
                        }
                    ),
                },
            ),
            id="fsi_accounts_partition_agg",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "account_info= loyalty_members.CALCULATE("
                " first_transaction_sent=MIN(transactions.transaction_date),"
                ")\n"
                "result = RETAIL.CALCULATE(avg_secs=ROUND(AVG(DATEDIFF('seconds', account_info.join_date, account_info.first_transaction_sent)), 2))",
                "RETAIL",
                "retail_members_agg",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"avg_secs": [24121125.35]}),
                },
            ),
            id="retail_members_agg",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "result = accounts.CALCULATE("
                " acct_type=account_type,"
                " pct_total_txn=ROUND(SUM(transactions.amount) / RELSUM(SUM(transactions.amount)), 2)"
                ").TOP_K(5, by=pct_total_txn.DESC())",
                "FSI",
                "fsi_accounts_agg_pct_total",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            # note: these account types are masked values
                            "acct_type": [
                                "Checking",
                                "Checking",
                                "Savings",
                                "Savings",
                                "Savings",
                            ],
                            "pct_total_txn": [0.23, 0.22, 0.20, 0.19, 0.18],
                        }
                    ),
                },
            ),
            id="fsi_accounts_agg_pct_total",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "mbr_info= transactions.CALCULATE("
                " name=JOIN_STRINGS(' ', member.first_name, member.last_name)"
                " ).PARTITION(name='store_locations', by=store_location)\n"
                "result = mbr_info.transactions.BEST(per='store_locations', by=total_amount.DESC()).CALCULATE(\n"
                "    store_location,"
                "    total_amount,"
                "    name"
                ").TOP_K(5, by=total_amount.DESC())",
                "RETAIL",
                "retail_members_agg_best",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            # note: store_location are masked values
                            "store_location": [
                                "Coxstad",
                                "Normanside",
                                "West Michelle",
                                "Johnview",
                                "East Samuel",
                            ],
                            "total_amount": [499.99, 499.75, 499.61, 499.44, 499.32],
                            "name": [
                                "Andrew Reilly",
                                "Christopher Garcia",
                                "Megan Norton",
                                "Steve Williams",
                                "Jillian Ritter",
                            ],
                        }
                    ),
                },
            ),
            id="retail_members_agg_best",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_customers = customers.accounts.WHERE(account_type != 'checking')\n"
                "result = FSI.CALCULATE(num_customers_checking_accounts=COUNT(selected_customers))",
                "FSI",
                "fsi_customers_accounts_join",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"NUM_CUSTOMERS_CHECKING_ACCOUNTS": [297]}),
                },
            ),
            id="fsi_customers_accounts_join",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "result = claims.TOP_K(2, by=claim_amount.DESC())",
                "HEALTH",
                "health_claim_scan_topk",
                order_sensitive=True,
                answers={
                    "NONE": pd.DataFrame(
                        {
                            "claim_id": [318700265, 521118761],
                            "patient_id": [None, None],
                            "claim_date": [None, None],
                            "provider_name": [None, None],
                            "diagnosis_code": ["lIg80", "IkU93"],
                            "procedure_code": ["nw235", "hk915"],
                            "claim_amount": [9999.53, 9997.96],
                            "approved_amount": [3505.62, 3000.31],
                            "claim_status": [None, None],
                        }
                    ),
                    "PARTIAL": pd.DataFrame(
                        {
                            "claim_id": [318700265, 521118761],
                            "patient_id": ["62***6815", "65***3879"],
                            "claim_date": [None, None],
                            "provider_name": [None, None],
                            "diagnosis_code": ["lIg80", "IkU93"],
                            "procedure_code": ["nw235", "hk915"],
                            "claim_amount": [9999.53, 9997.96],
                            "approved_amount": [3505.62, 3000.31],
                            "claim_status": ["Pe*ding", "Ap**oved"],
                        }
                    ),
                    "FULL": pd.DataFrame(
                        {
                            "claim_id": [318700265, 521118761],
                            "patient_id": ["622136815", "651103879"],
                            "claim_date": ["2023-09-15", "2023-07-25"],
                            "provider_name": ["Carr, Martinez and Fuller", "Weeks Ltd"],
                            "diagnosis_code": ["lIg80", "IkU93"],
                            "procedure_code": ["nw235", "hk915"],
                            "claim_amount": [9999.53, 9997.96],
                            "approved_amount": [3505.62, 3000.31],
                            "claim_status": ["Pending", "Approved"],
                        }
                    ),
                },
            ),
            id="health_claim_scan_topk",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "coverage_groups = insurance_plans.PARTITION(name='coverage_types', by=coverage_type)\n"
                "oldest_patient = insurance_plans.patients.BEST(per='coverage_types', by=(date_of_birth.ASC(), key.ASC()))\n"
                "result = coverage_groups.CALCULATE(coverage_type, oldest_patient.first_name, oldest_patient.last_name, oldest_patient.date_of_birth).ORDER_BY(coverage_type.ASC())",
                "HEALTH",
                "health_first_patient_by_coverage_type",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            "coverage_type": ["EPO", "HMO", "PPO"],
                            "first_name": ["Christian", "Kathleen", "Shelby"],
                            "last_name": ["Russell", "Greene", "Davis"],
                            "date_of_birth": ["1934-10-10", "1934-08-31", "1934-12-24"],
                        }
                    ),
                },
            ),
            id="health_first_patient_by_coverage_type",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_transactions = transactions.WHERE(payment_method == 'Cash')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_transactions))",
                "RETAIL",
                "retail_transactions_payment_method_cmp_a",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [860]}),
                },
            ),
            id="retail_transactions_payment_method_cmp_a",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_transactions = transactions.WHERE(payment_method != 'Credit Card')\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_transactions))",
                "RETAIL",
                "retail_transactions_payment_method_cmp_b",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [2544]}),
                },
            ),
            id="retail_transactions_payment_method_cmp_b",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_transactions = transactions.WHERE(ISIN(payment_method, ('Cash', 'Gift Card')))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_transactions))",
                "RETAIL",
                "retail_transactions_payment_method_cmp_c",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [1704]}),
                },
            ),
            id="retail_transactions_payment_method_cmp_c",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_transactions = transactions.WHERE(~ISIN(payment_method, ('Mobile Payment', 'Gift Card')))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_transactions))",
                "RETAIL",
                "retail_transactions_payment_method_cmp_d",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [1716]}),
                },
            ),
            id="retail_transactions_payment_method_cmp_d",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(ISIN(last_name, ('Johnson', 'Robinson')) & (date_of_birth >= datetime.date(2002, 1, 1)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_a",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [4]}),
                },
            ),
            id="retail_members_compound_a",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((last_name != 'Smith') & (date_of_birth == datetime.date(1979, 3, 7)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_b",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [2]}),
                },
            ),
            id="retail_members_compound_b",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((last_name < 'Cross') & (date_of_birth > datetime.date(1995, 12, 22)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_c",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [9]}),
                },
            ),
            id="retail_members_compound_c",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((last_name <= 'Zuniga') & (date_of_birth >= datetime.date(2000, 1, 1)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_d",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [55]}),
                },
            ),
            id="retail_members_compound_d",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((datetime.date(1983, 1, 10) <= date_of_birth) & (date_of_birth < datetime.date(1983, 1, 30)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_e",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [2]}),
                },
            ),
            id="retail_members_compound_e",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((datetime.date(1976, 7, 1) < date_of_birth) & (date_of_birth <= datetime.date(1976, 7, 28)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_f",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [2]}),
                },
            ),
            id="retail_members_compound_f",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(ISIN(YEAR(date_of_birth), (1960, 1970, 1980, 1990, 2000)) & ISIN(MONTH(date_of_birth), (1, 2, 5, 10, 12)) & (DAY(date_of_birth) <= 13) & (DAY(date_of_birth) > 3))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_g",
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [7]}),
                },
            ),
            id="retail_members_compound_g",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE((last_name >= 'Cross') & (date_of_birth < datetime.date(2007, 1, 1)))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_h",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [399]}),
                },
            ),
            id="retail_members_compound_h",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(ISIN(UPPER(last_name)[1:3], ('UA', 'CO', 'AY', 'AL')))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_i",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [25]}),
                },
            ),
            id="retail_members_compound_i",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = loyalty_members.WHERE(CONTAINS(LOWER(last_name), 'hu'))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_compound_j",
                kwargs={"datetime": datetime},
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [7]}),
                },
            ),
            id="retail_members_compound_j",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "state_groups = customers.PARTITION(name='states', by=state).CALCULATE(state).TOP_K(5, by=state.ASC())\n"
                "best_acct = state_groups.customers.CALCULATE(first_name, last_name).accounts.BEST(per='states', by=balance.DESC())\n"
                "result = best_acct.CALCULATE(state, balance, first_name, last_name).ORDER_BY(state.ASC())\n",
                "FSI",
                "fsi_best_account_customers_per_state",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            "state": [
                                "Alabama",
                                "Alaska",
                                "Arizona",
                                "Arkansas",
                                "California",
                            ],
                            "balance": [
                                26755.10,
                                26933.38,
                                26228.85,
                                13977.68,
                                27905.88,
                            ],
                            "first_name": [
                                "Eugene",
                                "Michelle",
                                "Ryan",
                                "Kara",
                                "Gloria",
                            ],
                            "last_name": [
                                "Spence",
                                "Moore",
                                "Brown",
                                "Schwartz",
                                "Miller",
                            ],
                        }
                    ),
                },
            ),
            id="fsi_best_account_customers_per_state",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = accounts.WHERE((currency != 'GBP') & (customer.state == 'California') & (balance < 20000))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_members))",
                "FSI",
                "fsi_accounts_customers_compound_a",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [6]}),
                },
            ),
            id="fsi_accounts_customers_compound_a",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = accounts.WHERE(ISIN(currency, ('USD', 'GPB', 'EUR', 'JPY', 'AUD')) & ~ISIN(customer.first_name, ['Jennifer', 'Julio', 'Johnson', 'Jameson', 'Michael', 'Robert']) & ISIN(customer.state, ('Georgia', 'Alabama', 'Mississippi', 'Arkansas', 'Louisiana', 'Florida', 'South Carolina', 'North Carolina', 'Texas', 'Tennessee', 'Missouri')) & (YEAR(creation_date) <= 2022))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_members))",
                "FSI",
                "fsi_accounts_customers_compound_b",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [16]}),
                },
            ),
            id="fsi_accounts_customers_compound_b",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_members = accounts.WHERE(MONOTONIC('2020-01-31', creation_date, '2020-03-13') | MONOTONIC('2022-12-25', creation_date, '2023-01-15') | MONOTONIC('2024-08-04', creation_date, '2024-11-08') | MONOTONIC('2025-06-07', creation_date, '2026-03-07'))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_members))",
                "FSI",
                "fsi_accounts_customers_compound_c",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [28]}),
                },
            ),
            id="fsi_accounts_customers_compound_c",
        ),
    ],
)
def sf_masked_test_data(
    request,
) -> PyDoughSnowflakeMaskedTest:
    """
    Returns a dataclass encapsulating all of the information needed to run the
    PyDough code and compare the result against the refsol.
    """
    return request.param


@pytest.mark.sf_masked
def test_pipeline_until_relational_masked_sf(
    sf_masked_test_data: PyDoughSnowflakeMaskedTest,
    get_sf_masked_graphs: graph_fetcher,  # noqa: F811
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
    enable_mask_rewrites: str,
) -> None:
    """
    Tests the conversion of the PyDough queries on the masked dataset
    into relational plans.
    """
    file_path: str = get_plan_test_filename(
        f"{sf_masked_test_data.test_name}_{enable_mask_rewrites}"
    )
    sf_masked_test_data.run_relational_test(
        get_sf_masked_graphs, file_path, update_tests
    )


@pytest.mark.sf_masked
def test_pipeline_until_sql_masked_sf(
    sf_masked_test_data: PyDoughSnowflakeMaskedTest,
    get_sf_masked_graphs: graph_fetcher,  # noqa: F811
    sf_masked_context: Callable[[str, str, str], DatabaseContext],  # noqa: F811
    get_sql_test_filename: Callable[[str, DatabaseDialect], str],
    update_tests: bool,
    enable_mask_rewrites: str,
):
    """
    Tests the conversion of the PyDough queries on the custom masked dataset
    into SQL text.
    """
    sf_data = sf_masked_context("BODO", sf_masked_test_data.graph_name, "FULL")
    file_path: str = get_sql_test_filename(
        f"{sf_masked_test_data.test_name}_{enable_mask_rewrites}", sf_data.dialect
    )
    sf_masked_test_data.run_sql_test(
        get_sf_masked_graphs,
        file_path,
        update_tests,
        sf_data,
    )


@pytest.mark.skip(
    reason="Skipping until masked table column relational handling is implemented"
)
@pytest.mark.execute
@pytest.mark.sf_masked
@pytest.mark.parametrize("account_type", ["NONE", "PARTIAL", "FULL"])
def test_pipeline_e2e_masked_sf(
    account_type: str,
    sf_masked_test_data: PyDoughSnowflakeMaskedTest,
    get_sf_masked_graphs: graph_fetcher,  # noqa: F811
    sf_masked_context: Callable[[str, str, str], DatabaseContext],  # noqa: F811
    enable_mask_rewrites: str,  # noqa: F811
) -> None:
    """
    End-to-end test for Snowflake with masked columns.
    """
    sf_masked_test_data.account_type = account_type
    if sf_masked_test_data.answers.get(account_type) is None:
        pytest.skip(f"No reference solution for account_type={account_type}")
    sf_masked_test_data.run_e2e_test(
        get_sf_masked_graphs,
        sf_masked_context("BODO", sf_masked_test_data.graph_name, account_type),
        coerce_types=True,
    )
