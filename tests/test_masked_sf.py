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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [2804]}),
                },
            ),
            id="health_claims_filter_year",
        ),
        pytest.param(
            PyDoughSnowflakeMaskedTest(
                "selected_accounts = accounts.WHERE(MONOTONIC(8000, balance, 9000))\n"
                "result = FSI.CALCULATE(n=COUNT(selected_accounts))",
                "FSI",
                "health_accounts_filter_monotonic",
                order_sensitive=True,
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
                order_sensitive=True,
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
                "selected_members = loyalty_members.WHERE(ENDSWITH(first_name, 'e') | ENDSWITH(last_name, 'e'))\n"
                "result = RETAIL.CALCULATE(n=COUNT(selected_members))",
                "RETAIL",
                "retail_members_filter_name_endswith",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"N": [53]}),
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                order_sensitive=True,
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
                "selected_transactions = transactions.WHERE((YEAR(transaction_date) == 2022) & (MONTH(transaction_date) == 8))\n"
                "result = RETAIL.CALCULATE(n=ROUND(AVG(selected_transactions.total_amount), 2))",
                "RETAIL",
                "retail_transactions_filter",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"n": [277.13]}),
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
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame(
                        {
                            "account_type": ["HPlnssRN", "XADfRcm"],
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
                "result = RETAIL.CALCULATE(avg_secs=ROUND(AVG(DATEDIFF('seconds', account_info.first_transaction_sent, account_info.join_date)), 2))",
                "RETAIL",
                "retail_members_agg",
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"avg_secs": [32116724632.07]}),
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
                order_sensitive=True,
                answers={
                    "NONE": None,
                    "PARTIAL": None,
                    "FULL": pd.DataFrame({"NUM_CUSTOMERS_CHECKING_ACCOUNTS": [268]}),
                },
            ),
            id="fsi_customers_accounts_join",
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
