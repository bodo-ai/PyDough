"""
Test suite for validating SQL generation coverage of all PyDough functions
across supported dialects.

Each test function exercises a specific category of operations
(e.g., arithmetic, string, datetime, etc.) using representative TPC-H tables.

These tests are designed to ensure correct translation of high-level expressions
into dialect-specific SQL, and to detect any missing or incorrect
implementations in the SQL generation layer.

"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pandas as pd
import datetime


def arithmetic_and_binary_operators():
    """Test all arithmetic and binary PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return lines.CALCULATE(
        computed_value=(extended_price * (1 - (discount**2)) + 1.0) / part.retail_price,
        total=quantity + extended_price,
        delta=extended_price - quantity,
        product=quantity * discount,
        ratio=extended_price / quantity,
        exponent=discount**2,
    )


def comparisons_and_logical_operators():
    """Test all comparison and logical PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    is_asian = nation.region.name == "ASIA"
    is_european = nation.region.name == "EUROPE"
    in_debt = account_balance < 0

    return customers.CALCULATE(
        in_debt=in_debt,
        at_most_12_orders=COUNT(orders) <= 12,
        is_european=is_european,
        non_german=nation.name != "GERMANY",
        non_empty_acct=account_balance > 0,
        at_least_5_orders=COUNT(orders) >= 5,
        is_eurasian=is_asian | is_european,
        is_european_in_debt=is_european & in_debt,
    )


def unary_and_slicing_operators():
    """Test all unary and slicing PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return customers.CALCULATE(
        country_code=phone[:3],
        name_without_first_char=name[1:],
        last_digit=phone[-1:],
        name_without_start_and_end_char=name[1:-1],
        phone_without_last_5_chars=phone[:-5],
        name_second_to_last_char=name[-2:-1],
        is_not_in_debt=~(account_balance < 0),
    )


def string_functions():
    """Test all string PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return customers.CALCULATE(
        lowercase_name=LOWER(name),
        uppercase_name=UPPER(name),
        name_length=LENGTH(name),
        starts_with_A=STARTSWITH(name, "A"),
        ends_with_z=ENDSWITH(name, "z"),
        contains_sub=CONTAINS(name, "sub"),
        matches_like=LIKE(name, "%test%"),
        joined_string=JOIN_STRINGS("::", name, nation.name),
        lpad_name=LPAD(name, 20, "*"),
        rpad_name=RPAD(name, 20, "-"),
        stripped=STRIP(name),
        stripped_vowels=STRIP(name, "aeiou"),
        replaced_name=REPLACE(name, "Corp", "Inc"),
        removed_substr=REPLACE(name, "Ltd"),
        count_e=STRCOUNT(name, "e"),
        idx_Alex=FIND(name, "Alex"),
    )


def datetime_functions():
    """Test all date/time PyDough functions.
    Main purpose to verify these functions are working as expected with
        supported SQL dialects.
    """
    today = pd.Timestamp("1995-10-10")
    specific_dt = datetime.datetime(1992, 1, 1, 12, 30, 45)

    return orders.CALCULATE(
        ts_now_1=DATETIME("now"),
        ts_now_2=DATETIME("NOW", "start of day"),
        ts_now_3=DATETIME("  current_date ", "start of month"),
        ts_now_4=DATETIME("current timestamp", "+1 hours"),
        ts_now_5=DATETIME(datetime.datetime(2025, 1, 1), "start of month"),
        ts_now_6=DATETIME(today, "-2 days"),
        year_col=YEAR(order_date),
        year_py=YEAR(datetime.datetime(2020, 5, 1)),
        year_pd=YEAR(today),
        month_col=MONTH(order_date),
        month_str=MONTH("2025-02-25"),
        month_dt=MONTH(specific_dt),
        day_col=DAY(order_date),
        day_str=DAY("1996-11-25 10:45:00"),
        hour_str=HOUR("1995-12-01 23:59:59"),
        minute_str=MINUTE("1995-12-01 23:59:59"),
        second_ts=SECOND(pd.Timestamp("1992-01-01 00:00:59")),
        # DATEDIFF: mixing all types
        dd_col_str=DATEDIFF("days", order_date, "1992-01-01"),
        dd_str_col=DATEDIFF("days", "1992-01-01", order_date),
        dd_pd_col=DATEDIFF("months", today, order_date),
        dd_col_dt=DATEDIFF("years", order_date, specific_dt),
        dd_dt_str=DATEDIFF("weeks", "1992-01-01", specific_dt),
        # DAYOFWEEK / DAYNAME: all types
        dow_col=DAYOFWEEK(order_date),
        dow_str=DAYOFWEEK("1992-07-01"),
        dow_dt=DAYOFWEEK(specific_dt),
        dow_pd=DAYOFWEEK(today),
        dayname_col=DAYNAME(order_date),
        dayname_str=DAYNAME("1995-06-30"),
        dayname_dt=DAYNAME(datetime.datetime(1993, 8, 15)),
    )


def conditional_functions():
    """Test all conditional PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return customers.CALCULATE(
        iff_col=IFF(account_balance > 1000, "High", "Low"),
        isin_col=ISIN(name, ("Alice", "Bob", "Charlie")),
        default_val=DEFAULT_TO(MIN(orders.total_price), 0.0),
        has_acct_bal=PRESENT(MIN(orders.total_price)),
        no_acct_bal=ABSENT(MIN(orders.total_price)),
        no_debt_bal=KEEP_IF(account_balance, account_balance > 0),
    ).WHERE(MONOTONIC(100, account_balance, 1000))


def numerical_functions():
    """Test all numerical PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return customers.CALCULATE(
        abs_value=ABS(account_balance),
        round_value=ROUND(account_balance, 2),
        ceil_value=CEIL(account_balance),
        floor_value=FLOOR(account_balance),
        power_value=POWER(account_balance, 2),
        sqrt_value=SQRT(account_balance),
        sign_value=SIGN(account_balance),
        smallest_value=SMALLEST(account_balance, 0),
        largest_value=LARGEST(account_balance, 0),
    )


def aggregation_functions():
    """Test all aggregation PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return nations.CALCULATE(
        sum_value=SUM(customers.account_balance),
        avg_value=AVG(customers.account_balance),
        median_value=MEDIAN(customers.account_balance),
        min_value=MIN(customers.account_balance),
        max_value=MAX(customers.account_balance),
        quantile_value=QUANTILE(customers.account_balance, 0.8),
        anything_value=ANYTHING(customers.account_balance),
        count_value=COUNT(customers.account_balance),
        count_distinct_value=NDISTINCT(customers.account_balance),
        variance_value=VAR(customers.account_balance, type="sample"),
        stddev_value=STD(customers.account_balance, type="sample"),
    ).WHERE(HAS(customers) & HASNOT(customers.orders))


def window_functions():
    """Test all window PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return regions.nations.customers.CALCULATE(
        rank_value=RANKING(by=account_balance.DESC(), allow_ties=True, dense=True),
        precentile_value=PERCENTILE(by=account_balance.ASC(), n_buckets=10),
        two_prev_value=PREV(
            account_balance, n=2, by=account_balance.ASC(), per="regions", default=0.0
        ),
        two_next_value=NEXT(
            account_balance, n=2, by=account_balance.ASC(), per="nations"
        ),
        relsum_value=RELSUM(
            account_balance, by=account_balance.ASC(), frame=(0, None), per="regions"
        ),
        relsum_value2=RELSUM(account_balance, by=account_balance, cumulative=True),
        relavg_value=account_balance
        / RELAVG(account_balance, by=account_balance, frame=(-4, 0)),
        relcount_value=account_balance
        / RELCOUNT(KEEP_IF(account_balance, account_balance > 0.0)),
        relsize_value=account_balance / RELSIZE(),
    )


def casting_functions():
    """Test all casting PyDough functions.
    Main purpose to verify these functions are working as expected with
    supported SQL dialects.
    """
    return orders.CALCULATE(
        cast_to_string=STRING(order_date, "%Y-%m-%d"),
        cast_to_string2=STRING(total_price),
        cast_to_integer=INTEGER(total_price),
        cast_to_float=FLOAT(ship_priority),
    )
