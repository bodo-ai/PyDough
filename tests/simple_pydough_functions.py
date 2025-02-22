"""
Various functions containing PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pandas as pd
import datetime


def simple_scan():
    return Orders(key)


def simple_filter():
    # Note: The SQL is non-deterministic once we add nested expressions.
    return Orders(o_orderkey=key, o_totalprice=total_price).WHERE(o_totalprice < 1000.0)


def simple_scan_top_five():
    return Orders(key).TOP_K(5, by=key.ASC())


def simple_filter_top_five():
    return Orders(key, total_price).WHERE(total_price < 1000.0).TOP_K(5, by=key.DESC())


def rank_a():
    return Customers(rank=RANKING(by=acctbal.DESC()))


def rank_b():
    return Orders(rank=RANKING(by=(order_priority.ASC()), allow_ties=True))


def rank_c():
    return Orders(
        order_date, rank=RANKING(by=order_date.ASC(), allow_ties=True, dense=True)
    )


def rank_nations_by_region():
    return Nations(name, rank=RANKING(by=region.name.ASC(), allow_ties=True))


def rank_nations_per_region_by_customers():
    return Regions.nations(
        name, rank=RANKING(by=COUNT(customers).DESC(), levels=1)
    ).TOP_K(5, by=rank.ASC())


def rank_parts_per_supplier_region_by_size():
    return Regions.nations.suppliers.supply_records.part(
        key,
        region=BACK(4).name,
        rank=RANKING(
            by=(size.DESC(), container.DESC(), part_type.DESC()),
            levels=4,
            allow_ties=True,
            dense=True,
        ),
    ).TOP_K(15, by=key.ASC())


def rank_with_filters_a():
    return (
        Customers(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(ENDSWITH(name, "0"))
        .WHERE(r <= 30)
    )


def rank_with_filters_b():
    return (
        Customers(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(r <= 30)
        .WHERE(ENDSWITH(name, "0"))
    )


def rank_with_filters_c():
    return (
        PARTITION(Parts, name="p", by=size)
        .TOP_K(5, by=size.DESC())
        .p(size, name)
        .WHERE(RANKING(by=retail_price.DESC(), levels=1) == 1)
    )


def percentile_nations():
    # For every nation, give its name & its bucket from 1-5 ordered by name
    # alphabetically
    return Nations(name, p=PERCENTILE(by=name.ASC(), n_buckets=5))


def percentile_customers_per_region():
    # For each region, give the name of all customers in that region that are
    # in the 95th percentile in terms of account balance (larger percentile
    # means more money) and whose phone number ends in two zeros, sorted by the
    # name of the customers
    return (
        Regions.nations.customers(name)
        .WHERE((PERCENTILE(by=(acctbal.ASC()), levels=2) == 95) & ENDSWITH(phone, "00"))
        .ORDER_BY(name.ASC())
    )


def regional_suppliers_percentile():
    # For each region, find the suppliers in the top 0.1% by number of parts
    # they supply, breaking ties by name, only keeping the suppliers in the top
    pct = PERCENTILE(
        by=(COUNT(supply_records).ASC(), name.ASC()), levels=2, n_buckets=1000
    )
    return Regions.nations.suppliers(name).WHERE(HAS(supply_records) & (pct == 1000))


def function_sampler():
    # Examples of using different functions
    return (
        Regions.nations.customers(
            a=JOIN_STRINGS("-", BACK(2).name, BACK(1).name, name[16:]),
            b=ROUND(acctbal, 1),
            c=KEEP_IF(name, phone[:1] == "3"),
            d=PRESENT(KEEP_IF(name, phone[1:2] == "1")),
            e=ABSENT(KEEP_IF(name, phone[14:] == "7")),
        )
        .WHERE(MONOTONIC(0.0, acctbal, 100.0))
        .TOP_K(10, by=address.ASC())
    )


def datetime_current():
    return TPCH(
        d1=DATETIME("now", "start of year", "5 months", "-1 DAY"),
        d2=DATETIME("current_date", "start  of mm", "+24 hours"),
        d3=DATETIME(
            " Current Timestamp ", "start of day", "+12 hours", "-150 minutes", "+2 s"
        ),
    )


def datetime_relative():
    selected_orders = Orders.TOP_K(
        10, by=(customer_key.ASC(), order_date.ASC())
    ).ORDER_BY(order_date.ASC())
    return selected_orders(
        d1=DATETIME(order_date, "Start of Year"),
        d2=DATETIME(order_date, "START OF MONTHS"),
        d3=DATETIME(
            order_date,
            "-11 years",
            "+9 months",
            " - 7 DaYs ",
            "+5 h",
            "-3 minutes",
            "+1 second",
        ),
        d4=DATETIME(pd.Timestamp("2025-07-04 12:58:45"), "start of hour"),
        d5=DATETIME(pd.Timestamp("2025-07-04 12:58:45"), "start of minute"),
        d6=DATETIME(pd.Timestamp("2025-07-14 12:58:45"), "+ 1000000 seconds"),
    )


def datetime_sampler():
    # Near-exhaustive edge cases coverage testing for DATETIME strings. The
    # terms were generated via random combination selection of various ways
    # of augmenting the base/modifier terms.
    return Orders(
        DATETIME("2025-07-04 12:58:45"),
        DATETIME("2024-12-31 11:59:00"),
        DATETIME("2025-01-01"),
        DATETIME("1999-03-14"),
        DATETIME("now"),
        DATETIME(" Now "),
        DATETIME("NOW\n\t\t\r"),
        DATETIME("current_date"),
        DATETIME(" Current_Date "),
        DATETIME("CURRENT_DATE\n\t\t\r"),
        DATETIME("cUrReNt_Timestamp"),
        DATETIME(" Current_Timestamp "),
        DATETIME("CURRENT_TIMESTAMP\n\t\t\r"),
        DATETIME("current date"),
        DATETIME(" Current Date "),
        DATETIME("CURRENT DATE\n\t\t\r"),
        DATETIME("current timestamp"),
        DATETIME(" Current Timestamp "),
        DATETIME("CURRENT TIMESTAMP\n\t\t\r"),
        DATETIME(order_date),
        DATETIME("CURRENT_DATE\n\t\t\r", "start of seconds"),
        DATETIME("current date", "\n  Start  Of\tY\n\n", "+8 minutes", "-141 mm"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "\tSTART\tOF\tmonth\t",
            "\tSTART\tOF\tsecond\t",
            "\tSTART\tOF\thour\t",
        ),
        DATETIME(
            " Current Timestamp ",
            "start of h",
            "START of    SECOND",
            "START of    HOUR",
        ),
        DATETIME("NOW\n\t\t\r", "- 96 H", "15 year"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "\n  Start  Of\tY\n\n",
            "-3 years",
            "\n  Start  Of\tM\n\n",
            "+65 month",
        ),
        DATETIME(order_date, "-56 h", "start of year"),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            "-63 days",
            "START of    MINUTE",
            "start of seconds",
        ),
        DATETIME("CURRENT_DATE\n\t\t\r", "\n  Start  Of\tMonth\n\n"),
        DATETIME("NOW\n\t\t\r", "-312 hour", "start of s", " \n +\t48 \nYear \n\r "),
        DATETIME(
            "CURRENT TIMESTAMP\n\t\t\r",
            " 75 DAY",
            "\n  Start  Of\tDays\n\n",
            "+600 minutes",
            " \n -\t294 \nDays \n\r ",
        ),
        DATETIME(
            " Current Date ",
            "\n  Start  Of\tMonths\n\n",
            " \n +\t480 \nMm \n\r ",
            " \n -\t45 \nY \n\r ",
        ),
        DATETIME(
            " CuRrEnT dAtE ",
            "- 270 MINUTES",
            "- 34 SECONDS",
            "\tSTART\tOF\td\t",
            "start of second",
        ),
        DATETIME("current timestamp", "START of    MM", " \n \t213 \nS \n\r "),
        DATETIME(
            " Now ", "\n  Start  Of\tMonth\n\n", "13 minute", "28 year", "+344 second"
        ),
        DATETIME("CURRENT_DATE\n\t\t\r", "\tSTART\tOF\tdays\t"),
        DATETIME("2025-01-01", "START of    H", "+ 49 MINUTE", "+91 y"),
        DATETIME("CURRENT_DATE\n\t\t\r", "START of    YEARS", "\tSTART\tOF\td\t"),
        DATETIME("NOW\n\t\t\r", "start of days", "START of    YEARS"),
        DATETIME("2025-07-04 12:58:45", "\tSTART\tOF\tmonths\t", " \n \t22 \nM \n\r "),
        DATETIME("current_date", "START of    YEAR"),
        DATETIME(
            order_date,
            "+ 82 S",
            "415 second",
            " \n -\t160 \nSecond \n\r ",
            "START of    Y",
        ),
        DATETIME(" Current Date ", "192 months"),
        DATETIME(
            "CURRENT TIMESTAMP\n\t\t\r",
            "START of    H",
            "start of minute",
            "\n  Start  Of\tHours\n\n",
            "+ 486 M",
        ),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r", "\n  Start  Of\tSeconds\n\n", "- 50 HOURS"
        ),
        DATETIME(
            "CURRENT_TIMESTAMP\n\t\t\r",
            " 297 D",
            "72 months",
            " \n -\t92 \nMonth \n\r ",
            "\tSTART\tOF\thours\t",
        ),
        DATETIME("now", " \n +\t285 \nSeconds \n\r ", "\tSTART\tOF\tday\t"),
        DATETIME("1999-03-14", "+62 d"),
        DATETIME(
            "current_date",
            "START of    MM",
            "+1 hour",
            "start of mm",
            " \n -\t21 \nDay \n\r ",
        ),
        DATETIME("current timestamp", "+212 minute", " \n +\t368 \nYears \n\r "),
        DATETIME(
            "2024-12-31 11:59:00",
            "\n  Start  Of\tMonths\n\n",
            "\n  Start  Of\tYears\n\n",
            "\n  Start  Of\tMinutes\n\n",
            "start of m",
        ),
        DATETIME("1999-03-14", "START of    HOURS", "start of day"),
        DATETIME(
            "now",
            " \n -\t60 \nH \n\r ",
            "START of    D",
            "START of    MINUTE",
            "+196 years",
        ),
        DATETIME(
            "current timestamp",
            "-40 hours",
            " \n -\t385 \nDay \n\r ",
            "start of m",
            " \n +\t29 \nHour \n\r ",
        ),
        DATETIME(
            " Current Date ", "+405 days", "start of hour", "\tSTART\tOF\tminutes\t"
        ),
        DATETIME(
            " Current Timestamp ",
            "\tSTART\tOF\tyear\t",
            "\n  Start  Of\tS\n\n",
            " \n +\t98 \nY \n\r ",
            " \n \t96 \nMonth \n\r ",
        ),
        DATETIME(
            " Now ",
            "\tSTART\tOF\tminutes\t",
            "\tSTART\tOF\ts\t",
            "start of day",
            "78 seconds",
        ),
        DATETIME(
            " Current Date ",
            " 136 HOURS",
            " \n +\t104 \nM \n\r ",
            "-104 months",
            " \n \t312 \nD \n\r ",
        ),
        DATETIME(" Current_Date ", "+ 45 MM", "-135 s"),
    )


def loop_generated_terms():
    # Using a loop & dictionary to generate PyDough calc terms
    terms = {"name": name}
    for i in range(3):
        terms[f"interval_{i}"] = COUNT(
            customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
        )
    return Nations(**terms)


def function_defined_terms():
    # Using a regular function to generate PyDough calc terms
    def interval_n(n):
        return COUNT(customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000)))

    return Nations(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def function_defined_terms_with_duplicate_names():
    # Using a regular function to generate PyDough calc terms with the function argument same as collection's fields.
    def interval_n(n, name="test"):
        return COUNT(customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000)))

    return Nations(
        name,
        redefined_name=name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def lambda_defined_terms():
    # Using a lambda function to generate PyDough calc terms
    interval_n = lambda n: COUNT(
        customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000))
    )

    return Nations(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def dict_comp_terms():
    # Using a dictionary comprehension to generate PyDough calc terms
    terms = {"name": name}
    terms.update(
        {
            f"interval_{i}": COUNT(
                customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
            )
            for i in range(3)
        }
    )
    return Nations(**terms)


def list_comp_terms():
    # Using a list comprehension to generate PyDough calc terms
    terms = [name]
    terms.extend(
        [
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
            for i in range(3)
        ]
    )
    return Nations(*terms)


def set_comp_terms():
    # Using a set comprehension to generate PyDough calc terms
    terms = [name]
    terms.extend(
        set(
            {
                COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
                for i in range(3)
            }
        )
    )
    sorted_terms = sorted(terms, key=lambda x: repr(x))
    return Nations(*sorted_terms)


def generator_comp_terms():
    # Using a generator comprehension to generate PyDough calc terms
    terms = {"name": name}
    for term, value in (
        (
            f"interval_{i}",
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))),
        )
        for i in range(3)
    ):
        terms[term] = value
    return Nations(**terms)


def agg_partition():
    # Doing a global aggregation on the output of a partition aggregation
    yearly_data = PARTITION(Orders(year=YEAR(order_date)), name="orders", by=year)(
        n_orders=COUNT(orders)
    )
    return TPCH(best_year=MAX(yearly_data.n_orders))


def multi_partition_access_1():
    # A use of multiple PARTITION and stepping into partition children that is
    # a no-op.
    data = Tickers(symbol).TOP_K(5, by=symbol.ASC())
    grps_a = PARTITION(data, name="child_3", by=(currency, exchange, ticker_type))
    grps_b = PARTITION(grps_a, name="child_2", by=(currency, exchange))
    grps_c = PARTITION(grps_b, name="child_1", by=exchange)
    return grps_c.child_1.child_2.child_3


def multi_partition_access_2():
    # Identify transactions that are below the average number of shares for
    # transactions of the same combinations of (customer, stock, type), or
    # the same combination of (customer, stock), or the same customer.
    grps_a = PARTITION(
        Transactions, name="child_3", by=(customer_id, ticker_id, transaction_type)
    )(avg_shares_a=AVG(child_3.shares))
    grps_b = PARTITION(grps_a, name="child_2", by=(customer_id, ticker_id))(
        avg_shares_b=AVG(child_2.child_3.shares)
    )
    grps_c = PARTITION(grps_b, name="child_1", by=customer_id)(
        avg_shares_c=AVG(child_1.child_2.child_3.shares)
    )
    return grps_c.child_1.child_2.child_3.WHERE(
        (shares < BACK(1).avg_shares_a)
        & (shares < BACK(2).avg_shares_b)
        & (shares < BACK(3).avg_shares_c)
    )(
        transaction_id,
        customer.name,
        ticker.symbol,
        transaction_type,
        BACK(1).avg_shares_a,
        BACK(2).avg_shares_b,
        BACK(3).avg_shares_c,
    ).ORDER_BY(transaction_id.ASC())


def double_partition():
    # Doing a partition aggregation on the output of a partition aggregation
    year_month_data = PARTITION(
        Orders(year=YEAR(order_date), month=MONTH(order_date)),
        name="orders",
        by=(year, month),
    )(n_orders=COUNT(orders))
    return PARTITION(
        year_month_data,
        name="months",
        by=year,
    )(year, best_month=MAX(months.n_orders))


def triple_partition():
    # Doing three layers of partitioned aggregation. Goal of the question:
    # for each region, calculate the average percentage of purchases made from
    # suppliers in that region belonging to the most common part type shipped
    # from the supplier region to the customer region, averaging across all
    # customer region. Only considers lineitems from June of 1992 where the
    # container is small.
    line_info = (
        Parts.WHERE(
            STARTSWITH(container, "SM"),
        )
        .lines.WHERE((MONTH(ship_date) == 6) & (YEAR(ship_date) == 1992))(
            supp_region=supplier.nation.region.name,
        )
        .order.WHERE(YEAR(order_date) == 1992)(
            supp_region=BACK(1).supp_region,
            part_type=BACK(2).part_type,
            cust_region=customer.nation.region.name,
        )
    )
    rrt_combos = PARTITION(
        line_info, name="lines", by=(supp_region, cust_region, part_type)
    )(n_instances=COUNT(lines))
    rr_combos = PARTITION(rrt_combos, name="part_types", by=(supp_region, cust_region))(
        percentage=100.0 * MAX(part_types.n_instances) / SUM(part_types.n_instances)
    )
    return PARTITION(
        rr_combos,
        name="cust_regions",
        by=supp_region,
    )(supp_region, avg_percentage=AVG(cust_regions.percentage)).ORDER_BY(
        supp_region.ASC()
    )


def hour_minute_day():
    """
    Return the transaction IDs with the hour, minute, and second extracted from
    transaction timestamps for specific ticker symbols ("AAPL","GOOGL","NFLX"),
    ordered by transaction ID in ascending order.
    """
    return (
        Transactions(
            transaction_id, HOUR(date_time), MINUTE(date_time), SECOND(date_time)
        )
        .WHERE(ISIN(ticker.symbol, ("AAPL", "GOOGL", "NFLX")))
        .ORDER_BY(transaction_id.ASC())
    )


def exponentiation():
    return DailyPrices(
        low_square=low**2,
        low_sqrt=SQRT(low),
        low_cbrt=POWER(low, 1 / 3),
    ).TOP_K(10, by=low_square.ASC())


def args_kwargs():
    def impl(*args, **kwargs):
        terms = {}
        for i, color in enumerate(args):
            terms[f"n_{color}"] = COUNT(parts.WHERE(CONTAINS(part_name, color)))
        for n, size in kwargs.items():
            terms[n] = COUNT(parts.WHERE(size == size))
        return TPCH(**terms)

    result = impl("tomato", "almond", small=10, large=40)
    return result


def unpacking():
    start, end = (1992, 1994)
    selects_orders = orders.WHERE(MONOTONIC(start, YEAR(order_date), end))
    return selects_orders


def nested_unpacking():
    a, (b, c) = ["GERMANY", ["FRANCE", "ARGENTINA"]]
    chosen_customers = customers.WHERE(ISIN(nation.name, (a, b, c)))
    return chosen_customers


def unpacking_in_iterable():
    terms = {}
    for i, j in zip(range(5), range(1992, 1997)):
        terms[f"c{i}"] = COUNT(orders.WHERE(YEAR(order_date) == j))
    return Nations(**terms)


def with_import_statement():
    import tempfile as tf
    import logging.config

    logging.config.dictConfig({"version": 1})
    from os import path as opath

    result1 = opath.join("folder", "file.txt")
    with tf.TemporaryFile() as tf_handle1, tf.TemporaryFile() as tf_handle2:
        tf_handle1.write(b"Canada")
        tf_handle2.write(b"Mexico")
        tf_handle1.seek(0)
        tf_handle2.seek(0)
        a = str(tf_handle1.read().decode("utf-8"))
        b = str(tf_handle2.read().decode("utf-8"))
        return customers.WHERE(ISIN(nation.name, (a, b)))


def exception_handling():
    try:
        raise Exception("Canada")
    except Exception as e:
        country = str(e)
        return customers.WHERE(ISIN(nation.name, (country, "Mexico")))
    finally:
        pass


def class_handling():
    class Customer:
        def __init__(self, countries):
            self._countries = countries

        def query(self):
            return customers.WHERE(ISIN(nation.name, self._countries))

    return Customer(("Canada", "Mexico")).query()


def annotated_assignment():
    direction1: str
    direction1 = "SOUTH "
    specific_region: tuple[str, str] = "WEST", "AMERICA"
    chosen_region: str = direction1 + " ".join(specific_region)
    return Nations.WHERE(region.name == chosen_region)


def abs_round_magic_method():
    return DailyPrices(abs_low=abs(low), round_low=round(low, 2), round_zero=round(low))


def years_months_days_hours_datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    return Transactions.WHERE((YEAR(date_time) < 2025))(
        x=date_time,
        y1=y1_datetime,
        years_diff=DATEDIFF("years", date_time, y1_datetime),
        c_years_diff=DATEDIFF("YEARS", date_time, y1_datetime),
        c_y_diff=DATEDIFF("Y", date_time, y1_datetime),
        y_diff=DATEDIFF("y", date_time, y1_datetime),
        months_diff=DATEDIFF("months", date_time, y1_datetime),
        c_months_diff=DATEDIFF("MONTHS", date_time, y1_datetime),
        mm_diff=DATEDIFF("mm", date_time, y1_datetime),
        days_diff=DATEDIFF("days", date_time, y1_datetime),
        c_days_diff=DATEDIFF("DAYS", date_time, y1_datetime),
        c_d_diff=DATEDIFF("D", date_time, y1_datetime),
        d_diff=DATEDIFF("d", date_time, y1_datetime),
        hours_diff=DATEDIFF("hours", date_time, y1_datetime),
        c_hours_diff=DATEDIFF("HOURS", date_time, y1_datetime),
        c_h_diff=DATEDIFF("H", date_time, y1_datetime),
    ).TOP_K(30, by=years_diff.ASC())


def minutes_seconds_datediff():
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return Transactions.WHERE(YEAR(date_time) <= 2024)(
        x=date_time,
        y=y_datetime,
        minutes_diff=DATEDIFF("m", date_time, y_datetime),
        seconds_diff=DATEDIFF("s", date_time, y_datetime),
    ).TOP_K(30, by=x.DESC())


def datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return Transactions.WHERE((YEAR(date_time) < 2025))(
        x=date_time,
        y1=y1_datetime,
        y=y_datetime,
        years_diff=DATEDIFF("years", date_time, y1_datetime),
        months_diff=DATEDIFF("months", date_time, y1_datetime),
        days_diff=DATEDIFF("days", date_time, y1_datetime),
        hours_diff=DATEDIFF("hours", date_time, y1_datetime),
        minutes_diff=DATEDIFF("minutes", date_time, y_datetime),
        seconds_diff=DATEDIFF("seconds", date_time, y_datetime),
    ).TOP_K(30, by=years_diff.ASC())
