"""
Various functions containing PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pandas as pd
import datetime


def simple_scan():
    return Orders.CALCULATE(key)


def simple_filter():
    return Orders.CALCULATE(o_orderkey=key, o_totalprice=total_price).WHERE(
        o_totalprice < 1000.0
    )


def simple_scan_top_five():
    return Orders.CALCULATE(key).TOP_K(5, by=key.ASC())


def simple_filter_top_five():
    return (
        Orders.CALCULATE(key, total_price)
        .WHERE(total_price < 1000.0)
        .TOP_K(5, by=key.DESC())
    )


def order_info_per_priority():
    # Find information about the highest total price order for each priority
    # type in 1992. Specifically, for each order priority, the key & total
    # price of the order. Order the results by priority.
    priorities = PARTITION(
        Orders.WHERE(YEAR(order_date) == 1992), name="orders", by=order_priority
    )
    return (
        priorities.orders.WHERE(RANKING(by=total_price.DESC(), levels=1) == 1)
        .CALCULATE(order_priority, order_key=key, order_total_price=total_price)
        .ORDER_BY(order_priority.ASC())
    )


def simple_collation():
    return (
        Suppliers.CALCULATE(
            p=PERCENTILE(
                by=(
                    COUNT(supply_records).ASC(),
                    name,
                    address,
                    nation_key,
                    phone,
                    account_balance.DESC(),
                    comment,
                )
            ),
            r=RANKING(
                by=(
                    key,
                    COUNT(supply_records),
                    name.DESC(),
                    address,
                    nation_key,
                    phone,
                    account_balance.ASC(),
                    comment,
                )
            ),
        )
        .ORDER_BY(
            COUNT(supply_records).ASC(),
            name,
            address,
            nation_key,
            phone,
            account_balance.DESC(),
            comment,
        )
        .TOP_K(
            5,
            by=(
                key,
                COUNT(supply_records),
                name.DESC(),
                address,
                nation_key,
                phone,
                account_balance.ASC(),
                comment,
            ),
        )
    )


def year_month_nation_orders():
    # Finds the 5 largest instances of numbers of orders made in a month of a
    # year by customers in a nation, only looking at nations from Asia and
    # Africa.
    selected_regions = Regions.WHERE(ISIN(name, ("ASIA", "AFRICA")))
    urgent_orders = (
        selected_regions.nations.CALCULATE(nation_name=name)
        .customers.orders.WHERE(order_priority == "1-URGENT")
        .CALCULATE(
            nation_name,
            order_year=YEAR(order_date),
            order_month=MONTH(order_date),
        )
    )
    groups = PARTITION(
        urgent_orders, name="u", by=(nation_name, order_year, order_month)
    )
    return groups.CALCULATE(
        nation_name, order_year, order_month, n_orders=COUNT(u)
    ).TOP_K(5, by=n_orders.DESC())


def rank_a():
    return Customers.CALCULATE(rank=RANKING(by=acctbal.DESC()))


def rank_b():
    return Orders.CALCULATE(rank=RANKING(by=(order_priority.ASC()), allow_ties=True))


def rank_c():
    return Orders.CALCULATE(
        order_date, rank=RANKING(by=order_date.ASC(), allow_ties=True, dense=True)
    )


def rank_nations_by_region():
    return Nations.CALCULATE(name, rank=RANKING(by=region.name.ASC(), allow_ties=True))


def rank_nations_per_region_by_customers():
    return Regions.nations.CALCULATE(
        name, rank=RANKING(by=COUNT(customers).DESC(), levels=1)
    ).TOP_K(5, by=rank.ASC())


def rank_parts_per_supplier_region_by_size():
    return (
        Regions.CALCULATE(region_name=name)
        .nations.suppliers.supply_records.part.CALCULATE(
            key,
            region=region_name,
            rank=RANKING(
                by=(size.DESC(), container.DESC(), part_type.DESC()),
                levels=4,
                allow_ties=True,
                dense=True,
            ),
        )
        .TOP_K(15, by=key.ASC())
    )


def rank_with_filters_a():
    return (
        Customers.CALCULATE(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(ENDSWITH(name, "0"))
        .WHERE(r <= 30)
    )


def rank_with_filters_b():
    return (
        Customers.CALCULATE(n=name, r=RANKING(by=acctbal.DESC()))
        .WHERE(r <= 30)
        .WHERE(ENDSWITH(name, "0"))
    )


def rank_with_filters_c():
    return (
        PARTITION(Parts, name="p", by=size)
        .TOP_K(5, by=size.DESC())
        .p.CALCULATE(size, name)
        .WHERE(RANKING(by=retail_price.DESC(), levels=1) == 1)
    )


def percentile_nations():
    # For every nation, give its name & its bucket from 1-5 ordered by name
    # alphabetically
    return Nations.CALCULATE(name, p=PERCENTILE(by=name.ASC(), n_buckets=5))


def percentile_customers_per_region():
    # For each region, give the name of all customers in that region that are
    # in the 95th percentile in terms of account balance (larger percentile
    # means more money) and whose phone number ends in two zeros, sorted by the
    # name of the customers
    return (
        Regions.nations.customers.CALCULATE(name)
        .WHERE((PERCENTILE(by=(acctbal.ASC()), levels=2) == 95) & ENDSWITH(phone, "00"))
        .ORDER_BY(name.ASC())
    )


def regional_suppliers_percentile():
    # For each region, find the suppliers in the top 0.1% by number of parts
    # they supply, breaking ties by name, only keeping the suppliers in the top
    pct = PERCENTILE(
        by=(COUNT(supply_records).ASC(), name.ASC()), levels=2, n_buckets=1000
    )
    return Regions.nations.suppliers.CALCULATE(name).WHERE(
        HAS(supply_records) & (pct == 1000)
    )


def function_sampler():
    # Functions tested:
    # JOIN_STRINGS,
    # ROUND (with and without precision),
    # KEEP_IF,
    # PRESENT,
    # ABSENT,
    # MONOTONIC
    return (
        Regions.CALCULATE(region_name=name)
        .nations.CALCULATE(nation_name=name)
        .customers.CALCULATE(
            a=JOIN_STRINGS("-", region_name, nation_name, name[16:]),
            b=ROUND(acctbal, 1),
            c=KEEP_IF(name, phone[:1] == "3"),
            d=PRESENT(KEEP_IF(name, phone[1:2] == "1")),
            e=ABSENT(KEEP_IF(name, phone[14:] == "7")),
            f=ROUND(acctbal),
        )
        .WHERE(MONOTONIC(0.0, acctbal, 100.0))
        .TOP_K(10, by=address.ASC())
    )


def datetime_current():
    return TPCH.CALCULATE(
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
    return selected_orders.CALCULATE(
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
    return Orders.CALCULATE(
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
    # Using a loop & dictionary to generate PyDough calculate terms
    terms = {"name": name}
    for i in range(3):
        terms[f"interval_{i}"] = COUNT(
            customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
        )
    return Nations.CALCULATE(**terms)


def function_defined_terms():
    # Using a regular function to generate PyDough calculate terms
    def interval_n(n):
        return COUNT(customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000)))

    return Nations.CALCULATE(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def function_defined_terms_with_duplicate_names():
    # Using a regular function to generate PyDough calculate terms with the
    # function argument same as collection's fields.
    def interval_n(n, name="test"):
        return COUNT(customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000)))

    return Nations.CALCULATE(
        name,
        redefined_name=name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def lambda_defined_terms():
    # Using a lambda function to generate PyDough calculate terms
    interval_n = lambda n: COUNT(
        customers.WHERE(MONOTONIC(n * 1000, acctbal, (n + 1) * 1000))
    )

    return Nations.CALCULATE(
        name,
        interval_7=interval_n(7),
        interval_4=interval_n(4),
        interval_13=interval_n(13),
    )


def dict_comp_terms():
    # Using a dictionary comprehension to generate PyDough calculate terms
    terms = {"name": name}
    terms.update(
        {
            f"interval_{i}": COUNT(
                customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))
            )
            for i in range(3)
        }
    )
    return Nations.CALCULATE(**terms)


def list_comp_terms():
    # Using a list comprehension to generate PyDough calculate terms
    terms = [name]
    terms.extend(
        [
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000)))
            for i in range(3)
        ]
    )
    return Nations.CALCULATE(*terms)


def set_comp_terms():
    # Using a set comprehension to generate PyDough calculate terms
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
    return Nations.CALCULATE(*sorted_terms)


def generator_comp_terms():
    # Using a generator comprehension to generate PyDough calculate terms
    terms = {"name": name}
    for term, value in (
        (
            f"interval_{i}",
            COUNT(customers.WHERE(MONOTONIC(i * 1000, acctbal, (i + 1) * 1000))),
        )
        for i in range(3)
    ):
        terms[term] = value
    return Nations.CALCULATE(**terms)


def partition_as_child():
    # Count how many part sizes have an above-average number of parts of that
    # size.
    sizes = PARTITION(Parts, name="p", by=size).CALCULATE(n_parts=COUNT(p))
    return TPCH.CALCULATE(avg_n_parts=AVG(sizes.n_parts)).CALCULATE(
        n_parts=COUNT(sizes.WHERE(n_parts > avg_n_parts))
    )


def agg_partition():
    # Doing a global aggregation on the output of a partition aggregation
    yearly_data = PARTITION(
        Orders.CALCULATE(year=YEAR(order_date)), name="orders", by=year
    ).CALCULATE(n_orders=COUNT(orders))
    return TPCH.CALCULATE(best_year=MAX(yearly_data.n_orders))


def multi_partition_access_1():
    # A use of multiple PARTITION and stepping into partition children that is
    # a no-op.
    data = Tickers.CALCULATE(symbol).TOP_K(5, by=symbol.ASC())
    grps_a = PARTITION(data, name="child_3", by=(currency, exchange, ticker_type))
    grps_b = PARTITION(grps_a, name="child_2", by=(currency, exchange))
    grps_c = PARTITION(grps_b, name="child_1", by=exchange)
    return grps_c.child_1.child_2.child_3


def multi_partition_access_2():
    # Identify transactions that are below the average number of shares for
    # transactions of the same combinations of (customer, stock, type), or
    # the same combination of (customer, stock), or the same customer.
    cust_tick_typ_groups = PARTITION(
        Transactions,
        name="original_data",
        by=(customer_id, ticker_id, transaction_type),
    ).CALCULATE(cus_tick_typ_avg_shares=AVG(original_data.shares))
    cust_tick_groups = PARTITION(
        cust_tick_typ_groups, name="typs", by=(customer_id, ticker_id)
    ).CALCULATE(cust_tick_avg_shares=AVG(typs.original_data.shares))
    cus_groups = PARTITION(cust_tick_groups, name="ticks", by=customer_id).CALCULATE(
        cust_avg_shares=AVG(ticks.typs.original_data.shares)
    )
    return (
        cus_groups.ticks.typs.original_data.WHERE(
            (shares < cus_tick_typ_avg_shares)
            & (shares < cust_tick_avg_shares)
            & (shares < cust_avg_shares)
        )
        .CALCULATE(
            transaction_id,
            customer.name,
            ticker.symbol,
            transaction_type,
            cus_tick_typ_avg_shares,
            cust_tick_avg_shares,
            cust_avg_shares,
        )
        .ORDER_BY(transaction_id.ASC())
    )


def multi_partition_access_3():
    # Find all daily price updates whose closing price was the high mark for
    # that ticker, but not for tickers of that type.
    data = Tickers.CALCULATE(symbol, ticker_type).historical_prices
    ticker_groups = PARTITION(data, name="ticker_data", by=ticker_id).CALCULATE(
        ticker_high_price=MAX(ticker_data.close)
    )
    type_groups = PARTITION(
        ticker_groups.ticker_data, name="type_data", by=ticker_type
    ).CALCULATE(type_high_price=MAX(type_data.close))
    return (
        type_groups.type_data.WHERE(
            (close == ticker_high_price) & (close < type_high_price)
        )
        .CALCULATE(symbol, close)
        .ORDER_BY(symbol.ASC())
    )


def multi_partition_access_4():
    # Find all transacitons that were the largest for a customer of that ticker
    # (by number of shares) but not the largest for that customer overall.
    cust_ticker_groups = PARTITION(
        Transactions, name="data", by=(customer_id, ticker_id)
    ).CALCULATE(cust_ticker_max_shares=MAX(data.shares))
    cust_groups = PARTITION(
        cust_ticker_groups, name="ticker_groups", by=customer_id
    ).CALCULATE(cust_max_shares=MAX(ticker_groups.cust_ticker_max_shares))
    return (
        cust_groups.ticker_groups.data.WHERE(
            (shares >= cust_ticker_max_shares) & (shares < cust_max_shares)
        )
        .CALCULATE(transaction_id)
        .ORDER_BY(transaction_id.ASC())
    )


def multi_partition_access_5():
    # Find all transactions where more than 80% of all transactions of that
    # that ticker were of that type, but less than 20% of all transactions of
    # that type were from that ticker. List the transaction ID, the number of
    # transactions of that ticker/type, ticker, and type. Sort by the number of
    # transactions of that ticker/type, breaking ties by trnasaction ID.
    ticker_type_groups = PARTITION(
        Transactions, name="data", by=(ticker_id, transaction_type)
    ).CALCULATE(n_ticker_type_trans=COUNT(data))
    ticker_groups = PARTITION(
        ticker_type_groups, name="sub_trans", by=ticker_id
    ).CALCULATE(n_ticker_trans=SUM(sub_trans.n_ticker_type_trans))
    type_groups = PARTITION(
        ticker_groups.sub_trans, name="sub_trans", by=transaction_type
    ).CALCULATE(n_type_trans=SUM(sub_trans.n_ticker_type_trans))
    return (
        type_groups.sub_trans.data.CALCULATE(
            transaction_id,
            n_ticker_type_trans,
            n_ticker_trans,
            n_type_trans,
        )
        .WHERE(
            ((n_ticker_type_trans / n_ticker_trans) > 0.8)
            & ((n_ticker_type_trans / n_type_trans) < 0.2)
        )
        .ORDER_BY(n_ticker_type_trans.ASC(), transaction_id.ASC())
    )


def multi_partition_access_6():
    # Find all transactions that are the only transaction of that type for
    # that ticker, or the only transaction of that type for that customer,
    # but not the only transaction for that customer, type, or ticker. List
    # the transaction IDs in ascending order.
    ticker_type_groups = PARTITION(
        Transactions, name="data", by=(ticker_id, transaction_type)
    ).CALCULATE(n_ticker_type_trans=COUNT(data))
    ticker_groups = PARTITION(
        ticker_type_groups, name="sub_trans", by=ticker_id
    ).CALCULATE(n_ticker_trans=SUM(sub_trans.n_ticker_type_trans))
    type_groups = PARTITION(
        ticker_groups.sub_trans, name="sub_trans", by=transaction_type
    ).CALCULATE(n_type_trans=SUM(sub_trans.n_ticker_type_trans))
    cust_type_groups = PARTITION(
        type_groups.sub_trans.data, name="data", by=(customer_id, transaction_type)
    ).CALCULATE(n_cust_type_trans=COUNT(data))
    cust_groups = PARTITION(
        cust_type_groups, name="sub_trans", by=customer_id
    ).CALCULATE(n_cust_trans=SUM(sub_trans.n_cust_type_trans))
    return (
        cust_groups.sub_trans.data.CALCULATE(transaction_id)
        .WHERE(
            ((n_ticker_type_trans == 1) | (n_cust_type_trans == 1))
            & (n_cust_trans > 1)
            & (n_type_trans > 1)
            & (n_ticker_trans > 1)
        )
        .ORDER_BY(transaction_id.ASC())
    )


def double_partition():
    # Doing a partition aggregation on the output of a partition aggregation
    year_month_data = PARTITION(
        Orders.CALCULATE(year=YEAR(order_date), month=MONTH(order_date)),
        name="orders",
        by=(year, month),
    ).CALCULATE(n_orders=COUNT(orders))
    return PARTITION(
        year_month_data,
        name="months",
        by=year,
    ).CALCULATE(year, best_month=MAX(months.n_orders))


def triple_partition():
    # Doing three layers of partitioned aggregation. Goal of the question:
    # for each region, calculate the average percentage of purchases made from
    # suppliers in that region belonging to the most common part type shipped
    # from the supplier region to the customer region, averaging across all
    # customer region. Only considers lineitems from June of 1992 where the
    # container is small.
    line_info = (
        Parts.CALCULATE(part_type)
        .WHERE(STARTSWITH(container, "SM"))
        .lines.WHERE((MONTH(ship_date) == 6) & (YEAR(ship_date) == 1992))
        .CALCULATE(supp_region=supplier.nation.region.name)
        .order.WHERE(YEAR(order_date) == 1992)
        .CALCULATE(cust_region=customer.nation.region.name)
    )
    rrt_combos = PARTITION(
        line_info, name="lines", by=(supp_region, cust_region, part_type)
    ).CALCULATE(n_instances=COUNT(lines))
    rr_combos = PARTITION(
        rrt_combos, name="part_types", by=(supp_region, cust_region)
    ).CALCULATE(
        percentage=100.0 * MAX(part_types.n_instances) / SUM(part_types.n_instances)
    )
    return (
        PARTITION(
            rr_combos,
            name="cust_regions",
            by=supp_region,
        )
        .CALCULATE(supp_region, avg_percentage=AVG(cust_regions.percentage))
        .ORDER_BY(supp_region.ASC())
    )


def hour_minute_day():
    """
    Return the transaction IDs with the hour, minute, and second extracted from
    transaction timestamps for specific ticker symbols ("AAPL","GOOGL","NFLX"),
    ordered by transaction ID in ascending order.
    """
    return (
        Transactions.CALCULATE(
            transaction_id, HOUR(date_time), MINUTE(date_time), SECOND(date_time)
        )
        .WHERE(ISIN(ticker.symbol, ("AAPL", "GOOGL", "NFLX")))
        .ORDER_BY(transaction_id.ASC())
    )


def exponentiation():
    return DailyPrices.CALCULATE(
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
        return TPCH.CALCULATE(**terms)

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
    return Nations.CALCULATE(**terms)


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
    return DailyPrices.CALCULATE(
        abs_low=abs(low), round_low=round(low, 2), round_zero=round(low)
    )


def years_months_days_hours_datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    return (
        Transactions.WHERE((YEAR(date_time) < 2025))
        .CALCULATE(
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
        )
        .TOP_K(30, by=years_diff.ASC())
    )


def minutes_seconds_datediff():
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return (
        Transactions.WHERE(YEAR(date_time) <= 2024)
        .CALCULATE(
            x=date_time,
            y=y_datetime,
            minutes_diff=DATEDIFF("m", date_time, y_datetime),
            seconds_diff=DATEDIFF("s", date_time, y_datetime),
        )
        .TOP_K(30, by=x.DESC())
    )


def datediff():
    y1_datetime = datetime.datetime(2025, 5, 2, 11, 00, 0)
    y_datetime = datetime.datetime(2023, 4, 3, 13, 16, 30)
    return (
        Transactions.WHERE((YEAR(date_time) < 2025))
        .CALCULATE(
            x=date_time,
            y1=y1_datetime,
            y=y_datetime,
            years_diff=DATEDIFF("years", date_time, y1_datetime),
            months_diff=DATEDIFF("months", date_time, y1_datetime),
            days_diff=DATEDIFF("days", date_time, y1_datetime),
            hours_diff=DATEDIFF("hours", date_time, y1_datetime),
            minutes_diff=DATEDIFF("minutes", date_time, y_datetime),
            seconds_diff=DATEDIFF("seconds", date_time, y_datetime),
        )
        .TOP_K(30, by=years_diff.ASC())
    )


def padding_functions():
    return Customers.CALCULATE(
        original_name=name,
        ref_rpad=RPAD("Cust0001", 30, "*"),
        ref_lpad=LPAD("Cust0001", 30, "*"),
        right_padded=RPAD(name, 30, "*"),
        left_padded=LPAD(name, 30, "#"),
        truncated_right=RPAD(name, 8, "-"),
        truncated_left=LPAD(name, 8, "-"),
        zero_pad_right=RPAD(name, 0, "."),
        zero_pad_left=LPAD(name, 0, "."),
        right_padded_space=RPAD(name, 30, " "),
        left_padded_space=LPAD(name, 30, " "),
    ).TOP_K(5, by=name.ASC())


def step_slicing():
    return Customers.CALCULATE(
        name,
        neg_none_step=name[-2::1],
        pos_none_step=name[3::1],
        none_pos_step=name[:3:1],
        none_neg_step=name[:-2:1],
        pos_pos_step=name[2:4:1],
        pos_neg_step=name[2:-2:1],
        neg_pos_step=name[-12:2:1],
        neg_neg_step=name[-4:-2:1],
        inbetween_chars=name[1:-1:1],
        empty1=name[2:2:1],
        empty2=name[-2:-2:1],
        empty3=name[-2:-4:1],
        empty4=name[4:2:1],
        oob1=name[100:200:1],
        oob2=name[-200:-100:1],
        oob3=name[100::1],
        oob4=name[-200::1],
        oob5=name[:100:1],
        oob6=name[:-200:1],
        oob7=name[100:-200:1],
        oob8=name[-200:100:1],
        oob9=name[100:-1:1],
        oob10=name[-100:-1:1],
        oob11=name[-3:100:1],
        oob12=name[-3:-100:1],
        zero1=name[0:0:1],
        zero2=name[0:1:1],
        zero3=name[-1:0:1],
        zero4=name[1:0:1],
        zero5=name[0:-1:1],
        zero6=name[0:-20:1],
        zero7=name[0:100:1],
        zero8=name[20:0:1],
        zero9=name[-20:0:1],
        wo_step1=name[-2:],
        wo_step2=name[3:],
        wo_step3=name[:3],
        wo_step4=name[:-2],
        wo_step5=name[2:4],
        wo_step6=name[2:-2],
        wo_step7=name[-4:2],
        wo_step8=name[-4:-2],
        wo_step9=name[2:2],
    )


def sign():
    return (
        DailyPrices.CALCULATE(
            high,
            high_neg=-1 * high,
            high_zero=0 * high,
        )
        .TOP_K(5, by=high.ASC())
        .CALCULATE(
            high,
            high_neg,
            high_zero,
            sign_high=SIGN(high),
            sign_high_neg=SIGN(high_neg),
            sign_high_zero=SIGN(high_zero),
        )
    )


def find():
    return Customers.WHERE(name == "Alex Rodriguez").CALCULATE(
        name,
        idx_Alex=FIND(name, "Alex"),
        idx_Rodriguez=FIND(name, "Rodriguez"),
        idx_bob=FIND(name, "bob"),
        idx_e=FIND(name, "e"),
        idx_space=FIND(name, " "),
        idx_of_R=FIND(name, "R"),
        idx_of_Alex_Rodriguez=FIND(name, "Alex Rodriguez"),
    )


def strip():
    return (
        Customers.WHERE(name == "Alex Rodriguez")
        .CALCULATE(
            name,
            alt_name1="  Alex Rodriguez  ",
            alt_name2="aeiAlex Rodriguezaeiou",
            alt_name3=";;Alex Rodriguez;;",
            alt_name4="""
    Alex Rodriguez
        """,  # equivalent to "\n\tAlex Rodriguez\n"
        )
        .CALCULATE(
            stripped_name=STRIP(name, "Alex Rodriguez"),
            stripped_name1=STRIP(name),
            stripped_name_with_chars=STRIP(name, "lAez"),
            stripped_alt_name1=STRIP(alt_name1),
            stripped_alt_name2=STRIP(alt_name2, "aeiou"),
            stripped_alt_name3=STRIP(alt_name3, ";"),
            stripped_alt_name4=STRIP(alt_name4),
        )
    )
