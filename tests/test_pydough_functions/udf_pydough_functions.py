"""
PyDough function definitions using user defined functions from the graphs.
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def sqlite_udf_format_datetime():
    # For each of the 5 orders with the highest total price, list the following:
    # 1. The key
    # 2. The order date formatted as "dd/mm/yyyy"
    # 3. The order date formatted as "yyyy:ddd" (where ddd is the day of the year)
    # 4. The unix timestamp (in seconds) of the order date
    return orders.CALCULATE(
        key,
        d1=FORMAT_DATETIME("%d/%m/%Y", order_date),
        d2=FORMAT_DATETIME("%Y:%j", order_date),
        d3=INTEGER(FORMAT_DATETIME("%s", order_date)),
    ).TOP_K(5, by=total_price.ASC())


def sqlite_udf_combine_strings():
    # Compute the following quantities:
    # 1. The names of all regions, concatenated with ",".
    # 2. The names of all regions (with EUROPE replaced with NULL),
    #    concatenated with ", ".
    # 3. The first letter of the names of all nations, concatenated with "".
    # 4. The first character of each unique order priority value from orders in
    #    1992, concatenated with " <=> ".
    r = regions.CALCULATE(n=KEEP_IF(name, name != "EUROPE"))
    o = orders.WHERE(YEAR(order_date) == 1992).PARTITION(
        name="priorities", by=order_priority
    )
    return TPCH_SQLITE_UDFS.CALCULATE(
        s1=COMBINE_STRINGS(r.name),
        s2=COMBINE_STRINGS(r.n, ", "),
        s3=COMBINE_STRINGS(nations.name[:1], ""),
        s4=COMBINE_STRINGS(o.order_priority[2:], " <=> "),
    )


def sqlite_udf_percent_positive():
    # For each region, calculate the percentage of customers & percentage of
    # suppliers in that region with a positive account balance, rounded to 2
    # decimal places.
    return regions.CALCULATE(
        name,
        pct_cust_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.customers.account_balance)), 2
        ),
        pct_supp_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.suppliers.account_balance)), 2
        ),
    ).ORDER_BY(name.ASC())


def sqlite_udf_count_epsilon():
    # For each region, count how many customers are within 10% of the average
    # account balance of all customers in that region.
    selected_customers = nations.customers.CALCULATE(
        avg_balance=RELAVG(account_balance, per="regions")
    ).WHERE(EPSILON(account_balance, avg_balance, avg_balance * 0.1))
    return (
        regions.CALCULATE(name, n_cust=COUNT(selected_customers))
        .WHERE(HAS(selected_customers))
        .ORDER_BY(name.ASC())
    )


def sqlite_udf_percent_epsilon():
    # For epsilon values of each power of 10 from 1 to 10,000, calculate the
    # percentage of orders in 1992 that are within epsilon of the average of
    # all such orders' total prices, rounded to 4 decimal places.
    order_info = orders.WHERE(YEAR(order_date) == 1992).CALCULATE(
        global_avg=RELAVG(total_price)
    )
    return TPCH_SQLITE_UDFS.CALCULATE(
        pct_e1=ROUND(
            PERCENTAGE(EPSILON(order_info.total_price, order_info.global_avg, 1)), 4
        ),
        pct_e10=ROUND(
            PERCENTAGE(EPSILON(order_info.total_price, order_info.global_avg, 10)), 4
        ),
        pct_e100=ROUND(
            PERCENTAGE(EPSILON(order_info.total_price, order_info.global_avg, 100)), 4
        ),
        pct_e1000=ROUND(
            PERCENTAGE(EPSILON(order_info.total_price, order_info.global_avg, 1000)), 4
        ),
        pct_e10000=ROUND(
            PERCENTAGE(EPSILON(order_info.total_price, order_info.global_avg, 10000)), 4
        ),
    )


def sqlite_udf_covar_pop():
    # For each region, calculate the population covariance between the
    # account balance of customers in that region and the total price of
    # orders made by those customers (divided by 1 million). Only consider
    # customers in the BUILDING market segment, and only consider orders
    # from 1998 with a priority of "2-HIGH".
    order_info = (
        nations.customers.CALCULATE(account_balance)
        .WHERE(market_segment == "BUILDING")
        .orders.WHERE((YEAR(order_date) == 1998) & (order_priority == "2-HIGH"))
    )
    return regions.CALCULATE(
        region_name=name,
        cvp_ab_otp=ROUND(
            POPULATION_COVARIANCE(
                order_info.account_balance, order_info.total_price / 1_000_000.0
            ),
            3,
        ),
    ).ORDER_BY(region_name)


def sqlite_udf_nval():
    # For each nation, list the following:
    # 1. The region name.
    # 2. The nation name.
    # 3. The 3rd nation name alphabetically, overall.
    # 4. The 1st nation name alphabetically, per region.
    # 5. The 1st nation name alphabetically after the current one, per region.
    # 6. The 5th nation name alphabetically, only considering nations up to and
    #    including the current one..
    return (
        regions.CALCULATE(rname=name)
        .nations.CALCULATE(
            rname,
            nname=name,
            v1=NVAL(name, 3, by=name),
            v2=NVAL(name, 1, by=name, per="regions"),
            v3=NVAL(name, 2, by=name, per="regions", frame=(1, None)),
            v4=NVAL(name, 5, by=name, cumulative=True),
        )
        .ORDER_BY(rname.ASC(), nname.ASC())
    )


def sqlite_udf_gcat():
    # For each region, calculate the following:
    # 1. The region name.
    # 2. The concatenation of all region names alphabetically, with a
    #    delimiter of "-".
    # 3. The concatenation of all region names reverse alphabetically, with a
    #    delimiter of "-".
    # 4. The concatenation of all region names alphabetically, with a
    #    delimiter of "-", only including region names up to and including the
    #    current one.
    return regions.CALCULATE(
        name,
        c1=GCAT(name, "-", by=name.ASC()),
        c2=GCAT(name, "-", by=name.DESC()),
        c3=GCAT(name, "-", by=name.ASC(), cumulative=True),
    ).ORDER_BY(name.ASC())


def sqlite_udf_relmin():
    # Break down the urgent orders made in 1994 by month. For each month,
    # calculate the following:
    # 1. The month.
    # 2. The number of orders made in that month.
    # 3. The minimum number of orders made in all the months.
    # 4. The minimum number of orders made in the months up to and including
    #    the current month, cumulatively.
    # 5. The minimum number of orders made between the current month, the previous
    #    month, and the next month.
    return (
        orders.WHERE((YEAR(order_date) == 1994) & (order_priority == "1-URGENT"))
        .CALCULATE(month=MONTH(order_date))
        .PARTITION(name="months", by=month)
        .CALCULATE(
            month,
            n_orders=COUNT(orders),
            m1=RELMIN(COUNT(orders)),
            m2=RELMIN(COUNT(orders), by=month, cumulative=True),
            m3=RELMIN(COUNT(orders), by=month, frame=(-1, 1)),
        )
        .ORDER_BY(month.ASC())
    )


def sqlite_udf_cumulative_distribution():
    # For all orders made in 1998, or orders made in 1992 with a priority of
    # "2-HIGH", identify the cumulative distribution of the order priority
    # values within each market segment. Then, for each cumulative distribution
    # value, count how many orders fall into that distribution value.
    return (
        orders.WHERE(
            (YEAR(order_date) == 1998)
            | ((order_priority == "2-HIGH") & (YEAR(order_date) == 1992))
        )
        .CALCULATE(segment=customer.market_segment)
        .PARTITION(name="segments", by=segment)
        .orders.CALCULATE(c=ROUND(CUMULATIVE_DISTRIBUTION(by=order_priority.ASC()), 4))
        .PARTITION(name="groups", by=c)
        .CALCULATE(c, n=COUNT(orders))
        .ORDER_BY(c)
    )


def sqlite_udf_decode3():
    # For the orders made by the clerk "Clerk#000000951", calculate the
    # key and a value that is derived from the first character of the
    # order priority. The value should be:
    # 1. "A" if the first character is "1"
    # 2. "B" if the first character is "2"
    # 3. "C" if the first character is "3"
    # 4. "D" otherwise
    return (
        orders.WHERE(clerk == "Clerk#000000951")
        .CALCULATE(
            key,
            val=DECODE3(INTEGER(order_priority[:1]), 1, "A", 2, "B", 3, "C", "D"),
        )
        .TOP_K(10, by=key.ASC())
    )


def sqlite_udf_nested():
    # Return the percentage of customers with a certain condition as True
    # depending on their market segment:
    # - For "BUILDING", the account balance must be positive.
    # - For "MACHINERY", the account balance must be within 500 of the
    #   minimum account balance globally.
    # - For "HOUSEHOLD", the customer's first ever order must have been made
    #   on the 366th day of the year.
    # - For anything else, the condition is False.
    # Round to 2 decimal places.
    custs = customers.CALCULATE(
        first_order_date=MIN(orders.order_date), min_bal=RELMIN(account_balance)
    ).WHERE(HAS(orders))
    return TPCH_SQLITE_UDFS.CALCULATE(
        p=ROUND(
            PERCENTAGE(
                DECODE3(
                    custs.market_segment,
                    "BUILDING",
                    POSITIVE(custs.account_balance),
                    "MACHINERY",
                    EPSILON(custs.account_balance, custs.min_bal, 500),
                    "HOUSEHOLD",
                    INTEGER(FORMAT_DATETIME("%j", custs.first_order_date)) == "366",
                    False,
                )
            ),
            2,
        )
    )
