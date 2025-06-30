"""
TODO
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def sqlite_udf_format_datetime():
    return orders.CALCULATE(
        key,
        d1=FORMAT_DATETIME("%d/%m/%Y", order_date),
        d2=FORMAT_DATETIME("%Y:%j", order_date),
        d3=INTEGER(FORMAT_DATETIME("%s", order_date)),
    ).TOP_K(5, by=total_price.ASC())


def sqlite_udf_combine_strings():
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
    return regions.CALCULATE(
        name,
        pct_cust_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.customers.account_balance)), 2
        ),
        pct_supp_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.suppliers.account_balance)), 2
        ),
    ).ORDER_BY(name.ASC())


def sqlite_udf_percent_epsilon():
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
    return regions.CALCULATE(
        name,
        c1=GCAT(name, "-", by=name.ASC()),
        c2=GCAT(name, "-", by=name.DESC()),
        c3=GCAT(name, "-", by=name.ASC(), cumulative=True),
    ).ORDER_BY(name.ASC())


def sqlite_udf_relmin():
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
