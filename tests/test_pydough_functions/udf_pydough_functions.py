"""
TODO
"""

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def sqlite_format_datetime():
    return orders.CALCULATE(
        key,
        d1=FORMAT_DATETIME("%d/%m/%Y", order_date),
        d2=FORMAT_DATETIME("%Y:%j", order_date),
        d3=INTEGER(FORMAT_DATETIME("%s", order_date)),
    ).TOP_K(5, by=total_price.ASC())


def sqlite_combine_strings():
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


def sqlite_percent_positive():
    return regions.CALCULATE(
        name,
        pct_cust_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.customers.account_balance)), 2
        ),
        pct_supp_positive=ROUND(
            PERCENTAGE(POSITIVE(nations.suppliers.account_balance)), 2
        ),
    ).ORDER_BY(name.ASC())


def sqlite_percent_epsilon():
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


def sqlite_covar_pop():
    order_info = (
        nations.customers.CALCULATE(account_balance)
        .WHERE(market_segment == "BUILDING")
        .orders.WHERE(YEAR(order_date) == 1998)
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


def sqlite_nval():
    return regions.nations.CALCULATE(
        rname=region.name,
        nname=name,
        v1=NVAL(name, 3, by=name),
        v2=NVAL(name, 1, by=name, per="regions"),
        v3=NVAL(name, 2, by=name, per="regions", frame=(1, None)),
        v4=NVAL(name, 5, by=name, cumulative=True),
    ).ORDER_BY(rname.ASC(), nname.ASC())
