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
