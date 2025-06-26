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
    ).TOP_K(10, by=total_price.ASC())
