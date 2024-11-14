__all__ = ["impl_tpch_q6", "impl_tpch_q10"]

import datetime

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def impl_tpch_q6():
    """
    PyDough implementation of TPCH Q6.
    """
    selected_lines = Lineitems.WHERE(
        (ship_date >= datetime.date(1994, 1, 1))
        & (ship_date < datetime.date(1995, 1, 1))
        & (0.05 < discount < 0.07)
        & (quantity < 24)
    )(amt=extendedprice * discount)
    return TPCH(revenue=SUM(selected_lines.amt))


def impl_tpch_q10():
    """
    PyDough implementation of TPCH Q10.
    """
    selected_lines = orders.WHERE(
        (order_date >= datetime.date(1993, 10, 1))
        & (order_date < datetime.date(1994, 1, 1))
    ).lines.WHERE(return_flag == "R")(amt=extendedprice * (1 - discount))
    return (
        Customers(
            key=key,
            name=name,
            revenue=SUM(selected_lines.amt),
            acctbal=acctbal,
            nation_name=nation.name,
            address=address,
            phone=phone,
            comment=comment,
        )
        .ORDER_BY(revenue.DESC(), key.ASC())
        .TOP_K(20)
    )
