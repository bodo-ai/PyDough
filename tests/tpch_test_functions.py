__all__ = ["impl_tpch_q6", "impl_tpch_q6", "impl_tpch_q10"]

import datetime

from pydough.unqualified import (
    SUM,
    PARTITION,
    AVG,
    MIN,
    MAX,
    COUNT,
    YEAR,
    CONTAINS,
    IFF,
    LIKE,
)

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def impl_tpch_q1():
    """
    PyDough implementation of TPCH Q1.
    """
    selected_lines = Lineitems.WHERE((ship_date <= datetime.date(1998, 12, 1)))
    return PARTITION(selected_lines, name="l", by=(return_flag, line_status))(
        return_flag=return_flag,
        line_status=line_status,
        sum_qty=SUM(l.quantity),
        sum_base_price=SUM(l.extended_price),
        sum_disc_price=SUM(l.extended_price * (1 - l.discount)),
        sum_charge=SUM(l.extended_price * (1 - l.discount) * (1 + l.tax)),
        avg_qty=AVG(l.quantity),
        avg_price=AVG(l.extended_price),
        avg_disc=AVG(l.discount),
        count_order=COUNT(l),
    ).ORDER_BY(return_flag.ASC(), line_status.ASC())


def impl_tpch_q2():
    """
    PyDough implementation of TPCH Q2.
    """
    selected_parts = Nations.WHERE(region.name == "EUROPE").suppliers.parts_supplied

    minimal_parts = PARTITION(selected_parts, name="p", by=part_key)(
        best_cost=MIN(ps_supplycost)
    ).p.WHERE(ps_supplycost == BACK(1).best_cost)

    return minimal_parts.WHERE(ps_supplycost == BACK(1).best_cost)(
        s_acctbal=BACK(1).account_balance,
        s_name=BACK(1).name,
        n_name=BACK(2).name,
        p_partkey=key,
        p_mfgr=mfgr,
        s_address=BACK(1).address,
        s_phone=BACK(1).phone,
        s_comment=BACK(1).comment,
    ).ORDER_BY(
        account_balance.DESC(),
        n_name.ASC(),
        s_name.ASC(),
        p_partkey.ASC(),
    )


def impl_tpch_q3():
    """
    PyDough implementation of TPCH Q3.
    """
    selected_lines = Orders.WHERE(
        (customer.mktsegment == "BUILDING") & (order_date < datetime.date(1995, 3, 15))
    ).lines.WHERE(ship_date > datetime.date(1995, 3, 15))(
        order_date=BACK(1).order_date,
        ship_priority=BACK(1).ship_priority,
    )

    return PARTITION(
        selected_lines, name="l", by=(order_key, order_date, ship_priority)
    )(
        l_orderkey=order_key,
        revenue=SUM(l.extended_price * (1 - l.discount)),
        o_orderdate=order_date,
        o_shippriority=ship_priority,
    ).TOP_K(10, by=(revenue.DESC(), o_orderdate.ASC(), l_orderkey.ASC()))


def impl_tpch_q4():
    """
    PyDough implementation of TPCH Q4.
    """
    selected_lines = lines.WHERE(commit_date < receipt_date)
    selected_orders = Orders.WHERE(
        (order_date >= datetime.date(1993, 7, 1))
        & (order_date < datetime.date(1993, 10, 1))
        & (COUNT(selected_lines) > 0)
    )
    return PARTITION(selected_orders, name="o", by=order_priority)(
        order_priority,
        order_count=COUNT(o),
    ).ORDER_BY(order_priority.ASC())


def impl_tpch_q5():
    """
    PyDough implementation of TPCH Q5.
    """
    selected_lines = customers.orders.WHERE(
        (order_date >= datetime.date(1994, 1, 1))
        & (order_date < datetime.date(1995, 1, 1))
    ).lines.WHERE(supplier.nation.name == BACK(3).name)
    return Nations.WHERE(region.name == "ASIA")(
        n_name=name, revenue=SUM(selected_lines.value)
    ).ORDER_BY(revenue.DESC())


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


def impl_tpch_q7():
    """
    PyDough implementation of TPCH Q7.
    """
    line_info = Lineitems(
        supp_nation=supplier.nation.name,
        cust_nation=customer.nation.name,
        l_year=YEAR(ship_date),
        volume=extended_price * (1 - discount),
    ).WHERE(
        (ship_date >= datetime.date(1995, 1, 1))
        & (ship_date <= datetime.date(1996, 12, 31))
        & (
            ((supp_nation == "France") & (cust_nation == "Germany"))
            | ((supp_nation == "Germany") & (cust_nation == "France"))
        )
    )

    return PARTITION(line_info, name="l", by=(supp_nation, cust_nation, l_year))(
        supp_nation=supp_nation,
        cust_nation=cust_nation,
        l_year=l_year,
        revenue=SUM(l.volume),
    ).ORDER_BY(
        supp_nation.ASC(),
        cust_nation.ASC(),
        l_year.ASC(),
    )


def impl_tpch_q8():
    """
    PyDough implementation of TPCH Q8.
    """
    selected_orders = Orders.WHERE(
        (ship_date >= datetime.date(1995, 1, 1))
        & (ship_date <= datetime.date(1996, 12, 31))
        & (customer.region.name == "AMERICA")
    )

    volume_data = selected_orders.lines(
        volume=extended_price * (1 - discount)
    ).supplier.WHERE(ps_part.p_type == "ECONOMY ANODIZED STEEL")(
        o_year=YEAR(BACK(2).order_date),
        volume=BACK(1).volume,
        brazil_volume=IFF(nation.name == "BRAZIL", BACK(1).volume, 0),
    )

    return PARTITION(volume_data, name="v", by=o_year)(
        o_year=o_year, mkt_share=SUM(v.brazil_volume) / SUM(v.volume)
    )


def impl_tpch_q9():
    """
    PyDough implementation of TPCH Q9.
    """
    selected_lines = Nations.suppliers.lines(
        nation=BACK(2).name,
        o_year=YEAR(order.order_date),
        value=extended_price * (1 - discount) - ps_supplycost * quantity,
    ).WHERE(CONTAINS(part.name, "green"))
    return PARTITION(selected_lines, name="l", by=(nation, o_year))(
        nation=nation, o_year=o_year, amount=SUM(l.value)
    ).ORDER_BY(
        nation.ASC(),
        o_year.dESC(),
    )


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
            c_key=key,
            c_name=name,
            revenue=SUM(selected_lines.amt),
            c_acctbal=acctbal,
            n_name=nation.name,
            c_address=address,
            c_phone=phone,
            c_comment=comment,
        )
        .ORDER_BY(revenue.DESC(), c_key.ASC())
        .TOP_K(20)
    )


def impl_tpch_q11():
    """
    PyDough implementation of TPCH Q11.

    TODO: this one may need fixing since it relates a global aggregated value
    to something done within a partition.
    """
    selected_records = PartSupp.WHERE(supplier.nation.name == "GERMANY")
    min_market_share = TPCH(
        amt=SUM(selected_records.supplycost * selected_records.availqty) * 0.0001
    )
    return PARTITION(selected_records, name="ps", by=part_key)(
        ps_partkey=part_key, val=SUM(ps.supplycost * ps.availqty)
    ).WHERE(val > min_market_share.amt)


def impl_tpch_q12():
    """
    PyDough implementation of TPCH Q12.
    """
    selected_lines = Lineitems.WHERE(
        ((ship_mode == "MAIL") | (ship_mode == "SHIP"))
        & (ship_date < commit_date)
        & (commit_date < receipt_date)
        & (receipt_date >= datetime.date(1994, 1, 1))
        & (receipt_date < datetime.date(1995, 1, 1))
    )(
        is_high_priority=(order.order_priority == "1-URGENT")
        | (order.order_priority == "2-HIGH"),
    )
    return PARTITION(selected_lines, "l", by=ship_mode)(
        ship_mode=ship_mode,
        high_line_count=SUM(l.is_high_priority),
        low_line_count=SUM(~(l.is_high_priority)),
    ).ORDER_BY(ship_mode.ASC())


def impl_tpch_q13():
    """
    PyDough implementation of TPCH Q12.
    """
    customer_info = Customers(
        key=key,
        num_non_special_orders=COUNT(
            orders.WHERE(~(LIKE(comment, "%special%requests%")))
        ),
    )
    return PARTITION(customer_info, name="custs", by=num_non_special_orders)(
        c_count=num_non_special_orders, custdist=COUNT(custs)
    )
