__all__ = [
    "impl_tpch_q1",
    "impl_tpch_q2",
    "impl_tpch_q3",
    "impl_tpch_q4",
    "impl_tpch_q5",
    "impl_tpch_q6",
    "impl_tpch_q7",
    "impl_tpch_q8",
    "impl_tpch_q9",
    "impl_tpch_q10",
    "impl_tpch_q11",
    "impl_tpch_q12",
    "impl_tpch_q13",
    "impl_tpch_q14",
    "impl_tpch_q15",
    "impl_tpch_q16",
    "impl_tpch_q17",
    "impl_tpch_q18",
]

import datetime

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def impl_tpch_q1():
    """
    PyDough implementation of TPCH Q1.
    """
    selected_lines = Lineitems.WHERE((ship_date <= datetime.date(1998, 12, 1)))
    return PARTITION(selected_lines, name="l", by=(return_flag, status))(
        l_returnflag=return_flag,
        l_linestatus=status,
        sum_qty=SUM(l.quantity),
        sum_base_price=SUM(l.extended_price),
        sum_disc_price=SUM(l.extended_price * (1 - l.discount)),
        sum_charge=SUM(l.extended_price * (1 - l.discount) * (1 + l.tax)),
        avg_qty=AVG(l.quantity),
        avg_price=AVG(l.extended_price),
        avg_disc=AVG(l.discount),
        count_order=COUNT(l),
    ).ORDER_BY(l_returnflag.ASC(), l_linestatus.ASC())


def impl_tpch_q2():
    """
    PyDough implementation of TPCH Q2.
    """
    selected_parts = Nations.WHERE(region.name == "EUROPE").suppliers.parts_supplied(
        s_acctbal=BACK(1).account_balance,
        s_name=BACK(1).name,
        n_name=BACK(2).name,
        p_partkey=key,
        p_mfgr=manufacturer,
        s_address=BACK(1).address,
        s_phone=BACK(1).phone,
        s_comment=BACK(1).comment,
    )

    return (
        PARTITION(selected_parts, name="p", by=key)(best_cost=MIN(p.ps_supplycost))
        .p.WHERE(
            (ps_supplycost == BACK(1).best_cost)
            & ENDSWITH(part_type, "BRASS")
            & (size == 15)
        )
        .ORDER_BY(
            s_acctbal.DESC(),
            n_name.ASC(),
            s_name.ASC(),
            p_partkey.ASC(),
        )
    )


def impl_tpch_q3():
    """
    PyDough implementation of TPCH Q3.
    """
    selected_lines = Orders.WHERE(
        (customer.mktsegment == "BUILDING") & (order_date < datetime.date(1995, 3, 15))
    ).lines.WHERE(ship_date > datetime.date(1995, 3, 15))(
        BACK(1).order_date,
        BACK(1).ship_priority,
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
    ).lines.WHERE(supplier.nation.name == BACK(3).name)(
        value=extended_price * (1 - discount)
    )
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
        & (0.05 < discount)
        & (discount < 0.07)
        & (quantity < 24)
    )(amt=extended_price * discount)
    return TPCH(revenue=SUM(selected_lines.amt))


def impl_tpch_q7():
    """
    PyDough implementation of TPCH Q7.
    """
    line_info = Lineitems(
        supp_nation=supplier.nation.name,
        cust_nation=order.customer.nation.name,
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
        supp_nation,
        cust_nation,
        l_year,
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
        (order_date >= datetime.date(1995, 1, 1))
        & (order_date <= datetime.date(1996, 12, 31))
        & (customer.region.name == "AMERICA")
    )

    volume_data = selected_orders.lines(
        volume=extended_price * (1 - discount)
    ).supplier.WHERE(ps_part.part_type == "ECONOMY ANODIZED STEEL")(
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
        o_year.DESC(),
    )


def impl_tpch_q10():
    """
    PyDough implementation of TPCH Q10.
    """
    selected_lines = orders.WHERE(
        (order_date >= datetime.date(1993, 10, 1))
        & (order_date < datetime.date(1994, 1, 1))
    ).lines.WHERE(return_flag == "R")(amt=extended_price * (1 - discount))
    return Customers(
        c_key=key,
        c_name=name,
        revenue=SUM(selected_lines.amt),
        c_acctbal=acctbal,
        n_name=nation.name,
        c_address=address,
        c_phone=phone,
        c_comment=comment,
    ).TOP_K(20, by=(revenue.DESC(), c_key.ASC()))


def impl_tpch_q11():
    """
    PyDough implementation of TPCH Q11.
    """
    is_german_supplier = supplier.nation.name == "GERMANY"
    metric = supplycost * availqty
    selected_records = PartSupp.WHERE(is_german_supplier)(metric=metric)
    selected_part_records = supply_records.WHERE(is_german_supplier)(metric=metric)
    return (
        TPCH(min_market_share=SUM(selected_records.metric) * 0.0001)
        .Parts(ps_partkey=key, val=SUM(selected_part_records.metric))
        .WHERE(val > BACK(1).min_market_share)
        .ORDER_BY(val.DESC())
    )


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
        ship_mode,
        high_line_count=SUM(l.is_high_priority),
        low_line_count=SUM(~(l.is_high_priority)),
    ).ORDER_BY(ship_mode.ASC())


def impl_tpch_q13():
    """
    PyDough implementation of TPCH Q13.
    """
    customer_info = Customers(
        key,
        num_non_special_orders=COUNT(
            orders.WHERE(~(LIKE(comment, "%special%requests%")))
        ),
    )
    return PARTITION(customer_info, name="custs", by=num_non_special_orders)(
        c_count=num_non_special_orders, custdist=COUNT(custs)
    )


def impl_tpch_q14():
    """
    PyDough implementation of TPCH Q14.
    """
    value = extended_price * (1 - discount)
    selected_lines = Lineitems.WHERE(
        (ship_date >= datetime.date(1995, 9, 1))
        & (ship_date < datetime.date(1995, 10, 1))
    )(
        value=value,
        promo_value=IFF(
            STARTSWITH(part_and_supplier.part.part_type, "PROMO"), value, 0
        ),
    )
    return TPCH(
        promo_revenue=100.0
        * SUM(selected_lines.promo_value)
        / SUM(selected_lines.value)
    )


def impl_tpch_q15():
    """
    PyDough implementation of TPCH Q15.
    """
    selected_lines = lines.WHERE(
        (ship_date >= datetime.date(1996, 1, 1))
        & (ship_date < datetime.date(1996, 3, 1))
    )
    total = SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    return (
        TPCH(max_revenue=MAX(Suppliers(total_revenue=total).total_revenue))
        .Suppliers(
            s_suppkey=key,
            s_name=name,
            s_address=address,
            s_phone=phone_number,
            total_revenue=total_revenue,
        )
        .WHERE(total_revenue == BACK(1).max_revenue)
        .ORDER_BY(s_suppkey.ASC())
    )


def impl_tpch_q16():
    """
    PyDough implementation of TPCH Q16.
    """
    selected_records = (
        Parts.WHERE(
            (brand != "BRAND#45")
            & ~STARTSWITH(part_type, "MEDIUM POLISHED%")
            & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])
        )
        .supply_records(
            p_brand=BACK(1).brand,
            p_type=BACK(1).part_type,
            p_size=BACK(1).size,
            ps_suppkey=supplier_key,
        )
        .WHERE(~LIKE(supplier.comment, "%Customer%Complaints%"))
    )
    return PARTITION(selected_records, name="ps", by=(p_brand, p_type, p_size))(
        p_brand,
        p_type,
        p_size,
        supplier_cnt=NDISTINCT(ps.supplier_key),
    )


def impl_tpch_q17():
    """
    PyDough implementation of TPCH Q17.
    """
    selected_lines = Parts.WHERE((brand == "Brand#23") & (container == "MED BOX"))(
        avg_quantity=AVG(lines.quantity)
    ).lines.WHERE(quantity < 0.2 * BACK(1).avg_quantity)
    return TPCH(avg_yearly=SUM(selected_lines.extended_price) / 7.0)


def impl_tpch_q18():
    """
    PyDough implementation of TPCH Q18.
    """
    return (
        Orders(
            c_name=customer.name,
            c_custkey=customer.key,
            o_orderkey=key,
            o_orderdate=order_date,
            o_totalprice=total_price,
            total_quantity=SUM(lines.quantity),
        )
        .WHERE(total_quantity > 300)
        .ORDER_BY(
            o_totalprice.DESC(),
            o_orderdate.ASC(),
        )
    )


def impl_tpch_q19():
    """
    PyDough implementation of TPCH Q19.
    """
    selected_lines = Lineitems.WHERE(
        (shipmode in ("AIR", "AIR REG"))
        & (ship_instruct == "DELIVER IN PERSON")
        & (part_and_supplier.part.size >= 1)
        & (
            (
                (part_and_supplier.part.size < 5)
                & (quantity >= 1)
                & (quantity <= 11)
                & ISIN(
                    part_and_supplier.part.container,
                    ("SM CASE", "SM BOX", "SM PACK", "SM PKG"),
                )
                & (part_and_supplier.part.brand == "Brand#12")
            )
            | (
                (part_and_supplier.part.size < 10)
                & (quantity >= 10)
                & (quantity <= 21)
                & ISIN(
                    part_and_supplier.part.container,
                    ("MED CASE", "MED BOX", "MED PACK", "MED PKG"),
                )
                & (part_and_supplier.part.brand == "Brand#23")
            )
            | (
                (part_and_supplier.part.size < 15)
                & (quantity >= 20)
                & (quantity <= 31)
                & ISIN(
                    part_and_supplier.part.container,
                    ("LG CASE", "LG BOX", "LG PACK", "LG PKG"),
                )
                & (part_and_supplier.part.brand == "Brand#34")
            )
        )
    )
    return TPCH(
        revenue=SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    )


def impl_tpch_q20():
    """
    PyDough implementation of TPCH Q20.
    """
    selected_lines = lines.WHERE(
        (ship_date >= datetime.date(1994, 1, 1))
        & (ship_date < datetime.date(1995, 1, 1))
    )
    return Parts.WHERE(STARTSWITH(name, "forest"))(
        quantity_threshold=SUM(selected_lines.quantity)
    ).suppliers_of_part.WHERE(
        (ps_availqty > BACK(1).quantity_threshold) & (nation.name == "CANADA")
    )(s_name=name, s_address=address)


def impl_tpch_q21():
    """
    PyDough implementation of TPCH Q21.
    """
    date_check = receipt_date > commit_date
    different_supplier = supplier_key != BACK(2).supplier_key
    waiting_entries = lines.WHERE(receipt_date > commit_date).order.WHERE(
        (order_status == "F")
        & (COUNT(lines.WHERE(different_supplier)) > 0)
        & (COUNT(lines.WHERE(different_supplier & date_check)) == 0)
    )
    return Suppliers.WHERE(nation.name == "SAUDI ARABIA")(
        s_name=name,
        numwait=COUNT(waiting_entries),
    ).ORDER_BY(
        numwait.DESC(),
        s_name.ASC(),
    )


def impl_tpch_q22():
    """
    PyDough implementation of TPCH Q22.
    """
    cust_info = Customers(cntry_code=phone[:2]).WHERE(
        ISIN(cntry_code, ("13", "31", "23", "29", "30", "18", "17"))
        & (COUNT(orders) == 0)
    )
    selected_customers = Customers(cntry_code=phone[:2]).WHERE(
        ISIN(cntry_code, ("13", "31", "23", "29", "30", "18", "17"))
        & (COUNT(orders) == 0)
        & (acctbal > BACK(1).avg_balance)
    )
    return TPCH(avg_balance=AVG(cust_info.WHERE(acctbal > 0.0).acctbal)).PARTITION(
        selected_customers, name="custs", by=cntry_code
    )(
        cntry_code,
        num_custs=COUNT(custs),
        totacctbal=SUM(custs.acctbal),
    )
