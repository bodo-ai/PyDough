__all__ = [
    "impl_tpch_q1",
    "impl_tpch_q10",
    "impl_tpch_q11",
    "impl_tpch_q12",
    "impl_tpch_q13",
    "impl_tpch_q14",
    "impl_tpch_q15",
    "impl_tpch_q16",
    "impl_tpch_q17",
    "impl_tpch_q18",
    "impl_tpch_q2",
    "impl_tpch_q3",
    "impl_tpch_q4",
    "impl_tpch_q5",
    "impl_tpch_q6",
    "impl_tpch_q7",
    "impl_tpch_q8",
    "impl_tpch_q9",
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
        L_RETURNFLAG=return_flag,
        L_LINESTATUS=status,
        SUM_QTY=SUM(l.quantity),
        SUM_BASE_PRICE=SUM(l.extended_price),
        SUM_DISC_PRICE=SUM(l.extended_price * (1 - l.discount)),
        SUM_CHARGE=SUM(l.extended_price * (1 - l.discount) * (1 + l.tax)),
        AVG_QTY=AVG(l.quantity),
        AVG_PRICE=AVG(l.extended_price),
        AVG_DISC=AVG(l.discount),
        COUNT_ORDER=COUNT(l),
    ).ORDER_BY(L_RETURNFLAG.ASC(), L_LINESTATUS.ASC())


def impl_tpch_q2():
    """
    PyDough implementation of TPCH Q2, truncated to 10 rows.
    """
    selected_parts = (
        Nations.WHERE(region.name == "EUROPE")
        .suppliers.supply_records.part(
            s_acctbal=BACK(2).account_balance,
            s_name=BACK(2).name,
            n_name=BACK(3).name,
            s_address=BACK(2).address,
            s_phone=BACK(2).phone,
            s_comment=BACK(2).comment,
            supplycost=BACK(1).supplycost,
        )
        .WHERE(ENDSWITH(part_type, "BRASS") & (size == 15))
    )

    return (
        PARTITION(selected_parts, name="p", by=key)(best_cost=MIN(p.supplycost))
        .p.WHERE(
            (supplycost == BACK(1).best_cost)
            & ENDSWITH(part_type, "BRASS")
            & (size == 15)
        )(
            S_ACCTBAL=s_acctbal,
            S_NAME=s_name,
            N_NAME=n_name,
            P_PARTKEY=key,
            P_MFGR=manufacturer,
            S_ADDRESS=s_address,
            S_PHONE=s_phone,
            S_COMMENT=s_comment,
        )
        .TOP_K(
            10,
            by=(S_ACCTBAL.DESC(), N_NAME.ASC(), S_NAME.ASC(), P_PARTKEY.ASC()),
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
        L_ORDERKEY=order_key,
        REVENUE=SUM(l.extended_price * (1 - l.discount)),
        O_ORDERDATE=order_date,
        O_SHIPPRIORITY=ship_priority,
    ).TOP_K(10, by=(REVENUE.DESC(), O_ORDERDATE.ASC(), L_ORDERKEY.ASC()))


def impl_tpch_q4():
    """
    PyDough implementation of TPCH Q4.
    """
    selected_lines = lines.WHERE(commit_date < receipt_date)
    selected_orders = Orders.WHERE(
        (order_date >= datetime.date(1993, 7, 1))
        & (order_date < datetime.date(1993, 10, 1))
        & HAS(selected_lines)
    )
    return PARTITION(selected_orders, name="o", by=order_priority)(
        O_ORDERPRIORITY=order_priority,
        ORDER_COUNT=COUNT(o),
    ).ORDER_BY(O_ORDERPRIORITY.ASC())


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
        N_NAME=name, REVENUE=SUM(selected_lines.value)
    ).ORDER_BY(REVENUE.DESC())


def impl_tpch_q6():
    """
    PyDough implementation of TPCH Q6.
    """
    selected_lines = Lineitems.WHERE(
        (ship_date >= datetime.date(1994, 1, 1))
        & (ship_date < datetime.date(1995, 1, 1))
        & (0.05 <= discount)
        & (discount <= 0.07)
        & (quantity < 24)
    )(amt=extended_price * discount)
    return TPCH(REVENUE=SUM(selected_lines.amt))


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
            ((supp_nation == "FRANCE") & (cust_nation == "GERMANY"))
            | ((supp_nation == "GERMANY") & (cust_nation == "FRANCE"))
        )
    )

    return PARTITION(line_info, name="l", by=(supp_nation, cust_nation, l_year))(
        SUPP_NATION=supp_nation,
        CUST_NATION=cust_nation,
        L_YEAR=l_year,
        REVENUE=SUM(l.volume),
    ).ORDER_BY(
        SUPP_NATION.ASC(),
        CUST_NATION.ASC(),
        L_YEAR.ASC(),
    )


def impl_tpch_q8():
    """
    PyDough implementation of TPCH Q8.
    """
    volume_data = (
        Nations.suppliers.supply_records.WHERE(
            part.part_type == "ECONOMY ANODIZED STEEL"
        )
        .lines(volume=extended_price * (1 - discount))
        .order(
            o_year=YEAR(order_date),
            volume=BACK(1).volume,
            brazil_volume=IFF(BACK(4).name == "BRAZIL", BACK(1).volume, 0),
        )
        .WHERE(
            (order_date >= datetime.date(1995, 1, 1))
            & (order_date <= datetime.date(1996, 12, 31))
            & (customer.nation.region.name == "AMERICA")
        )
    )

    return PARTITION(volume_data, name="v", by=o_year)(
        O_YEAR=o_year,
        MKT_SHARE=SUM(v.brazil_volume) / SUM(v.volume),
    )


def impl_tpch_q9():
    """
    PyDough implementation of TPCH Q9, truncated to 10 rows.
    """
    selected_lines = Nations.suppliers.supply_records.WHERE(
        CONTAINS(part.name, "green")
    ).lines(
        nation=BACK(3).name,
        o_year=YEAR(order.order_date),
        value=extended_price * (1 - discount) - BACK(1).supplycost * quantity,
    )
    return PARTITION(selected_lines, name="l", by=(nation, o_year))(
        NATION=nation, O_YEAR=o_year, AMOUNT=SUM(l.value)
    ).TOP_K(
        10,
        by=(NATION.ASC(), O_YEAR.DESC()),
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
        C_CUSTKEY=key,
        C_NAME=name,
        REVENUE=SUM(selected_lines.amt),
        C_ACCTBAL=acctbal,
        N_NAME=nation.name,
        C_ADDRESS=address,
        C_PHONE=phone,
        C_COMMENT=comment,
    ).TOP_K(20, by=(REVENUE.DESC(), C_CUSTKEY.ASC()))


def impl_tpch_q11():
    """
    PyDough implementation of TPCH Q11, truncated to 10 rows
    """
    is_german_supplier = supplier.nation.name == "GERMANY"
    selected_records = PartSupp.WHERE(is_german_supplier)(metric=supplycost * availqty)
    return (
        TPCH(min_market_share=SUM(selected_records.metric) * 0.0001)
        .PARTITION(selected_records, name="ps", by=part_key)(
            PS_PARTKEY=part_key, VALUE=SUM(ps.metric)
        )
        .WHERE(VALUE > BACK(1).min_market_share)
        .TOP_K(10, by=VALUE.DESC())
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
        L_SHIPMODE=ship_mode,
        HIGH_LINE_COUNT=SUM(l.is_high_priority),
        LOW_LINE_COUNT=SUM(~(l.is_high_priority)),
    ).ORDER_BY(L_SHIPMODE.ASC())


def impl_tpch_q13():
    """
    PyDough implementation of TPCH Q13, truncated to 10 rows.
    """
    customer_info = Customers(
        key,
        num_non_special_orders=COUNT(
            orders.WHERE(~(LIKE(comment, "%special%requests%")))
        ),
    )
    return PARTITION(customer_info, name="custs", by=num_non_special_orders)(
        C_COUNT=num_non_special_orders, CUSTDIST=COUNT(custs)
    ).TOP_K(10, by=(CUSTDIST.DESC(), C_COUNT.DESC()))


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
        promo_value=IFF(STARTSWITH(part.part_type, "PROMO"), value, 0),
    )
    return TPCH(
        PROMO_REVENUE=100.0
        * SUM(selected_lines.promo_value)
        / SUM(selected_lines.value)
    )


def impl_tpch_q15():
    """
    PyDough implementation of TPCH Q15.
    """
    selected_lines = lines.WHERE(
        (ship_date >= datetime.date(1996, 1, 1))
        & (ship_date < datetime.date(1996, 4, 1))
    )
    total = SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    return (
        TPCH(max_revenue=MAX(Suppliers(total_revenue=total).total_revenue))
        .Suppliers(
            S_SUPPKEY=key,
            S_NAME=name,
            S_ADDRESS=address,
            S_PHONE=phone,
            TOTAL_REVENUE=total,
        )
        .WHERE(TOTAL_REVENUE == BACK(1).max_revenue)
        .ORDER_BY(S_SUPPKEY.ASC())
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
        P_BRAND=p_brand,
        P_TYPE=p_type,
        P_SIZE=p_size,
        SUPPLIER_COUNT=NDISTINCT(ps.supplier_key),
    ).TOP_K(10, by=(SUPPLIER_COUNT.DESC(), P_BRAND.ASC(), P_TYPE.ASC(), P_SIZE.ASC()))


def impl_tpch_q17():
    """
    PyDough implementation of TPCH Q17.
    """
    selected_lines = Parts.WHERE((brand == "Brand#23") & (container == "MED BOX"))(
        avg_quantity=AVG(lines.quantity)
    ).lines.WHERE(quantity < 0.2 * BACK(1).avg_quantity)
    return TPCH(AVG_YEARLY=SUM(selected_lines.extended_price) / 7.0)


def impl_tpch_q18():
    """
    PyDough implementation of TPCH Q18, truncated to 10 rows
    """
    return (
        Orders(
            C_NAME=customer.name,
            C_CUSTKEY=customer.key,
            O_ORDERKEY=key,
            O_ORDERDATE=order_date,
            O_TOTALPRICE=total_price,
            TOTAL_QUANTITY=SUM(lines.quantity),
        )
        .WHERE(TOTAL_QUANTITY > 300)
        .TOP_K(
            10,
            by=(O_TOTALPRICE.DESC(), O_ORDERDATE.ASC()),
        )
    )


def impl_tpch_q19():
    """
    PyDough implementation of TPCH Q19.
    """
    selected_lines = Lineitems.WHERE(
        (ISIN(ship_mode, ("AIR", "AIR REG")))
        & (ship_instruct == "DELIVER IN PERSON")
        & (part.size >= 1)
        & (
            (
                (part.size <= 5)
                & (quantity >= 1)
                & (quantity <= 11)
                & ISIN(
                    part.container,
                    ("SM CASE", "SM BOX", "SM PACK", "SM PKG"),
                )
                & (part.brand == "Brand#12")
            )
            | (
                (part.size <= 10)
                & (quantity >= 10)
                & (quantity <= 20)
                & ISIN(
                    part.container,
                    ("MED BAG", "MED BOX", "MED PACK", "MED PKG"),
                )
                & (part.brand == "Brand#23")
            )
            | (
                (part.size <= 15)
                & (quantity >= 20)
                & (quantity <= 30)
                & ISIN(
                    part.container,
                    ("LG CASE", "LG BOX", "LG PACK", "LG PKG"),
                )
                & (part.brand == "Brand#34")
            )
        )
    )
    return TPCH(
        REVENUE=SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    )


def impl_tpch_q20():
    """
    PyDough implementation of TPCH Q20, truncated to 10 rows.
    """
    part_qty = SUM(
        lines.WHERE(
            (ship_date >= datetime.date(1994, 1, 1))
            & (ship_date < datetime.date(1995, 1, 1))
        ).quantity
    )
    selected_part_supplied = supply_records.part.WHERE(
        STARTSWITH(name, "forest") & (BACK(1).availqty > part_qty * 0.5)
    )

    return (
        Suppliers(
            S_NAME=name,
            S_ADDRESS=address,
        )
        .WHERE((nation.name == "CANADA") & COUNT(selected_part_supplied) > 0)
        .TOP_K(10, by=S_NAME.ASC())
    )


def impl_tpch_q21():
    """
    PyDough implementation of TPCH Q21, truncated to 10 rows.
    """
    date_check = receipt_date > commit_date
    different_supplier = supplier_key != BACK(2).supplier_key
    waiting_entries = lines.WHERE(date_check).order.WHERE(
        (order_status == "F")
        & HAS(lines.WHERE(different_supplier))
        & HASNOT(lines.WHERE(different_supplier & date_check))
    )
    return Suppliers.WHERE(nation.name == "SAUDI ARABIA")(
        S_NAME=name,
        NUMWAIT=COUNT(waiting_entries),
    ).TOP_K(
        10,
        by=(NUMWAIT.DESC(), S_NAME.ASC()),
    )


def impl_tpch_q22():
    """
    PyDough implementation of TPCH Q22.
    """
    selected_customers = Customers(cntry_code=phone[:2]).WHERE(
        ISIN(cntry_code, ("13", "31", "23", "29", "30", "18", "17"))
    )
    return (
        TPCH(avg_balance=AVG(selected_customers.WHERE(acctbal > 0.0).acctbal))
        .PARTITION(
            selected_customers.WHERE(
                (acctbal > BACK(1).avg_balance) & (COUNT(orders) == 0)
            ),
            name="custs",
            by=cntry_code,
        )(
            CNTRY_CODE=cntry_code,
            NUM_CUSTS=COUNT(custs),
            TOTACCTBAL=SUM(custs.acctbal),
        )
        .ORDER_BY(CNTRY_CODE.ASC())
    )
