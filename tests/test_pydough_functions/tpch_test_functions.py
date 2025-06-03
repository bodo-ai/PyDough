"""
PyDough implementation of TPCH queries.
"""

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
    return (
        lines.WHERE((ship_date <= datetime.date(1998, 12, 1)))
        .PARTITION(name="groups", by=(return_flag, status))
        .CALCULATE(
            L_RETURNFLAG=return_flag,
            L_LINESTATUS=status,
            SUM_QTY=SUM(lines.quantity),
            SUM_BASE_PRICE=SUM(lines.extended_price),
            SUM_DISC_PRICE=SUM(lines.extended_price * (1 - lines.discount)),
            SUM_CHARGE=SUM(
                lines.extended_price * (1 - lines.discount) * (1 + lines.tax)
            ),
            AVG_QTY=AVG(lines.quantity),
            AVG_PRICE=AVG(lines.extended_price),
            AVG_DISC=AVG(lines.discount),
            COUNT_ORDER=COUNT(lines),
        )
        .ORDER_BY(L_RETURNFLAG.ASC(), L_LINESTATUS.ASC())
    )


def impl_tpch_q2():
    """
    PyDough implementation of TPCH Q2, truncated to 10 rows.
    """
    return (
        nations.CALCULATE(n_name=name)
        .WHERE(region.name == "EUROPE")
        .suppliers.CALCULATE(
            s_acctbal=account_balance,
            s_name=name,
            s_address=address,
            s_phone=phone,
            s_comment=comment,
        )
        .supply_records.CALCULATE(
            supply_cost=supply_cost,
        )
        .part.WHERE(ENDSWITH(part_type, "BRASS") & (size == 15))
        .PARTITION(name="groups", by=key)
        .CALCULATE(best_cost=MIN(part.supply_cost))
        .part.WHERE(
            (supply_cost == best_cost) & ENDSWITH(part_type, "BRASS") & (size == 15)
        )
        .CALCULATE(
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
    return (
        orders.CALCULATE(order_date, ship_priority)
        .WHERE(
            (customer.market_segment == "BUILDING")
            & (order_date < datetime.date(1995, 3, 15))
        )
        .lines.WHERE(ship_date > datetime.date(1995, 3, 15))
        .PARTITION(name="groups", by=(order_key, order_date, ship_priority))
        .CALCULATE(
            L_ORDERKEY=order_key,
            REVENUE=SUM(lines.extended_price * (1 - lines.discount)),
            O_ORDERDATE=order_date,
            O_SHIPPRIORITY=ship_priority,
        )
        .TOP_K(10, by=(REVENUE.DESC(), O_ORDERDATE.ASC(), L_ORDERKEY.ASC()))
    )


def impl_tpch_q4():
    """
    PyDough implementation of TPCH Q4.
    """
    return (
        orders.WHERE(
            (YEAR(order_date) == 1993)
            & (QUARTER(order_date) == 3)
            & HAS(lines.WHERE(commit_date < receipt_date))
        )
        .PARTITION(name="priorities", by=order_priority)
        .CALCULATE(
            O_ORDERPRIORITY=order_priority,
            ORDER_COUNT=COUNT(orders),
        )
        .ORDER_BY(O_ORDERPRIORITY.ASC())
    )


def impl_tpch_q5():
    """
    PyDough implementation of TPCH Q5.
    """
    selected_lines = (
        customers.orders.WHERE(
            (order_date >= datetime.date(1994, 1, 1))
            & (order_date < datetime.date(1995, 1, 1))
        )
        .lines.WHERE(supplier.nation.name == nation_name)
        .CALCULATE(value=extended_price * (1 - discount))
    )
    return (
        nations.CALCULATE(nation_name=name)
        .WHERE(region.name == "ASIA")
        .WHERE(HAS(selected_lines))
        .CALCULATE(N_NAME=name, REVENUE=SUM(selected_lines.value))
        .ORDER_BY(REVENUE.DESC())
    )


def impl_tpch_q6():
    """
    PyDough implementation of TPCH Q6.
    """
    selected_lines = lines.WHERE(
        (ship_date >= datetime.date(1994, 1, 1))
        & (ship_date < datetime.date(1995, 1, 1))
        & (0.05 <= discount)
        & (discount <= 0.07)
        & (quantity < 24)
    ).CALCULATE(amt=extended_price * discount)
    return TPCH.CALCULATE(REVENUE=SUM(selected_lines.amt))


def impl_tpch_q7():
    """
    PyDough implementation of TPCH Q7.
    """
    return (
        lines.CALCULATE(
            supp_nation=supplier.nation.name,
            cust_nation=order.customer.nation.name,
            l_year=YEAR(ship_date),
            volume=extended_price * (1 - discount),
        )
        .WHERE(
            (ship_date >= datetime.date(1995, 1, 1))
            & (ship_date <= datetime.date(1996, 12, 31))
            & (
                ((supp_nation == "FRANCE") & (cust_nation == "GERMANY"))
                | ((supp_nation == "GERMANY") & (cust_nation == "FRANCE"))
            )
        )
        .PARTITION(name="groups", by=(supp_nation, cust_nation, l_year))
        .CALCULATE(
            SUPP_NATION=supp_nation,
            CUST_NATION=cust_nation,
            L_YEAR=l_year,
            REVENUE=SUM(lines.volume),
        )
        .ORDER_BY(
            SUPP_NATION.ASC(),
            CUST_NATION.ASC(),
            L_YEAR.ASC(),
        )
    )


def impl_tpch_q8():
    """
    PyDough implementation of TPCH Q8.
    """
    vol_metric = extended_price * (1 - discount)
    is_brazilian = supplier.nation.name == "BRAZIL"
    return (
        lines.WHERE(
            (part.part_type == "ECONOMY ANODIZED STEEL")
            & ISIN(YEAR(order.order_date), (1995, 1996))
            & (order.customer.nation.region.name == "AMERICA")
        )
        .CALCULATE(
            O_YEAR=YEAR(order.order_date),
            volume=vol_metric,
            brazil_volume=IFF(is_brazilian, vol_metric, 0),
        )
        .PARTITION(name="years", by=O_YEAR)
        .CALCULATE(
            O_YEAR,
            MKT_SHARE=SUM(lines.brazil_volume) / SUM(lines.volume),
        )
    )


def impl_tpch_q9():
    """
    PyDough implementation of TPCH Q9, truncated to 10 rows.
    """
    return (
        lines.WHERE(CONTAINS(part.name, "green"))
        .CALCULATE(
            nation_name=supplier.nation.name,
            o_year=YEAR(order.order_date),
            value=extended_price * (1 - discount)
            - part_and_supplier.supply_cost * quantity,
        )
        .PARTITION(name="groups", by=(nation_name, o_year))
        .CALCULATE(NATION=nation_name, O_YEAR=o_year, AMOUNT=SUM(lines.value))
        .TOP_K(
            10,
            by=(NATION.ASC(), O_YEAR.DESC()),
        )
    )


def impl_tpch_q10():
    """
    PyDough implementation of TPCH Q10.
    """
    selected_lines = orders.WHERE(
        (YEAR(order_date) == 1993) & (QUARTER(order_date) == 4)
    ).lines.WHERE(return_flag == "R")
    return customers.CALCULATE(
        C_CUSTKEY=key,
        C_NAME=name,
        REVENUE=SUM(selected_lines.extended_price * (1 - selected_lines.discount)),
        C_ACCTBAL=account_balance,
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
    selected_records = supply_records.WHERE(is_german_supplier).CALCULATE(
        metric=supply_cost * available_quantity
    )
    return (
        TPCH.CALCULATE(min_market_share=SUM(selected_records.metric) * 0.0001)
        .supply_records.WHERE(is_german_supplier)
        .PARTITION(name="parts", by=part_key)
        .CALCULATE(
            PS_PARTKEY=part_key,
            VALUE=SUM(supply_records.supply_cost * supply_records.available_quantity),
        )
        .WHERE(VALUE > min_market_share)
        .TOP_K(10, by=VALUE.DESC())
    )


def impl_tpch_q12():
    """
    PyDough implementation of TPCH Q12.
    """
    return (
        lines.WHERE(
            ((ship_mode == "MAIL") | (ship_mode == "SHIP"))
            & (ship_date < commit_date)
            & (commit_date < receipt_date)
            & (YEAR(receipt_date) == 1994)
        )
        .CALCULATE(
            is_high_priority=ISIN(order.order_priority, ("1-URGENT", "2-HIGH")),
        )
        .PARTITION("modes", by=ship_mode)
        .CALCULATE(
            L_SHIPMODE=ship_mode,
            HIGH_LINE_COUNT=SUM(lines.is_high_priority),
            LOW_LINE_COUNT=SUM(~(lines.is_high_priority)),
        )
        .ORDER_BY(L_SHIPMODE.ASC())
    )


def impl_tpch_q13():
    """
    PyDough implementation of TPCH Q13, truncated to 10 rows.
    """
    selected_orders = orders.WHERE(~(LIKE(comment, "%special%requests%")))
    return (
        customers.CALCULATE(num_non_special_orders=COUNT(selected_orders))
        .PARTITION(name="num_order_groups", by=num_non_special_orders)
        .CALCULATE(C_COUNT=num_non_special_orders, CUSTDIST=COUNT(customers))
        .TOP_K(10, by=(CUSTDIST.DESC(), C_COUNT.DESC()))
    )


def impl_tpch_q14():
    """
    PyDough implementation of TPCH Q14.
    """
    value_metric = extended_price * (1 - discount)
    selected_lines = lines.WHERE(
        (YEAR(ship_date) == 1995) & (MONTH(ship_date) == 9)
    ).CALCULATE(
        value=value_metric,
        promo_value=IFF(STARTSWITH(part.part_type, "PROMO"), value_metric, 0),
    )
    return TPCH.CALCULATE(
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
        TPCH.CALCULATE(
            max_revenue=MAX(
                suppliers.WHERE(HAS(selected_lines))
                .CALCULATE(total_revenue=total)
                .total_revenue
            )
        )
        .suppliers.WHERE(HAS(selected_lines) & (total == max_revenue))
        .CALCULATE(
            S_SUPPKEY=key,
            S_NAME=name,
            S_ADDRESS=address,
            S_PHONE=phone,
            TOTAL_REVENUE=total,
        )
        .ORDER_BY(S_SUPPKEY.ASC())
    )


def impl_tpch_q16():
    """
    PyDough implementation of TPCH Q16.
    """
    valid_part = part.WHERE(
        (brand != "BRAND#45")
        & ~STARTSWITH(part_type, "MEDIUM POLISHED%")
        & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])
    )
    return (
        supply_records.WHERE(
            ~LIKE(supplier.comment, "%Customer%Complaints%") & HAS(valid_part)
        )
        .CALCULATE(
            P_BRAND=valid_part.brand,
            P_TYPE=valid_part.part_type,
            P_SIZE=valid_part.size,
        )
        .PARTITION(name="groups", by=(P_BRAND, P_TYPE, P_SIZE))
        .CALCULATE(
            P_BRAND,
            P_TYPE,
            P_SIZE,
            SUPPLIER_COUNT=NDISTINCT(supply_records.supplier_key),
        )
        .TOP_K(
            10, by=(SUPPLIER_COUNT.DESC(), P_BRAND.ASC(), P_TYPE.ASC(), P_SIZE.ASC())
        )
    )


def impl_tpch_q17():
    """
    PyDough implementation of TPCH Q17.
    """
    selected_lines = parts.WHERE(
        (brand == "Brand#23") & (container == "MED BOX")
    ).lines.WHERE(quantity < 0.2 * RELAVG(quantity, per="parts"))
    return TPCH.CALCULATE(AVG_YEARLY=SUM(selected_lines.extended_price) / 7.0)


def impl_tpch_q18():
    """
    PyDough implementation of TPCH Q18, truncated to 10 rows
    """
    return (
        orders.CALCULATE(
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
    selected_lines = lines.WHERE(
        (ISIN(ship_mode, ("AIR", "AIR REG")))
        & (ship_instruct == "DELIVER IN PERSON")
        & (
            (
                MONOTONIC(1, part.size, 5)
                & MONOTONIC(1, quantity, 11)
                & ISIN(
                    part.container,
                    ("SM CASE", "SM BOX", "SM PACK", "SM PKG"),
                )
                & (part.brand == "Brand#12")
            )
            | (
                MONOTONIC(1, part.size, 10)
                & MONOTONIC(10, quantity, 20)
                & ISIN(
                    part.container,
                    ("MED BAG", "MED BOX", "MED PACK", "MED PKG"),
                )
                & (part.brand == "Brand#23")
            )
            | (
                MONOTONIC(1, part.size, 15)
                & MONOTONIC(20, quantity, 30)
                & ISIN(
                    part.container,
                    ("LG CASE", "LG BOX", "LG PACK", "LG PKG"),
                )
                & (part.brand == "Brand#34")
            )
        )
    )
    return TPCH.CALCULATE(
        REVENUE=SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    )


def impl_tpch_q20():
    """
    PyDough implementation of TPCH Q20, truncated to 10 rows.
    """
    selected_lines = lines.WHERE(YEAR(ship_date) == 1994)
    selected_part = part.WHERE(
        STARTSWITH(name, "forest") & HAS(selected_lines)
    ).CALCULATE(part_qty=SUM(selected_lines.quantity))
    selected_parts_supplied = supply_records.WHERE(
        HAS(selected_part) & (available_quantity > 0.5 * SUM(selected_part.part_qty))
    )

    return (
        suppliers.CALCULATE(
            S_NAME=name,
            S_ADDRESS=address,
        )
        .WHERE(
            (nation.name == "CANADA")
            & (COUNT(selected_parts_supplied) > 0)
            & HAS(selected_parts_supplied)
        )
        .TOP_K(10, by=S_NAME.ASC())
    )


def impl_tpch_q21():
    """
    PyDough implementation of TPCH Q21, truncated to 10 rows.
    """
    date_check = receipt_date > commit_date
    waiting_entries = (
        lines.CALCULATE(original_key=supplier_key)
        .WHERE(date_check)
        .order.WHERE(
            (order_status == "F")
            & HAS(lines.WHERE(supplier_key != original_key))
            & HASNOT(lines.WHERE((supplier_key != original_key) & date_check))
        )
    )
    return (
        suppliers.WHERE(nation.name == "SAUDI ARABIA")
        .CALCULATE(
            S_NAME=name,
            NUMWAIT=COUNT(waiting_entries),
        )
        .TOP_K(
            10,
            by=(NUMWAIT.DESC(), S_NAME.ASC()),
        )
    )


def impl_tpch_q22():
    """
    PyDough implementation of TPCH Q22.
    """
    is_selected_code = ISIN(cntry_code, ("13", "31", "23", "29", "30", "18", "17"))
    selected_customers = customers.CALCULATE(cntry_code=phone[:2]).WHERE(
        is_selected_code
    )
    return (
        TPCH.CALCULATE(
            global_avg_balance=AVG(
                selected_customers.WHERE(account_balance > 0.0).account_balance
            )
        )
        .customers.CALCULATE(cntry_code=phone[:2])
        .WHERE(
            is_selected_code
            & (account_balance > global_avg_balance)
            & (COUNT(orders) == 0)
        )
        .PARTITION(
            name="countries",
            by=cntry_code,
        )
        .CALCULATE(
            CNTRY_CODE=cntry_code,
            NUM_CUSTS=COUNT(customers),
            TOTACCTBAL=SUM(customers.account_balance),
        )
        .ORDER_BY(CNTRY_CODE.ASC())
    )
