"""
Unit tests the PyDough qualification process that transforms unqualified nodes
into qualified DAG nodes.
"""

import datetime
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


def pydough_impl_misc_01(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    TPCH.Nations(nation_name=name, total_balance=SUM(customers.acctbal))
    ```
    """
    return root.Nations(
        nation_name=root.name, total_balance=root.SUM(root.customers.acctbal)
    )


def pydough_impl_misc_02(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    lines_1994 = orders.WHERE(
        (datetime.date(1994, 1, 1) <= order_date) &
        (order_date < datetime.date(1995, 1, 1))
    ).lines
    lines_1995 = orders.WHERE(
        (datetime.date(1995, 1, 1) <= order_date) &
        (order_date < datetime.date(1996, 1, 1))
    ).lines
    TPCH.Nations.customers(
        name=LOWER(name),
        nation_name=BACK(1).name,
        total_1994=SUM(lines_1994.extended_price - lines_1994.tax / 2),
        total_1995=SUM(lines_1995.extended_price - lines_1995.tax / 2),
    )
    ```
    """
    lines_1994 = root.orders.WHERE(
        (datetime.date(1994, 1, 1) <= root.order_date)
        & (root.order_date < datetime.date(1995, 1, 1))
    ).lines
    lines_1995 = root.orders.WHERE(
        (datetime.date(1995, 1, 1) <= root.order_date)
        & (root.order_date < datetime.date(1996, 1, 1))
    ).lines
    return root.Nations.customers(
        name=root.LOWER(root.name),
        nation_name=root.BACK(1).name,
        total_1994=root.SUM(lines_1994.extended_price - lines_1994.tax / 2),
        total_1995=root.SUM(lines_1995.extended_price - lines_1995.tax / 2),
    )


def pydough_impl_misc_03(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for the following PyDough snippet:
    ```
    sizes = PARTITION(Parts, name="p", by=size)(n_parts=COUNT(p))
    TPCH(
        avg_n_parts=AVG(sizes.n_parts)
    )(
        n_parts=COUNT(sizes.WHERE(n_parts > BACK(1).avg_n_parts))
    )
    ```
    """
    sizes = root.PARTITION(root.Parts, name="p", by=root.size)(
        n_parts=root.COUNT(root.p)
    )
    return root.TPCH(avg_n_parts=root.AVG(sizes.n_parts))(
        n_parts=root.COUNT(sizes.WHERE(root.n_parts > root.BACK(1).avg_n_parts))
    )


def pydough_impl_tpch_q1(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 1.
    """
    selected_lines = root.Lineitems.WHERE(root.ship_date <= datetime.date(1998, 12, 1))
    return root.PARTITION(selected_lines, name="l", by=(root.return_flag, root.status))(
        l_returnflag=root.return_flag,
        l_linestatus=root.status,
        sum_qty=root.SUM(root.l.quantity),
        sum_base_price=root.SUM(root.l.extended_price),
        sum_disc_price=root.SUM(root.l.extended_price * (1 - root.l.discount)),
        sum_charge=root.SUM(
            root.l.extended_price * (1 - root.l.discount) * (1 + root.l.tax)
        ),
        avg_qty=root.AVG(root.l.quantity),
        avg_price=root.AVG(root.l.extended_price),
        avg_disc=root.AVG(root.l.discount),
        count_order=root.COUNT(root.l),
    ).ORDER_BY(
        root.return_flag.ASC(),
        root.status.ASC(),
    )


def pydough_impl_tpch_q2(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 2.
    """
    selected_parts = (
        root.Nations.WHERE(root.region.name == "EUROPE")
        .suppliers.supply_records.part(
            s_acctbal=root.BACK(2).account_balance,
            s_name=root.BACK(2).name,
            n_name=root.BACK(3).name,
            s_address=root.BACK(2).address,
            s_phone=root.BACK(2).phone,
            s_comment=root.BACK(2).comment,
            supplycost=root.BACK(1).supplycost,
        )
        .WHERE(root.ENDSWITH(root.part_type, "BRASS") & (root.size == 15))
    )

    return (
        root.PARTITION(selected_parts, name="p", by=root.key)(
            best_cost=root.MIN(root.p.supplycost)
        )
        .p.WHERE(root.supplycost == root.BACK(1).best_cost)(
            s_acctbal=root.s_acctbal,
            s_name=root.s_name,
            n_name=root.n_name,
            p_partkey=root.key,
            p_mfgr=root.manufacturer,
            s_address=root.s_address,
            s_phone=root.s_phone,
            s_comment=root.s_comment,
        )
        .TOP_K(
            10,
            by=(
                root.s_acctbal.DESC(),
                root.n_name.ASC(),
                root.s_name.ASC(),
                root.p_partkey.ASC(),
            ),
        )
    )


def pydough_impl_tpch_q3(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 3.
    """
    selected_lines = root.Orders.WHERE(
        (root.customer.mktsegment == "BUILDING")
        & (root.order_date < datetime.date(1995, 3, 15))
    ).lines.WHERE(root.ship_date > datetime.date(1995, 3, 15))(
        root.BACK(1).order_date,
        root.BACK(1).ship_priority,
    )

    return root.PARTITION(
        selected_lines,
        name="l",
        by=(root.order_key, root.order_date, root.ship_priority),
    )(
        l_orderkey=root.order_key,
        revenue=root.SUM(root.l.extended_price * (1 - root.l.discount)),
        o_orderdate=root.order_date,
        o_shippriority=root.ship_priority,
    ).TOP_K(10, by=(root.revenue.DESC(), root.o_orderdate.ASC(), root.l_orderkey.ASC()))


def pydough_impl_tpch_q4(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 4.
    """
    selected_lines = root.lines.WHERE(root.commit_date < root.receipt_date)
    selected_orders = root.Orders.WHERE(
        (root.order_date >= datetime.date(1993, 7, 1))
        & (root.order_date < datetime.date(1993, 10, 1))
        & root.HAS(selected_lines)
    )
    return root.PARTITION(selected_orders, name="o", by=root.order_priority)(
        o_orderpriority=root.order_priority,
        order_count=root.COUNT(root.o),
    ).ORDER_BY(root.order_priority.ASC())


def pydough_impl_tpch_q5(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 5.
    """
    selected_lines = root.customers.orders.WHERE(
        (root.order_date >= datetime.date(1994, 1, 1))
        & (root.order_date < datetime.date(1995, 1, 1))
    ).lines.WHERE(root.supplier.nation.name == root.BACK(3).name)(
        value=root.extended_price * (1 - root.discount)
    )
    return root.Nations.WHERE(root.region.name == "ASIA")(
        n_name=root.name, revenue=root.SUM(selected_lines.value)
    ).ORDER_BY(root.revenue.DESC())


def pydough_impl_tpch_q6(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 6.
    """
    selected_lines = root.Lineitems.WHERE(
        (root.ship_date >= datetime.date(1994, 1, 1))
        & (root.ship_date < datetime.date(1995, 1, 1))
        & (0.05 <= root.discount)
        & (root.discount <= 0.07)
        & (root.quantity < 24)
    )(amt=root.extended_price * root.discount)
    return root.TPCH(revenue=root.SUM(selected_lines.amt))


def pydough_impl_tpch_q7(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 7.
    """
    line_info = root.Lineitems(
        supp_nation=root.supplier.nation.name,
        cust_nation=root.order.customer.nation.name,
        l_year=root.YEAR(root.ship_date),
        volume=root.extended_price * (1 - root.discount),
    ).WHERE(
        (root.ship_date >= datetime.date(1995, 1, 1))
        & (root.ship_date <= datetime.date(1996, 12, 31))
        & (
            ((root.supp_nation == "FRANCE") & (root.cust_nation == "GERMANY"))
            | ((root.supp_nation == "GERMANY") & (root.cust_nation == "FRANCE"))
        )
    )

    return root.PARTITION(
        line_info, name="l", by=(root.supp_nation, root.cust_nation, root.l_year)
    )(
        root.supp_nation,
        root.cust_nation,
        root.l_year,
        revenue=root.SUM(root.l.volume),
    ).ORDER_BY(
        root.supp_nation.ASC(),
        root.cust_nation.ASC(),
        root.l_year.ASC(),
    )


def pydough_impl_tpch_q8(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 8.
    """
    volume_data = (
        root.Nations.suppliers.supply_records.WHERE(
            root.part.part_type == "ECONOMY ANODIZED STEEL"
        )
        .lines(volume=root.extended_price * (1 - root.discount))
        .order(
            o_year=root.YEAR(root.order_date),
            volume=root.BACK(1).volume,
            brazil_volume=root.IFF(
                root.BACK(4).name == "BRAZIL", root.BACK(1).volume, 0
            ),
        )
        .WHERE(
            (root.order_date >= datetime.date(1995, 1, 1))
            & (root.order_date <= datetime.date(1996, 12, 31))
            & (root.customer.nation.region.name == "AMERICA")
        )
    )

    return root.PARTITION(volume_data, name="v", by=root.o_year)(
        o_year=root.o_year,
        mkt_share=root.SUM(root.v.brazil_volume) / root.SUM(root.v.volume),
    )


def pydough_impl_tpch_q9(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 9, truncated to 10 rows.
    """
    selected_lines = root.Nations.suppliers.supply_records.WHERE(
        root.CONTAINS(root.part.name, "green")
    ).lines(
        nation=root.BACK(3).name,
        o_year=root.YEAR(root.order.order_date),
        value=root.extended_price * (1 - root.discount)
        - root.BACK(1).supplycost * root.quantity,
    )
    return root.PARTITION(selected_lines, name="l", by=(root.nation, root.o_year))(
        nation=root.nation, o_year=root.o_year, amount=root.SUM(root.l.value)
    ).TOP_K(
        10,
        by=(root.nation.ASC(), root.o_year.DESC()),
    )


def pydough_impl_tpch_q10(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 10.
    """
    selected_lines = root.orders.WHERE(
        (root.order_date >= datetime.date(1993, 10, 1))
        & (root.order_date < datetime.date(1994, 1, 1))
    ).lines.WHERE(root.return_flag == "R")(
        amt=root.extended_price * (1 - root.discount)
    )
    return root.Customers(
        c_custkey=root.key,
        c_name=root.name,
        revenue=root.SUM(selected_lines.amt),
        c_acctbal=root.acctbal,
        n_name=root.nation.name,
        c_address=root.address,
        c_phone=root.phone,
        c_comment=root.comment,
    ).TOP_K(20, by=(root.revenue.DESC(), root.c_custkey.ASC()))


def pydough_impl_tpch_q11(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 11, truncated to 10 rows.
    """
    is_german_supplier = root.supplier.nation.name == "GERMANY"
    selected_records = root.PartSupp.WHERE(is_german_supplier)(
        metric=root.supplycost * root.availqty
    )
    return (
        root.TPCH(min_market_share=root.SUM(selected_records.metric) * 0.0001)
        .PARTITION(selected_records, name="ps", by=root.part_key)(
            ps_partkey=root.part_key, value=root.SUM(root.ps.metric)
        )
        .WHERE(root.value > root.BACK(1).min_market_share)
        .TOP_K(10, by=root.value.DESC())
    )


def pydough_impl_tpch_q12(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 12.
    """
    selected_lines = root.Lineitems.WHERE(
        ((root.ship_mode == "MAIL") | (root.ship_mode == "SHIP"))
        & (root.ship_date < root.commit_date)
        & (root.commit_date < root.receipt_date)
        & (root.receipt_date >= datetime.date(1994, 1, 1))
        & (root.receipt_date < datetime.date(1995, 1, 1))
    )(
        is_high_priority=(root.order.order_priority == "1-URGENT")
        | (root.order.order_priority == "2-HIGH"),
    )
    return root.PARTITION(selected_lines, "l", by=root.ship_mode)(
        l_shipmode=root.ship_mode,
        high_line_count=root.SUM(root.l.is_high_priority),
        low_line_count=root.SUM(~(root.l.is_high_priority)),
    ).ORDER_BY(root.ship_mode.ASC())


def pydough_impl_tpch_q13(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 13, truncated to 10 rows.
    """
    customer_info = root.Customers(
        root.key,
        num_non_special_orders=root.COUNT(
            root.orders.WHERE(~(root.LIKE(root.comment, "%special%requests%")))
        ),
    )
    return root.PARTITION(customer_info, name="custs", by=root.num_non_special_orders)(
        c_count=root.num_non_special_orders, custdist=root.COUNT(root.custs)
    ).TOP_K(10, by=(root.custdist.DESC(), root.c_count.DESC()))


def pydough_impl_tpch_q14(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 14.
    """
    value = root.extended_price * (1 - root.discount)
    selected_lines = root.Lineitems.WHERE(
        (root.ship_date >= datetime.date(1995, 9, 1))
        & (root.ship_date < datetime.date(1995, 10, 1))
    )(
        value=value,
        promo_value=root.IFF(root.STARTSWITH(root.part.part_type, "PROMO"), value, 0),
    )
    return root.TPCH(
        promo_revenue=100.0
        * root.SUM(selected_lines.promo_value)
        / root.SUM(selected_lines.value)
    )


def pydough_impl_tpch_q15(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 15.
    """
    selected_lines = root.lines.WHERE(
        (root.ship_date >= datetime.date(1996, 1, 1))
        & (root.ship_date < datetime.date(1996, 4, 1))
    )
    total = root.SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    return (
        root.TPCH(
            max_revenue=root.MAX(root.Suppliers(total_revenue=total).total_revenue)
        )
        .Suppliers(
            s_suppkey=root.key,
            s_name=root.name,
            s_address=root.address,
            s_phone=root.phone,
            total_revenue=total,
        )
        .WHERE(root.total_revenue == root.BACK(1).max_revenue)
        .ORDER_BY(root.s_suppkey.ASC())
    )


def pydough_impl_tpch_q16(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 16, truncated to 10 rows.
    """
    selected_records = (
        root.Parts.WHERE(
            (root.brand != "BRAND#45")
            & ~root.STARTSWITH(root.part_type, "MEDIUM POLISHED%")
            & root.ISIN(root.size, [49, 14, 23, 45, 19, 3, 36, 9])
        )
        .supply_records(
            p_brand=root.BACK(1).brand,
            p_type=root.BACK(1).part_type,
            p_size=root.BACK(1).size,
            ps_suppkey=root.supplier_key,
        )
        .WHERE(~root.LIKE(root.supplier.comment, "%Customer%Complaints%"))
    )
    return root.PARTITION(
        selected_records, name="ps", by=(root.p_brand, root.p_type, root.p_size)
    )(
        root.p_brand,
        root.p_type,
        root.p_size,
        supplier_count=root.NDISTINCT(root.ps.supplier_key),
    ).TOP_K(10, by=(root.supplier_count.DESC(), root.p_brand.ASC(), root.p_type.ASC()))


def pydough_impl_tpch_q17(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 17.
    """
    selected_lines = root.Parts.WHERE(
        (root.brand == "Brand#23") & (root.container == "MED BOX")
    )(avg_quantity=root.AVG(root.lines.quantity)).lines.WHERE(
        root.quantity < 0.2 * root.BACK(1).avg_quantity
    )
    return root.TPCH(avg_yearly=root.SUM(selected_lines.extended_price) / 7.0)


def pydough_impl_tpch_q18(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 18, truncated to 10 rows.
    """
    return (
        root.Orders(
            c_name=root.customer.name,
            c_custkey=root.customer.key,
            o_orderkey=root.key,
            o_orderdate=root.order_date,
            o_totalprice=root.total_price,
            total_quantity=root.SUM(root.lines.quantity),
        )
        .WHERE(root.total_quantity > 300)
        .TOP_K(10, by=(root.o_totalprice.DESC(), root.o_orderdate.ASC()))
    )


def pydough_impl_tpch_q19(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 19.
    """
    selected_lines = root.Lineitems.WHERE(
        (root.ISIN(root.ship_mode, ("AIR", "AIR REG")))
        & (root.ship_instruct == "DELIVER IN PERSON")
        & (root.part.size >= 1)
        & (
            (
                (root.part.size <= 5)
                & (root.quantity >= 1)
                & (root.quantity <= 11)
                & root.ISIN(
                    root.part.container,
                    ("SM CASE", "SM BOX", "SM PACK", "SM PKG"),
                )
                & (root.part.brand == "Brand#12")
            )
            | (
                (root.part.size <= 10)
                & (root.quantity >= 10)
                & (root.quantity <= 20)
                & root.ISIN(
                    root.part.container,
                    ("MED BAG", "MED BOX", "MED PACK", "MED PKG"),
                )
                & (root.part.brand == "Brand#23")
            )
            | (
                (root.part.size <= 15)
                & (root.quantity >= 20)
                & (root.quantity <= 30)
                & root.ISIN(
                    root.part.container,
                    ("LG CASE", "LG BOX", "LG PACK", "LG PKG"),
                )
                & (root.part.brand == "Brand#34")
            )
        )
    )
    return root.TPCH(
        revenue=root.SUM(selected_lines.extended_price * (1 - selected_lines.discount))
    )


def pydough_impl_tpch_q20(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 20, truncated to 10 rows.
    """
    selected_lines = root.lines.WHERE(
        (root.ship_date >= datetime.date(1994, 1, 1))
        & (root.ship_date < datetime.date(1995, 1, 1))
    )

    selected_part_supplied = root.supply_records.part.WHERE(
        root.STARTSWITH(root.name, "forest")
        & root.HAS(selected_lines)
        & (root.BACK(1).availqty > (root.SUM(selected_lines.quantity) * 0.5))
    )

    return (
        root.Suppliers(
            s_name=root.name,
            s_address=root.address,
        )
        .WHERE(
            (root.nation.name == "CANADA") & (root.COUNT(selected_part_supplied) > 0)
        )
        .TOP_K(10, by=root.s_name.ASC())
    )


def pydough_impl_tpch_q21(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 21, truncated to 10 rows.
    """
    date_check = root.receipt_date > root.commit_date
    different_supplier = root.supplier_key != root.BACK(2).supplier_key
    waiting_entries = root.lines.WHERE(
        root.receipt_date > root.commit_date
    ).order.WHERE(
        (root.order_status == "F")
        & root.HAS(root.lines.WHERE(different_supplier))
        & root.HASNOT(root.lines.WHERE(different_supplier & date_check))
    )
    return root.Suppliers.WHERE(root.nation.name == "SAUDI ARABIA")(
        s_name=root.name,
        numwait=root.COUNT(waiting_entries),
    ).TOP_K(
        10,
        by=(root.numwait.DESC(), root.s_name.ASC()),
    )


def pydough_impl_tpch_q22(root: UnqualifiedNode) -> UnqualifiedNode:
    """
    Creates an UnqualifiedNode for TPC-H query 22.
    """
    selected_customers = root.Customers(cntry_code=root.phone[:2]).WHERE(
        root.ISIN(root.cntry_code, ("13", "31", "23", "29", "30", "18", "17"))
        & root.HASNOT(root.orders)
    )
    return root.TPCH(
        avg_balance=root.AVG(selected_customers.WHERE(root.acctbal > 0.0).acctbal)
    ).PARTITION(
        selected_customers.WHERE(root.acctbal > root.BACK(1).avg_balance),
        name="custs",
        by=root.cntry_code,
    )(
        root.cntry_code,
        num_custs=root.COUNT(root.custs),
        totacctbal=root.SUM(root.custs.acctbal),
    )


@pytest.mark.parametrize(
    "impl, answer_tree_str",
    [
        pytest.param(
            pydough_impl_misc_01,
            """
──┬─ TPCH
  ├─── TableCollection[Nations]
  └─┬─ Calc[nation_name=name, total_balance=SUM($1.acctbal)]
    └─┬─ AccessChild
      └─── SubCollection[customers]
""",
            id="misc_01",
        ),
        pytest.param(
            pydough_impl_misc_02,
            """
──┬─ TPCH
  └─┬─ TableCollection[Nations]
    ├─── SubCollection[customers]
    └─┬─ Calc[name=LOWER(name), nation_name=BACK(1).name, total_1994=SUM($1.extended_price - ($1.tax / 2)), total_1995=SUM($2.extended_price - ($2.tax / 2))]
      ├─┬─ AccessChild
      │ ├─── SubCollection[orders]
      │ └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]
      │   └─── SubCollection[lines]
      └─┬─ AccessChild
        ├─── SubCollection[orders]
        └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date < datetime.date(1996, 1, 1))]
          └─── SubCollection[lines]
""",
            id="misc_02",
        ),
        pytest.param(
            pydough_impl_misc_03,
            """
┌─── TPCH
├─┬─ Calc[avg_n_parts=AVG($1.n_parts)]
│ └─┬─ AccessChild
│   ├─┬─ Partition[name='p', by=size]
│   │ └─┬─ AccessChild
│   │   └─── TableCollection[Parts]
│   └─┬─ Calc[n_parts=COUNT($1)]
│     └─┬─ AccessChild
│       └─── PartitionChild[p]
└─┬─ Calc[n_parts=COUNT($1)]
  └─┬─ AccessChild
    ├─┬─ Partition[name='p', by=size]
    │ └─┬─ AccessChild
    │   └─── TableCollection[Parts]
    ├─┬─ Calc[n_parts=COUNT($1)]
    │ └─┬─ AccessChild
    │   └─── PartitionChild[p]
    └─── Where[n_parts > BACK(1).avg_n_parts]
""",
            id="misc_03",
        ),
        pytest.param(
            pydough_impl_tpch_q1,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(return_flag, status)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   └─── Where[ship_date <= datetime.date(1998, 12, 1)]
  ├─┬─ Calc[l_returnflag=return_flag, l_linestatus=status, sum_qty=SUM($1.quantity), sum_base_price=SUM($1.extended_price), sum_disc_price=SUM($1.extended_price * (1 - $1.discount)), sum_charge=SUM(($1.extended_price * (1 - $1.discount)) * (1 + $1.tax)), avg_qty=AVG($1.quantity), avg_price=AVG($1.extended_price), avg_disc=AVG($1.discount), count_order=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[return_flag.ASC(na_pos='first'), status.ASC(na_pos='first')]
""",
            id="tpch_q1",
        ),
        pytest.param(
            pydough_impl_tpch_q2,
            """
──┬─ TPCH
  ├─┬─ Partition[name='p', by=key]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Nations]
  │   └─┬─ Where[$1.name == 'EUROPE']
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[region]
  │     └─┬─ SubCollection[suppliers]
  │       └─┬─ SubCollection[supply_records]
  │         ├─── SubCollection[part]
  │         ├─── Calc[s_acctbal=BACK(2).account_balance, s_name=BACK(2).name, n_name=BACK(3).name, s_address=BACK(2).address, s_phone=BACK(2).phone, s_comment=BACK(2).comment, supplycost=BACK(1).supplycost]
  │         └─── Where[ENDSWITH(part_type, 'BRASS') & (size == 15)]
  └─┬─ Calc[best_cost=MIN($1.supplycost)]
    ├─┬─ AccessChild
    │ └─── PartitionChild[p]
    ├─── PartitionChild[p]
    ├─── Where[supplycost == BACK(1).best_cost]
    ├─── Calc[s_acctbal=s_acctbal, s_name=s_name, n_name=n_name, p_partkey=key, p_mfgr=manufacturer, s_address=s_address, s_phone=s_phone, s_comment=s_comment]
    └─── TopK[10, s_acctbal.DESC(na_pos='last'), n_name.ASC(na_pos='first'), s_name.ASC(na_pos='first'), p_partkey.ASC(na_pos='first')]
""",
            id="tpch_q2",
        ),
        pytest.param(
            pydough_impl_tpch_q3,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(order_key, order_date, ship_priority)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Orders]
  │   └─┬─ Where[($1.mktsegment == 'BUILDING') & (order_date < datetime.date(1995, 3, 15))]
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[customer]
  │     ├─── SubCollection[lines]
  │     ├─── Where[ship_date > datetime.date(1995, 3, 15)]
  │     └─── Calc[order_date=BACK(1).order_date, ship_priority=BACK(1).ship_priority]
  ├─┬─ Calc[l_orderkey=order_key, revenue=SUM($1.extended_price * (1 - $1.discount)), o_orderdate=order_date, o_shippriority=ship_priority]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, revenue.DESC(na_pos='last'), o_orderdate.ASC(na_pos='first'), l_orderkey.ASC(na_pos='first')]
""",
            id="tpch_q3",
        ),
        pytest.param(
            pydough_impl_tpch_q4,
            """
──┬─ TPCH
  ├─┬─ Partition[name='o', by=order_priority]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Orders]
  │   └─┬─ Where[(order_date >= datetime.date(1993, 7, 1)) & (order_date < datetime.date(1993, 10, 1)) & HAS($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[lines]
  │       └─── Where[commit_date < receipt_date]
  ├─┬─ Calc[o_orderpriority=order_priority, order_count=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[o]
  └─── OrderBy[order_priority.ASC(na_pos='first')]
""",
            id="tpch_q4",
        ),
        pytest.param(
            pydough_impl_tpch_q5,
            """
──┬─ TPCH
  ├─── TableCollection[Nations]
  ├─┬─ Where[$1.name == 'ASIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[region]
  ├─┬─ Calc[n_name=name, revenue=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[customers]
  │     ├─── SubCollection[orders]
  │     └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]
  │       ├─── SubCollection[lines]
  │       ├─┬─ Where[$1.name == BACK(3).name]
  │       │ └─┬─ AccessChild
  │       │   └─┬─ SubCollection[supplier]
  │       │     └─── SubCollection[nation]
  │       └─── Calc[value=extended_price * (1 - discount)]
  └─── OrderBy[revenue.DESC(na_pos='last')]
""",
            id="tpch_q5",
        ),
        pytest.param(
            pydough_impl_tpch_q6,
            """
┌─── TPCH
└─┬─ Calc[revenue=SUM($1.amt)]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    ├─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1)) & (discount >= 0.05) & (discount <= 0.07) & (quantity < 24)]
    └─── Calc[amt=extended_price * discount]
""",
            id="tpch_q6",
        ),
        pytest.param(
            pydough_impl_tpch_q7,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(supp_nation, cust_nation, l_year)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   ├─┬─ Calc[supp_nation=$1.name, cust_nation=$2.name, l_year=YEAR(ship_date), volume=extended_price * (1 - discount)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─┬─ SubCollection[supplier]
  │   │ │   └─── SubCollection[nation]
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[order]
  │   │     └─┬─ SubCollection[customer]
  │   │       └─── SubCollection[nation]
  │   └─── Where[(ship_date >= datetime.date(1995, 1, 1)) & (ship_date <= datetime.date(1996, 12, 31)) & (((supp_nation == 'FRANCE') & (cust_nation == 'GERMANY')) | ((supp_nation == 'GERMANY') & (cust_nation == 'FRANCE')))]
  ├─┬─ Calc[supp_nation=supp_nation, cust_nation=cust_nation, l_year=l_year, revenue=SUM($1.volume)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[supp_nation.ASC(na_pos='first'), cust_nation.ASC(na_pos='first'), l_year.ASC(na_pos='first')]
""",
            id="tpch_q7",
        ),
        pytest.param(
            pydough_impl_tpch_q8,
            """
──┬─ TPCH
  ├─┬─ Partition[name='v', by=o_year]
  │ └─┬─ AccessChild
  │   └─┬─ TableCollection[Nations]
  │     └─┬─ SubCollection[suppliers]
  │       ├─── SubCollection[supply_records]
  │       └─┬─ Where[$1.part_type == 'ECONOMY ANODIZED STEEL']
  │         ├─┬─ AccessChild
  │         │ └─── SubCollection[part]
  │         ├─── SubCollection[lines]
  │         └─┬─ Calc[volume=extended_price * (1 - discount)]
  │           ├─── SubCollection[order]
  │           ├─── Calc[o_year=YEAR(order_date), volume=BACK(1).volume, brazil_volume=IFF(BACK(4).name == 'BRAZIL', BACK(1).volume, 0)]
  │           └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date <= datetime.date(1996, 12, 31)) & ($1.name == 'AMERICA')]
  │             └─┬─ AccessChild
  │               └─┬─ SubCollection[customer]
  │                 └─┬─ SubCollection[nation]
  │                   └─── SubCollection[region]
  └─┬─ Calc[o_year=o_year, mkt_share=SUM($1.brazil_volume) / SUM($1.volume)]
    └─┬─ AccessChild
      └─── PartitionChild[v]
""",
            id="tpch_q8",
        ),
        pytest.param(
            pydough_impl_tpch_q9,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(nation, o_year)]
  │ └─┬─ AccessChild
  │   └─┬─ TableCollection[Nations]
  │     └─┬─ SubCollection[suppliers]
  │       ├─── SubCollection[supply_records]
  │       └─┬─ Where[CONTAINS($1.name, 'green')]
  │         ├─┬─ AccessChild
  │         │ └─── SubCollection[part]
  │         ├─── SubCollection[lines]
  │         └─┬─ Calc[nation=BACK(3).name, o_year=YEAR($1.order_date), value=(extended_price * (1 - discount)) - (BACK(1).supplycost * quantity)]
  │           └─┬─ AccessChild
  │             └─── SubCollection[order]
  ├─┬─ Calc[nation=nation, o_year=o_year, amount=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, nation.ASC(na_pos='first'), o_year.DESC(na_pos='last')]
""",
            id="tpch_q9",
        ),
        pytest.param(
            pydough_impl_tpch_q10,
            """
──┬─ TPCH
  ├─── TableCollection[Customers]
  ├─┬─ Calc[c_custkey=key, c_name=name, revenue=SUM($1.amt), c_acctbal=acctbal, n_name=$2.name, c_address=address, c_phone=phone, c_comment=comment]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[orders]
  │ │ └─┬─ Where[(order_date >= datetime.date(1993, 10, 1)) & (order_date < datetime.date(1994, 1, 1))]
  │ │   ├─── SubCollection[lines]
  │ │   ├─── Where[return_flag == 'R']
  │ │   └─── Calc[amt=extended_price * (1 - discount)]
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  └─── TopK[20, revenue.DESC(na_pos='last'), c_custkey.ASC(na_pos='first')]
""",
            id="tpch_q10",
        ),
        pytest.param(
            pydough_impl_tpch_q11,
            """
┌─── TPCH
└─┬─ Calc[min_market_share=SUM($1.metric) * 0.0001]
  ├─┬─ AccessChild
  │ ├─── TableCollection[PartSupp]
  │ ├─┬─ Where[$1.name == 'GERMANY']
  │ │ └─┬─ AccessChild
  │ │   └─┬─ SubCollection[supplier]
  │ │     └─── SubCollection[nation]
  │ └─── Calc[metric=supplycost * availqty]
  ├─┬─ Partition[name='ps', by=part_key]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[PartSupp]
  │   ├─┬─ Where[$1.name == 'GERMANY']
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[supplier]
  │   │     └─── SubCollection[nation]
  │   └─── Calc[metric=supplycost * availqty]
  ├─┬─ Calc[ps_partkey=part_key, value=SUM($1.metric)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  ├─── Where[value > BACK(1).min_market_share]
  └─── TopK[10, value.DESC(na_pos='last')]
""",
            id="tpch_q11",
        ),
        pytest.param(
            pydough_impl_tpch_q12,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=ship_mode]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   ├─── Where[((ship_mode == 'MAIL') | (ship_mode == 'SHIP')) & (ship_date < commit_date) & (commit_date < receipt_date) & (receipt_date >= datetime.date(1994, 1, 1)) & (receipt_date < datetime.date(1995, 1, 1))]
  │   └─┬─ Calc[is_high_priority=($1.order_priority == '1-URGENT') | ($1.order_priority == '2-HIGH')]
  │     └─┬─ AccessChild
  │       └─── SubCollection[order]
  ├─┬─ Calc[l_shipmode=ship_mode, high_line_count=SUM($1.is_high_priority), low_line_count=SUM(NOT($1.is_high_priority))]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[ship_mode.ASC(na_pos='first')]
""",
            id="tpch_q12",
        ),
        pytest.param(
            pydough_impl_tpch_q13,
            """
──┬─ TPCH
  ├─┬─ Partition[name='custs', by=num_non_special_orders]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Customers]
  │   └─┬─ Calc[key=key, num_non_special_orders=COUNT($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[orders]
  │       └─── Where[NOT(LIKE(comment, '%special%requests%'))]
  ├─┬─ Calc[c_count=num_non_special_orders, custdist=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[custs]
  └─── TopK[10, custdist.DESC(na_pos='last'), c_count.DESC(na_pos='last')]
""",
            id="tpch_q13",
        ),
        pytest.param(
            pydough_impl_tpch_q14,
            """
┌─── TPCH
└─┬─ Calc[promo_revenue=(100.0 * SUM($1.promo_value)) / SUM($1.value)]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    ├─── Where[(ship_date >= datetime.date(1995, 9, 1)) & (ship_date < datetime.date(1995, 10, 1))]
    └─┬─ Calc[value=extended_price * (1 - discount), promo_value=IFF(STARTSWITH($1.part_type, 'PROMO'), extended_price * (1 - discount), 0)]
      └─┬─ AccessChild
        └─── SubCollection[part]
""",
            id="tpch_q14",
        ),
        pytest.param(
            pydough_impl_tpch_q15,
            """
┌─── TPCH
└─┬─ Calc[max_revenue=MAX($1.total_revenue)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[Suppliers]
  │ └─┬─ Calc[total_revenue=SUM($1.extended_price * (1 - $1.discount))]
  │   └─┬─ AccessChild
  │     ├─── SubCollection[lines]
  │     └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── TableCollection[Suppliers]
  ├─┬─ Calc[s_suppkey=key, s_name=name, s_address=address, s_phone=phone, total_revenue=SUM($1.extended_price * (1 - $1.discount))]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── Where[total_revenue == BACK(1).max_revenue]
  └─── OrderBy[s_suppkey.ASC(na_pos='first')]
""",
            id="tpch_q15",
        ),
        pytest.param(
            pydough_impl_tpch_q16,
            """
──┬─ TPCH
  ├─┬─ Partition[name='ps', by=(p_brand, p_type, p_size)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Parts]
  │   └─┬─ Where[(brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%')) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])]
  │     ├─── SubCollection[supply_records]
  │     ├─── Calc[p_brand=BACK(1).brand, p_type=BACK(1).part_type, p_size=BACK(1).size, ps_suppkey=supplier_key]
  │     └─┬─ Where[NOT(LIKE($1.comment, '%Customer%Complaints%'))]
  │       └─┬─ AccessChild
  │         └─── SubCollection[supplier]
  ├─┬─ Calc[p_brand=p_brand, p_type=p_type, p_size=p_size, supplier_count=NDISTINCT($1.supplier_key)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  └─── TopK[10, supplier_count.DESC(na_pos='last'), p_brand.ASC(na_pos='first'), p_type.ASC(na_pos='first')]
""",
            id="tpch_q16",
        ),
        pytest.param(
            pydough_impl_tpch_q17,
            """
┌─── TPCH
└─┬─ Calc[avg_yearly=SUM($1.extended_price) / 7.0]
  └─┬─ AccessChild
    ├─── TableCollection[Parts]
    ├─── Where[(brand == 'Brand#23') & (container == 'MED BOX')]
    └─┬─ Calc[avg_quantity=AVG($1.quantity)]
      ├─┬─ AccessChild
      │ └─── SubCollection[lines]
      ├─── SubCollection[lines]
      └─── Where[quantity < (0.2 * BACK(1).avg_quantity)]
""",
            id="tpch_q17",
        ),
        pytest.param(
            pydough_impl_tpch_q18,
            """
──┬─ TPCH
  ├─── TableCollection[Orders]
  ├─┬─ Calc[c_name=$1.name, c_custkey=$1.key, o_orderkey=key, o_orderdate=order_date, o_totalprice=total_price, total_quantity=SUM($2.quantity)]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[customer]
  │ └─┬─ AccessChild
  │   └─── SubCollection[lines]
  ├─── Where[total_quantity > 300]
  └─── TopK[10, o_totalprice.DESC(na_pos='last'), o_orderdate.ASC(na_pos='first')]
""",
            id="tpch_q18",
        ),
        pytest.param(
            pydough_impl_tpch_q19,
            """
┌─── TPCH
└─┬─ Calc[revenue=SUM($1.extended_price * (1 - $1.discount))]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    └─┬─ Where[ISIN(ship_mode, ['AIR', 'AIR REG']) & (ship_instruct == 'DELIVER IN PERSON') & ($1.size >= 1) & (((($1.size <= 5) & (quantity >= 1) & (quantity <= 11) & ISIN($1.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) & ($1.brand == 'Brand#12')) | (($1.size <= 10) & (quantity >= 10) & (quantity <= 20) & ISIN($1.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG']) & ($1.brand == 'Brand#23'))) | (($1.size <= 15) & (quantity >= 20) & (quantity <= 30) & ISIN($1.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']) & ($1.brand == 'Brand#34')))]
      └─┬─ AccessChild
        └─── SubCollection[part]
""",
            id="tpch_q19",
        ),
        pytest.param(
            pydough_impl_tpch_q20,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─── Calc[s_name=name, s_address=address]
  ├─┬─ Where[($1.name == 'CANADA') & (COUNT($2) > 0)]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[nation]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[supply_records]
  │     ├─── SubCollection[part]
  │     └─┬─ Where[STARTSWITH(name, 'forest') & HAS($1) & (BACK(1).availqty > (SUM($1.quantity) * 0.5))]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1))]
  └─── TopK[10, s_name.ASC(na_pos='first')]
""",
            id="tpch_q20",
        ),
        pytest.param(
            pydough_impl_tpch_q21,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Where[$1.name == 'SAUDI ARABIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  ├─┬─ Calc[s_name=name, numwait=COUNT($1)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─┬─ Where[receipt_date > commit_date]
  │     ├─── SubCollection[order]
  │     └─┬─ Where[(order_status == 'F') & HAS($1) & HASNOT($2)]
  │       ├─┬─ AccessChild
  │       │ ├─── SubCollection[lines]
  │       │ └─── Where[supplier_key != BACK(2).supplier_key]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(supplier_key != BACK(2).supplier_key) & (receipt_date > commit_date)]
  └─── TopK[10, numwait.DESC(na_pos='last'), s_name.ASC(na_pos='first')]
""",
            id="tpch_q21",
        ),
        pytest.param(
            pydough_impl_tpch_q22,
            """
┌─── TPCH
└─┬─ Calc[avg_balance=AVG($1.acctbal)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[Customers]
  │ ├─── Calc[cntry_code=SLICE(phone, None, 2, None)]
  │ ├─┬─ Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT($1)]
  │ │ └─┬─ AccessChild
  │ │   └─── SubCollection[orders]
  │ └─── Where[acctbal > 0.0]
  ├─┬─ Partition[name='custs', by=cntry_code]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Customers]
  │   ├─── Calc[cntry_code=SLICE(phone, None, 2, None)]
  │   ├─┬─ Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT($1)]
  │   │ └─┬─ AccessChild
  │   │   └─── SubCollection[orders]
  │   └─── Where[acctbal > BACK(1).avg_balance]
  └─┬─ Calc[cntry_code=cntry_code, num_custs=COUNT($1), totacctbal=SUM($1.acctbal)]
    └─┬─ AccessChild
      └─── PartitionChild[custs]
""",
            id="tpch_q22",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    answer_tree_str: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = impl(root)
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    assert (
        qualified.to_tree_string() == answer_tree_str.strip()
    ), "Mismatch between tree string representation of qualified node and expected QDAG tree string"
