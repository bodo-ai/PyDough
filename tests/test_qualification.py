"""
TODO: add file-level docstring.
"""

import datetime
from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.metadata import GraphMetadata
from pydough.pydough_ast import PyDoughCollectionAST
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
    selected_parts = root.Nations.WHERE(
        root.region.name == "EUROPE"
    ).suppliers.parts_supplied(
        s_acctbal=root.BACK(1).account_balance,
        s_name=root.BACK(1).name,
        n_name=root.BACK(2).name,
        p_partkey=root.key,
        p_mfgr=root.manufacturer,
        s_address=root.BACK(1).address,
        s_phone=root.BACK(1).phone,
        s_comment=root.BACK(1).comment,
    )

    return (
        root.PARTITION(selected_parts, name="p", by=root.key)(
            best_cost=root.MIN(root.p.ps_supplycost)
        )
        .p.WHERE(
            (root.ps_supplycost == root.BACK(1).best_cost)
            & root.ENDSWITH(root.part_type, "BRASS")
            & (root.size == 15)
        )
        .ORDER_BY(
            root.s_acctbal.DESC(),
            root.n_name.ASC(),
            root.s_name.ASC(),
            root.p_partkey.ASC(),
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
        & (root.COUNT(selected_lines) > 0)
    )
    return root.PARTITION(selected_orders, name="o", by=root.order_priority)(
        root.order_priority,
        order_count=root.COUNT(root.o),
    ).ORDER_BY(root.order_priority.ASC())


@pytest.mark.parametrize(
    "impl, answer_tree_str",
    [
        pytest.param(
            pydough_impl_misc_01,
            "──┬─ TPCH\n"
            "  ├─── TableCollection[Nations]\n"
            "  └─┬─ Calc[nation_name=name, total_balance=SUM($1.acctbal)]\n"
            "    └─┬─ AccessChild\n"
            "      └─── SubCollection[customers]",
            id="misc-01",
        ),
        pytest.param(
            pydough_impl_misc_02,
            "──┬─ TPCH\n"
            "  └─┬─ TableCollection[Nations]\n"
            "    ├─── SubCollection[customers]\n"
            "    └─┬─ Calc[name=LOWER(name), nation_name=BACK(1).name, total_1994=SUM($1.extended_price - ($1.tax / 2)), total_1995=SUM($2.extended_price - ($2.tax / 2))]\n"
            "      ├─┬─ AccessChild\n"
            "      │ ├─── SubCollection[orders]\n"
            "      │ └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]\n"
            "      │   └─── SubCollection[lines]\n"
            "      └─┬─ AccessChild\n"
            "        ├─── SubCollection[orders]\n"
            "        └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date < datetime.date(1996, 1, 1))]\n"
            "          └─── SubCollection[lines]",
            id="misc-02",
        ),
        pytest.param(
            pydough_impl_tpch_q1,
            "┌─── TPCH\n"
            "├─┬─ Partition[name='l', by=('return_flag', 'status')]\n"
            "│ └─┬─ AccessChild\n"
            "│   ├─── TableCollection[Lineitems]\n"
            "│   └─── Where[ship_date <= datetime.date(1998, 12, 1)]\n"
            "├─┬─ Calc[l_returnflag=return_flag, l_linestatus=status, sum_qty=SUM($1.quantity), sum_base_price=SUM($1.extended_price), sum_disc_price=SUM($1.extended_price * (1 - $1.discount)), sum_charge=SUM(($1.extended_price * (1 - $1.discount)) * (1 + $1.tax)), avg_qty=AVG($1.quantity), avg_price=AVG($1.extended_price), avg_disc=AVG($1.discount), count_order=COUNT($1)]\n"
            "│ └─┬─ AccessChild\n"
            "│   └─── PartitionChild[l]\n"
            "└─── OrderBy[return_flag.ASC(na_pos='last'), status.ASC(na_pos='last')]",
            id="tpch-q1",
        ),
        pytest.param(
            pydough_impl_tpch_q2,
            "┌─── TPCH\n"
            "├─┬─ Partition[name='p', by=key]\n"
            "│ └─┬─ AccessChild\n"
            "│   ├─── TableCollection[Nations]\n"
            "│   └─┬─ Where[$1.name == 'EUROPE']\n"
            "│     ├─┬─ AccessChild\n"
            "│     │ └─── SubCollection[region]\n"
            "│     └─┬─ SubCollection[suppliers]\n"
            "│       ├─── SubCollection[parts_supplied]\n"
            "│       └─── Calc[s_acctbal=BACK(1).account_balance, s_name=BACK(1).name, n_name=BACK(2).name, p_partkey=key, p_mfgr=manufacturer, s_address=BACK(1).address, s_phone=BACK(1).phone, s_comment=BACK(1).comment]\n"
            "└─┬─ Calc[best_cost=MIN($1.ps_supplycost)]\n"
            "  ├─┬─ AccessChild\n"
            "  │ └─── PartitionChild[p]\n"
            "  ├─── PartitionChild[p]\n"
            "  ├─── Where[((ps_supplycost == BACK(1).best_cost) & ENDSWITH(part_type, 'BRASS')) & (size == 15)]\n"
            "  └─── OrderBy[s_acctbal.DESC(na_pos='last'), n_name.ASC(na_pos='last'), s_name.ASC(na_pos='last'), p_partkey.ASC(na_pos='last')]",
            id="tpch-q2",
        ),
        pytest.param(
            pydough_impl_tpch_q3,
            "┌─── TPCH\n"
            "├─┬─ Partition[name='l', by=('order_key', 'order_date', 'ship_priority')]\n"
            "│ └─┬─ AccessChild\n"
            "│   ├─── TableCollection[Orders]\n"
            "│   └─┬─ Where[($1.mktsegment == 'BUILDING') & (order_date < datetime.date(1995, 3, 15))]\n"
            "│     ├─┬─ AccessChild\n"
            "│     │ └─── SubCollection[customer]\n"
            "│     ├─── SubCollection[lines]\n"
            "│     ├─── Where[ship_date > datetime.date(1995, 3, 15)]\n"
            "│     └─── Calc[order_date=BACK(1).order_date, ship_priority=BACK(1).ship_priority]\n"
            "├─┬─ Calc[l_orderkey=order_key, revenue=SUM($1.extended_price * (1 - $1.discount)), o_orderdate=order_date, o_shippriority=ship_priority]\n"
            "│ └─┬─ AccessChild\n"
            "│   └─── PartitionChild[l]\n"
            "└─── TopK[10, revenue.DESC(na_pos='last'), o_orderdate.ASC(na_pos='last'), l_orderkey.ASC(na_pos='last')]",
            id="tpch-q3",
        ),
        pytest.param(
            pydough_impl_tpch_q4,
            "┌─── TPCH\n"
            "├─┬─ Partition[name='o', by=order_priority]\n"
            "│ └─┬─ AccessChild\n"
            "│   ├─── TableCollection[Orders]\n"
            "│   └─┬─ Where[((order_date >= datetime.date(1993, 7, 1)) & (order_date < datetime.date(1993, 10, 1))) & (COUNT($1) > 0)]\n"
            "│     └─┬─ AccessChild\n"
            "│       ├─── SubCollection[lines]\n"
            "│       └─── Where[commit_date < receipt_date]\n"
            "├─┬─ Calc[order_priority=order_priority, order_count=COUNT($1)]\n"
            "│ └─┬─ AccessChild\n"
            "│   └─── PartitionChild[o]\n"
            "└─── OrderBy[order_priority.ASC(na_pos='last')]",
            id="tpch-q4",
        ),
        # pytest.param(
        #     pydough_impl_tpch_q5,
        #     "",
        #     id="tpch-q5",
        # ),
        # pytest.param(
        #     pydough_impl_tpch_q6,
        #     "",
        #     id="tpch-q6",
        # ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[UnqualifiedNode], UnqualifiedNode],
    answer_tree_str: str,
    get_sample_graph: graph_fetcher,
):
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = impl(root)
    qualified: PyDoughCollectionAST = qualify_node(unqualified, graph)
    assert (
        qualified.to_tree_string() == answer_tree_str
    ), "Mismatch between tree string representation of qualified node and expected AST tree string"
