"""
Unit tests the PyDough qualification process that transforms unqualified nodes
into qualified DAG nodes.
"""

from collections.abc import Callable

import pytest
from test_utils import (
    graph_fetcher,
)
from tpch_test_functions import (
    impl_tpch_q1,
    impl_tpch_q2,
    impl_tpch_q3,
    impl_tpch_q4,
    impl_tpch_q5,
    impl_tpch_q6,
    impl_tpch_q7,
    impl_tpch_q8,
    impl_tpch_q9,
    impl_tpch_q10,
    impl_tpch_q11,
    impl_tpch_q12,
    impl_tpch_q13,
    impl_tpch_q14,
    impl_tpch_q15,
    impl_tpch_q16,
    impl_tpch_q17,
    impl_tpch_q18,
    impl_tpch_q19,
    impl_tpch_q20,
    impl_tpch_q21,
    impl_tpch_q22,
)

from pydough import init_pydough_context
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.unqualified import (
    UnqualifiedNode,
    qualify_node,
)


@pytest.mark.parametrize(
    "impl, answer_tree_str",
    [
        pytest.param(
            impl_tpch_q1,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(return_flag, status)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   └─── Where[ship_date <= datetime.date(1998, 12, 1)]
  ├─┬─ Calculate[L_RETURNFLAG=return_flag, L_LINESTATUS=status, SUM_QTY=SUM($1.quantity), SUM_BASE_PRICE=SUM($1.extended_price), SUM_DISC_PRICE=SUM($1.extended_price * (1 - $1.discount)), SUM_CHARGE=SUM(($1.extended_price * (1 - $1.discount)) * (1 + $1.tax)), AVG_QTY=AVG($1.quantity), AVG_PRICE=AVG($1.extended_price), AVG_DISC=AVG($1.discount), COUNT_ORDER=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[L_RETURNFLAG.ASC(na_pos='first'), L_LINESTATUS.ASC(na_pos='first')]
""",
            id="tpch_q1",
        ),
        pytest.param(
            impl_tpch_q2,
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
  │         ├─── Calculate[s_acctbal=account_balance, s_name=name, n_name=name, s_address=address, s_phone=phone, s_comment=comment, supplycost=supplycost]
  │         └─── Where[ENDSWITH(part_type, 'BRASS') & (size == 15)]
  └─┬─ Calculate[best_cost=MIN($1.supplycost)]
    ├─┬─ AccessChild
    │ └─── PartitionChild[p]
    ├─── PartitionChild[p]
    ├─── Where[supplycost == best_cost]
    ├─── Calculate[s_acctbal=s_acctbal, s_name=s_name, n_name=n_name, p_partkey=key, p_mfgr=manufacturer, s_address=s_address, s_phone=s_phone, s_comment=s_comment]
    └─── TopK[10, s_acctbal.DESC(na_pos='last'), n_name.ASC(na_pos='first'), s_name.ASC(na_pos='first'), p_partkey.ASC(na_pos='first')]
""",
            id="tpch_q2",
        ),
        pytest.param(
            impl_tpch_q3,
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
  │     └─── Calculate[order_date=order_date, ship_priority=ship_priority]
  ├─┬─ Calculate[l_orderkey=order_key, revenue=SUM($1.extended_price * (1 - $1.discount)), o_orderdate=order_date, o_shippriority=ship_priority]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, revenue.DESC(na_pos='last'), o_orderdate.ASC(na_pos='first'), l_orderkey.ASC(na_pos='first')]
""",
            id="tpch_q3",
        ),
        pytest.param(
            impl_tpch_q4,
            """
──┬─ TPCH
  ├─┬─ Partition[name='o', by=order_priority]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Orders]
  │   └─┬─ Where[(order_date >= datetime.date(1993, 7, 1)) & (order_date < datetime.date(1993, 10, 1)) & HAS($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[lines]
  │       └─── Where[commit_date < receipt_date]
  ├─┬─ Calculate[O_ORDERPRIORITY=order_priority, ORDER_COUNT=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[o]
  └─── OrderBy[O_ORDERPRIORITY.ASC(na_pos='first')]
""",
            id="tpch_q4",
        ),
        pytest.param(
            impl_tpch_q5,
            """
──┬─ TPCH
  ├─── TableCollection[Nations]
  ├─┬─ Where[$1.name == 'ASIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[region]
  ├─┬─ Calculate[N_NAME=name, REVENUE=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[customers]
  │     ├─── SubCollection[orders]
  │     └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]
  │       ├─── SubCollection[lines]
  │       ├─┬─ Where[$1.name == name]
  │       │ └─┬─ AccessChild
  │       │   └─┬─ SubCollection[supplier]
  │       │     └─── SubCollection[nation]
  │       └─── Calculate[value=extended_price * (1 - discount)]
  └─── OrderBy[REVENUE.DESC(na_pos='last')]
""",
            id="tpch_q5",
        ),
        pytest.param(
            impl_tpch_q6,
            """
┌─── TPCH
└─┬─ Calculate[REVENUE=SUM($1.amt)]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    ├─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1)) & (discount >= 0.05) & (discount <= 0.07) & (quantity < 24)]
    └─── Calculate[amt=extended_price * discount]
""",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q7,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(supp_nation, cust_nation, l_year)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   ├─┬─ Calculate[supp_nation=$1.name, cust_nation=$2.name, l_year=YEAR(ship_date), volume=extended_price * (1 - discount)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─┬─ SubCollection[supplier]
  │   │ │   └─── SubCollection[nation]
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[order]
  │   │     └─┬─ SubCollection[customer]
  │   │       └─── SubCollection[nation]
  │   └─── Where[(ship_date >= datetime.date(1995, 1, 1)) & (ship_date <= datetime.date(1996, 12, 31)) & (((supp_nation == 'FRANCE') & (cust_nation == 'GERMANY')) | ((supp_nation == 'GERMANY') & (cust_nation == 'FRANCE')))]
  ├─┬─ Calculate[SUPP_NATION=supp_nation, CUST_NATION=cust_nation, L_YEAR=l_year, REVENUE=SUM($1.volume)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[SUPP_NATION.ASC(na_pos='first'), CUST_NATION.ASC(na_pos='first'), L_YEAR.ASC(na_pos='first')]
""",
            id="tpch_q7",
        ),
        pytest.param(
            impl_tpch_q8,
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
  │         └─┬─ Calculate[volume=extended_price * (1 - discount)]
  │           ├─── SubCollection[order]
  │           ├─── Calculate[o_year=YEAR(order_date), volume=volume, brazil_volume=IFF(name == 'BRAZIL', volume, 0)]
  │           └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date <= datetime.date(1996, 12, 31)) & ($1.name == 'AMERICA')]
  │             └─┬─ AccessChild
  │               └─┬─ SubCollection[customer]
  │                 └─┬─ SubCollection[nation]
  │                   └─── SubCollection[region]
  └─┬─ Calculate[o_year=o_year, mkt_share=SUM($1.brazil_volume) / SUM($1.volume)]
    └─┬─ AccessChild
      └─── PartitionChild[v]
""",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
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
  │         └─┬─ Calculate[nation=name, o_year=YEAR($1.order_date), value=(extended_price * (1 - discount)) - (supplycost * quantity)]
  │           └─┬─ AccessChild
  │             └─── SubCollection[order]
  ├─┬─ Calculate[nation=nation, o_year=o_year, amount=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, nation.ASC(na_pos='first'), o_year.DESC(na_pos='last')]
""",
            id="tpch_q9",
        ),
        pytest.param(
            impl_tpch_q10,
            """
──┬─ TPCH
  ├─── TableCollection[Customers]
  ├─┬─ Calculate[C_CUSTKEY=key, C_NAME=name, REVENUE=SUM($1.amt), C_ACCTBAL=acctbal, N_NAME=$2.name, C_ADDRESS=address, C_PHONE=phone, C_COMMENT=comment]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[orders]
  │ │ └─┬─ Where[(order_date >= datetime.date(1993, 10, 1)) & (order_date < datetime.date(1994, 1, 1))]
  │ │   ├─── SubCollection[lines]
  │ │   ├─── Where[return_flag == 'R']
  │ │   └─── Calculate[amt=extended_price * (1 - discount)]
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  └─── TopK[20, REVENUE.DESC(na_pos='last'), C_CUSTKEY.ASC(na_pos='first')]
""",
            id="tpch_q10",
        ),
        pytest.param(
            impl_tpch_q11,
            """
┌─── TPCH
└─┬─ Calculate[min_market_share=SUM($1.metric) * 0.0001]
  ├─┬─ AccessChild
  │ ├─── TableCollection[PartSupp]
  │ ├─┬─ Where[$1.name == 'GERMANY']
  │ │ └─┬─ AccessChild
  │ │   └─┬─ SubCollection[supplier]
  │ │     └─── SubCollection[nation]
  │ └─── Calculate[metric=supplycost * availqty]
  ├─┬─ Partition[name='ps', by=part_key]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[PartSupp]
  │   ├─┬─ Where[$1.name == 'GERMANY']
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[supplier]
  │   │     └─── SubCollection[nation]
  │   └─── Calculate[metric=supplycost * availqty]
  ├─┬─ Calculate[ps_partkey=part_key, value=SUM($1.metric)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  ├─── Where[value > min_market_share]
  └─── TopK[10, value.DESC(na_pos='last')]
""",
            id="tpch_q11",
        ),
        pytest.param(
            impl_tpch_q12,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=ship_mode]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Lineitems]
  │   ├─── Where[((ship_mode == 'MAIL') | (ship_mode == 'SHIP')) & (ship_date < commit_date) & (commit_date < receipt_date) & (receipt_date >= datetime.date(1994, 1, 1)) & (receipt_date < datetime.date(1995, 1, 1))]
  │   └─┬─ Calculate[is_high_priority=($1.order_priority == '1-URGENT') | ($1.order_priority == '2-HIGH')]
  │     └─┬─ AccessChild
  │       └─── SubCollection[order]
  ├─┬─ Calculate[L_SHIPMODE=ship_mode, HIGH_LINE_COUNT=SUM($1.is_high_priority), LOW_LINE_COUNT=SUM(NOT($1.is_high_priority))]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── OrderBy[L_SHIPMODE.ASC(na_pos='first')]
""",
            id="tpch_q12",
        ),
        pytest.param(
            impl_tpch_q13,
            """
──┬─ TPCH
  ├─┬─ Partition[name='custs', by=num_non_special_orders]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Customers]
  │   └─┬─ Calculate[num_non_special_orders=COUNT($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[orders]
  │       └─── Where[NOT(LIKE(comment, '%special%requests%'))]
  ├─┬─ Calculate[C_COUNT=num_non_special_orders, CUSTDIST=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[custs]
  └─── TopK[10, CUSTDIST.DESC(na_pos='last'), C_COUNT.DESC(na_pos='last')]
""",
            id="tpch_q13",
        ),
        pytest.param(
            impl_tpch_q14,
            """
┌─── TPCH
└─┬─ Calculate[PROMO_REVENUE=(100.0 * SUM($1.promo_value)) / SUM($1.value)]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    ├─── Where[(ship_date >= datetime.date(1995, 9, 1)) & (ship_date < datetime.date(1995, 10, 1))]
    └─┬─ Calculate[value=extended_price * (1 - discount), promo_value=IFF(STARTSWITH($1.part_type, 'PROMO'), extended_price * (1 - discount), 0)]
      └─┬─ AccessChild
        └─── SubCollection[part]
""",
            id="tpch_q14",
        ),
        pytest.param(
            impl_tpch_q15,
            """
┌─── TPCH
└─┬─ Calculate[max_revenue=MAX($1.total_revenue)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[Suppliers]
  │ └─┬─ Calculate[total_revenue=SUM($1.extended_price * (1 - $1.discount))]
  │   └─┬─ AccessChild
  │     ├─── SubCollection[lines]
  │     └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── TableCollection[Suppliers]
  ├─┬─ Calculate[s_suppkey=key, s_name=name, s_address=address, s_phone=phone, total_revenue=SUM($1.extended_price * (1 - $1.discount))]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── Where[total_revenue == max_revenue]
  └─── OrderBy[s_suppkey.ASC(na_pos='first')]
""",
            id="tpch_q15",
        ),
        pytest.param(
            impl_tpch_q16,
            """
──┬─ TPCH
  ├─┬─ Partition[name='ps', by=(p_brand, p_type, p_size)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Parts]
  │   └─┬─ Where[(brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%')) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])]
  │     ├─── SubCollection[supply_records]
  │     ├─── Calculate[p_brand=brand, p_type=part_type, p_size=size, ps_suppkey=supplier_key]
  │     └─┬─ Where[NOT(LIKE($1.comment, '%Customer%Complaints%'))]
  │       └─┬─ AccessChild
  │         └─── SubCollection[supplier]
  ├─┬─ Calculate[p_brand=p_brand, p_type=p_type, p_size=p_size, supplier_count=NDISTINCT($1.supplier_key)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  └─── TopK[10, supplier_count.DESC(na_pos='last'), p_brand.ASC(na_pos='first'), p_type.ASC(na_pos='first')]
""",
            id="tpch_q16",
        ),
        pytest.param(
            impl_tpch_q17,
            """
┌─── TPCH
└─┬─ Calculate[avg_yearly=SUM($1.extended_price) / 7.0]
  └─┬─ AccessChild
    ├─── TableCollection[Parts]
    ├─── Where[(brand == 'Brand#23') & (container == 'MED BOX')]
    └─┬─ Calculate[avg_quantity=AVG($1.quantity)]
      ├─┬─ AccessChild
      │ └─── SubCollection[lines]
      ├─── SubCollection[lines]
      └─── Where[quantity < (0.2 * avg_quantity)]
""",
            id="tpch_q17",
        ),
        pytest.param(
            impl_tpch_q18,
            """
──┬─ TPCH
  ├─── TableCollection[Orders]
  ├─┬─ Calculate[C_NAME=$1.name, C_CUSTKEY=$1.key, O_ORDERKEY=key, O_ORDERDATE=order_date, O_TOTALPRICE=total_price, TOTAL_QUANTITY=SUM($2.quantity)]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[customer]
  │ └─┬─ AccessChild
  │   └─── SubCollection[lines]
  ├─── Where[TOTAL_QUANTITY > 300]
  └─── TopK[10, O_TOTALPRICE.DESC(na_pos='last'), O_ORDERDATE.ASC(na_pos='first')]
""",
            id="tpch_q18",
        ),
        pytest.param(
            impl_tpch_q19,
            """
┌─── TPCH
└─┬─ Calculate[REVENUE=SUM($1.extended_price * (1 - $1.discount))]
  └─┬─ AccessChild
    ├─── TableCollection[Lineitems]
    └─┬─ Where[ISIN(ship_mode, ['AIR', 'AIR REG']) & (ship_instruct == 'DELIVER IN PERSON') & ($1.size >= 1) & (((($1.size <= 5) & (quantity >= 1) & (quantity <= 11) & ISIN($1.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) & ($1.brand == 'Brand#12')) | (($1.size <= 10) & (quantity >= 10) & (quantity <= 20) & ISIN($1.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG']) & ($1.brand == 'Brand#23'))) | (($1.size <= 15) & (quantity >= 20) & (quantity <= 30) & ISIN($1.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']) & ($1.brand == 'Brand#34')))]
      └─┬─ AccessChild
        └─── SubCollection[part]
""",
            id="tpch_q19",
        ),
        pytest.param(
            impl_tpch_q20,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─── Calculate[s_name=name, s_address=address]
  ├─┬─ Where[($1.name == 'CANADA') & (COUNT($2) > 0)]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[nation]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[supply_records]
  │     ├─── SubCollection[part]
  │     └─┬─ Where[STARTSWITH(name, 'forest') & HAS($1) & (availqty > (SUM($1.quantity) * 0.5))]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1))]
  └─── TopK[10, s_name.ASC(na_pos='first')]
""",
            id="tpch_q20",
        ),
        pytest.param(
            impl_tpch_q21,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Where[$1.name == 'SAUDI ARABIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  ├─┬─ Calculate[s_name=name, numwait=COUNT($1)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─┬─ Where[receipt_date > commit_date]
  │     ├─── SubCollection[order]
  │     └─┬─ Where[(order_status == 'F') & HAS($1) & HASNOT($2)]
  │       ├─┬─ AccessChild
  │       │ ├─── SubCollection[lines]
  │       │ └─── Where[supplier_key != supplier_key]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(supplier_key != supplier_key) & (receipt_date > commit_date)]
  └─── TopK[10, numwait.DESC(na_pos='last'), s_name.ASC(na_pos='first')]
""",
            id="tpch_q21",
        ),
        pytest.param(
            impl_tpch_q22,
            """
┌─── TPCH
└─┬─ Calculate[avg_balance=AVG($1.acctbal)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[Customers]
  │ ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │ ├─┬─ Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT($1)]
  │ │ └─┬─ AccessChild
  │ │   └─── SubCollection[orders]
  │ └─── Where[acctbal > 0.0]
  ├─┬─ Partition[name='custs', by=cntry_code]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Customers]
  │   ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │   ├─┬─ Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT($1)]
  │   │ └─┬─ AccessChild
  │   │   └─── SubCollection[orders]
  │   └─── Where[acctbal > avg_balance]
  └─┬─ Calculate[cntry_code=cntry_code, num_custs=COUNT($1), totacctbal=SUM($1.acctbal)]
    └─┬─ AccessChild
      └─── PartitionChild[custs]
""",
            id="tpch_q22",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[], UnqualifiedNode],
    answer_tree_str: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    unqualified: UnqualifiedNode = init_pydough_context(graph)(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    assert (
        qualified.to_tree_string() == answer_tree_str.strip()
    ), "Mismatch between tree string representation of qualified node and expected QDAG tree string"
