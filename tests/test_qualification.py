"""
Unit tests the PyDough qualification process that transforms unqualified nodes
into qualified DAG nodes.
"""

from collections.abc import Callable

import pytest
from simple_pydough_functions import (
    partition_as_child,
    simple_collation,
    singular1,
    singular2,
    singular3,
    singular4,
)
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
from pydough.configs import PyDoughConfigs
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
            partition_as_child,
            """
┌─── TPCH
├─┬─ Calculate[avg_n_parts=AVG($1.n_parts)]
│ └─┬─ AccessChild
│   ├─┬─ Partition[name='p', by=size]
│   │ └─┬─ AccessChild
│   │   └─── TableCollection[Parts]
│   └─┬─ Calculate[n_parts=COUNT($1)]
│     └─┬─ AccessChild
│       └─── PartitionChild[p]
└─┬─ Calculate[n_parts=COUNT($1)]
  └─┬─ AccessChild
    ├─┬─ Partition[name='p', by=size]
    │ └─┬─ AccessChild
    │   └─── TableCollection[Parts]
    ├─┬─ Calculate[n_parts=COUNT($1)]
    │ └─┬─ AccessChild
    │   └─── PartitionChild[p]
    └─── Where[n_parts > avg_n_parts]
""",
            id="partition_as_child",
        ),
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
  │   ├─── Calculate[n_name=name]
  │   └─┬─ Where[$1.name == 'EUROPE']
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[region]
  │     ├─── SubCollection[suppliers]
  │     └─┬─ Calculate[s_acctbal=account_balance, s_name=name, s_address=address, s_phone=phone, s_comment=comment]
  │       ├─── SubCollection[supply_records]
  │       └─┬─ Calculate[supplycost=supplycost]
  │         ├─── SubCollection[part]
  │         └─── Where[ENDSWITH(part_type, 'BRASS') & (size == 15)]
  └─┬─ Calculate[best_cost=MIN($1.supplycost)]
    ├─┬─ AccessChild
    │ └─── PartitionChild[p]
    ├─── PartitionChild[p]
    ├─── Where[(supplycost == best_cost) & ENDSWITH(part_type, 'BRASS') & (size == 15)]
    ├─── Calculate[S_ACCTBAL=s_acctbal, S_NAME=s_name, N_NAME=n_name, P_PARTKEY=key, P_MFGR=manufacturer, S_ADDRESS=s_address, S_PHONE=s_phone, S_COMMENT=s_comment]
    └─── TopK[10, S_ACCTBAL.DESC(na_pos='last'), N_NAME.ASC(na_pos='first'), S_NAME.ASC(na_pos='first'), P_PARTKEY.ASC(na_pos='first')]
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
  │   ├─── Calculate[order_date=order_date, ship_priority=ship_priority]
  │   └─┬─ Where[($1.mktsegment == 'BUILDING') & (order_date < datetime.date(1995, 3, 15))]
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[customer]
  │     ├─── SubCollection[lines]
  │     └─── Where[ship_date > datetime.date(1995, 3, 15)]
  ├─┬─ Calculate[L_ORDERKEY=order_key, REVENUE=SUM($1.extended_price * (1 - $1.discount)), O_ORDERDATE=order_date, O_SHIPPRIORITY=ship_priority]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, REVENUE.DESC(na_pos='last'), O_ORDERDATE.ASC(na_pos='first'), L_ORDERKEY.ASC(na_pos='first')]
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
  ├─── Calculate[nation_name=name]
  ├─┬─ Where[$1.name == 'ASIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[region]
  ├─┬─ Calculate[N_NAME=name, REVENUE=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[customers]
  │     ├─── SubCollection[orders]
  │     └─┬─ Where[(order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1))]
  │       ├─── SubCollection[lines]
  │       ├─┬─ Where[$1.name == nation_name]
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
  │   ├─── TableCollection[Nations]
  │   └─┬─ Calculate[nation_name=name]
  │     └─┬─ SubCollection[suppliers]
  │       ├─── SubCollection[supply_records]
  │       └─┬─ Where[$1.part_type == 'ECONOMY ANODIZED STEEL']
  │         ├─┬─ AccessChild
  │         │ └─── SubCollection[part]
  │         ├─── SubCollection[lines]
  │         └─┬─ Calculate[volume=extended_price * (1 - discount)]
  │           ├─── SubCollection[order]
  │           ├─── Calculate[o_year=YEAR(order_date), brazil_volume=IFF(nation_name == 'BRAZIL', volume, 0)]
  │           └─┬─ Where[(order_date >= datetime.date(1995, 1, 1)) & (order_date <= datetime.date(1996, 12, 31)) & ($1.name == 'AMERICA')]
  │             └─┬─ AccessChild
  │               └─┬─ SubCollection[customer]
  │                 └─┬─ SubCollection[nation]
  │                   └─── SubCollection[region]
  └─┬─ Calculate[O_YEAR=o_year, MKT_SHARE=SUM($1.brazil_volume) / SUM($1.volume)]
    └─┬─ AccessChild
      └─── PartitionChild[v]
""",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
            """
──┬─ TPCH
  ├─┬─ Partition[name='l', by=(nation_name, o_year)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Nations]
  │   └─┬─ Calculate[nation_name=name]
  │     └─┬─ SubCollection[suppliers]
  │       ├─── SubCollection[supply_records]
  │       ├─── Calculate[supplycost=supplycost]
  │       └─┬─ Where[CONTAINS($1.name, 'green')]
  │         ├─┬─ AccessChild
  │         │ └─── SubCollection[part]
  │         ├─── SubCollection[lines]
  │         └─┬─ Calculate[o_year=YEAR($1.order_date), value=(extended_price * (1 - discount)) - (supplycost * quantity)]
  │           └─┬─ AccessChild
  │             └─── SubCollection[order]
  ├─┬─ Calculate[NATION=nation_name, O_YEAR=o_year, AMOUNT=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[l]
  └─── TopK[10, NATION.ASC(na_pos='first'), O_YEAR.DESC(na_pos='last')]
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
  ├─┬─ Calculate[PS_PARTKEY=part_key, VALUE=SUM($1.metric)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  ├─── Where[VALUE > min_market_share]
  └─── TopK[10, VALUE.DESC(na_pos='last')]
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
  ├─┬─ Calculate[S_SUPPKEY=key, S_NAME=name, S_ADDRESS=address, S_PHONE=phone, TOTAL_REVENUE=SUM($1.extended_price * (1 - $1.discount))]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── Where[TOTAL_REVENUE == max_revenue]
  └─── OrderBy[S_SUPPKEY.ASC(na_pos='first')]
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
  │   ├─── Where[(brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%')) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])]
  │   └─┬─ Calculate[p_brand=brand, p_type=part_type, p_size=size]
  │     ├─── SubCollection[supply_records]
  │     └─┬─ Where[NOT(LIKE($1.comment, '%Customer%Complaints%'))]
  │       └─┬─ AccessChild
  │         └─── SubCollection[supplier]
  ├─┬─ Calculate[P_BRAND=p_brand, P_TYPE=p_type, P_SIZE=p_size, SUPPLIER_COUNT=NDISTINCT($1.supplier_key)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[ps]
  └─── TopK[10, SUPPLIER_COUNT.DESC(na_pos='last'), P_BRAND.ASC(na_pos='first'), P_TYPE.ASC(na_pos='first'), P_SIZE.ASC(na_pos='first')]
""",
            id="tpch_q16",
        ),
        pytest.param(
            impl_tpch_q17,
            """
┌─── TPCH
└─┬─ Calculate[AVG_YEARLY=SUM($1.extended_price) / 7.0]
  └─┬─ AccessChild
    ├─── TableCollection[Parts]
    ├─── Where[(brand == 'Brand#23') & (container == 'MED BOX')]
    └─┬─ Calculate[part_avg_quantity=AVG($1.quantity)]
      ├─┬─ AccessChild
      │ └─── SubCollection[lines]
      ├─── SubCollection[lines]
      └─── Where[quantity < (0.2 * part_avg_quantity)]
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
  ├─── Calculate[S_NAME=name, S_ADDRESS=address]
  ├─┬─ Where[(($1.name == 'CANADA') & COUNT($2)) > 0]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[nation]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[supply_records]
  │   └─┬─ Calculate[availqty=availqty]
  │     ├─── SubCollection[part]
  │     └─┬─ Where[STARTSWITH(name, 'forest') & (availqty > (SUM($1.quantity) * 0.5))]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1))]
  └─── TopK[10, S_NAME.ASC(na_pos='first')]
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
  ├─┬─ Calculate[S_NAME=name, NUMWAIT=COUNT($1)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   ├─── Calculate[original_key=supplier_key]
  │   └─┬─ Where[receipt_date > commit_date]
  │     ├─── SubCollection[order]
  │     └─┬─ Where[(order_status == 'F') & HAS($1) & HASNOT($2)]
  │       ├─┬─ AccessChild
  │       │ ├─── SubCollection[lines]
  │       │ └─── Where[supplier_key != original_key]
  │       └─┬─ AccessChild
  │         ├─── SubCollection[lines]
  │         └─── Where[(supplier_key != original_key) & (receipt_date > commit_date)]
  └─── TopK[10, NUMWAIT.DESC(na_pos='last'), S_NAME.ASC(na_pos='first')]
""",
            id="tpch_q21",
        ),
        pytest.param(
            impl_tpch_q22,
            """
┌─── TPCH
└─┬─ Calculate[global_avg_balance=AVG($1.acctbal)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[Customers]
  │ ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │ ├─── Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17'])]
  │ └─── Where[acctbal > 0.0]
  ├─┬─ Partition[name='custs', by=cntry_code]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[Customers]
  │   ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │   ├─── Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17'])]
  │   └─┬─ Where[(acctbal > global_avg_balance) & (COUNT($1) == 0)]
  │     └─┬─ AccessChild
  │       └─── SubCollection[orders]
  ├─┬─ Calculate[CNTRY_CODE=cntry_code, NUM_CUSTS=COUNT($1), TOTACCTBAL=SUM($1.acctbal)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[custs]
  └─── OrderBy[CNTRY_CODE.ASC(na_pos='first')]
""",
            id="tpch_q22",
        ),
        pytest.param(
            singular1,
            """
──┬─ TPCH
  ├─── TableCollection[Regions]
  └─┬─ Calculate[name=name, nation_4_name=$1.name]
    └─┬─ AccessChild
      ├─── SubCollection[nations]
      ├─── Where[key == 4]
      └─── Singular
         """,
            id="singular1",
        ),
        pytest.param(
            singular2,
            """
──┬─ TPCH
  ├─── TableCollection[Nations]
  └─┬─ Calculate[name=name, okey=$1.key]
    └─┬─ AccessChild
      ├─── SubCollection[customers]
      ├─── Where[key == 1]
      └─┬─ Singular
        ├─── SubCollection[orders]
        ├─── Where[key == 454791]
        └─── Singular
        """,
            id="singular2",
        ),
        pytest.param(
            singular3,
            """
──┬─ TPCH
  ├─── TableCollection[Customers]
  ├─── TopK[5, name.ASC(na_pos='first')]
  ├─── Calculate[name=name]
  └─┬─ OrderBy[$1.order_date.ASC(na_pos='last')]
    └─┬─ AccessChild
      ├─── SubCollection[orders]
      ├─── Where[RANKING(by=(total_price.DESC(na_pos='last')), levels=1) == 1]
      └─── Singular
        """,
            id="singular3",
        ),
        pytest.param(
            singular4,
            """
──┬─ TPCH
  ├─── TableCollection[Customers]
  ├─── Where[nation_key == 6]
  ├─┬─ TopK[5, $1.order_date.ASC(na_pos='last')]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[orders]
  │   ├─── Where[order_priority == '1-URGENT']
  │   ├─── Where[RANKING(by=(total_price.DESC(na_pos='last')), levels=1) == 1]
  │   └─── Singular
  └─── Calculate[name=name]        
        """,
            id="singular4",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[[], UnqualifiedNode],
    answer_tree_str: str,
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    graph: GraphMetadata = get_sample_graph("TPCH")
    unqualified: UnqualifiedNode = init_pydough_context(graph)(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph, default_config)
    assert isinstance(qualified, PyDoughCollectionQDAG), (
        "Expected qualified answer to be a collection, not an expression"
    )
    assert qualified.to_tree_string() == answer_tree_str.strip(), (
        "Mismatch between tree string representation of qualified node and expected QDAG tree string"
    )


@pytest.mark.parametrize(
    "impl, answer_tree_str, collation_default_asc, propagate_collation",
    [
        pytest.param(
            simple_collation,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Calculate[p=PERCENTILE(by=(COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last'))), r=RANKING(by=(key.ASC(na_pos='first'), COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')))]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  ├─┬─ OrderBy[COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last')]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  └─┬─ TopK[5, key.ASC(na_pos='first'), COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')]
    └─┬─ AccessChild
      └─── SubCollection[supply_records]
            """,
            True,
            True,
            id="asc-with_propagation",
        ),
        pytest.param(
            simple_collation,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Calculate[p=PERCENTILE(by=(COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.ASC(na_pos='first'))), r=RANKING(by=(key.ASC(na_pos='first'), COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')))]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  ├─┬─ OrderBy[COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.ASC(na_pos='first')]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  └─┬─ TopK[5, key.ASC(na_pos='first'), COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')]
    └─┬─ AccessChild
      └─── SubCollection[supply_records]
            """,
            True,
            False,
            id="asc-no_propagation",
        ),
        pytest.param(
            simple_collation,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Calculate[p=PERCENTILE(by=(COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last'))), r=RANKING(by=(key.DESC(na_pos='last'), COUNT($1).DESC(na_pos='last'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')))]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  ├─┬─ OrderBy[COUNT($1).ASC(na_pos='first'), name.ASC(na_pos='first'), address.ASC(na_pos='first'), nation_key.ASC(na_pos='first'), phone.ASC(na_pos='first'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last')]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  └─┬─ TopK[5, key.DESC(na_pos='last'), COUNT($1).DESC(na_pos='last'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.ASC(na_pos='first')]
    └─┬─ AccessChild
      └─── SubCollection[supply_records]
            """,
            False,
            True,
            id="desc-with_propagation",
        ),
        pytest.param(
            simple_collation,
            """
──┬─ TPCH
  ├─── TableCollection[Suppliers]
  ├─┬─ Calculate[p=PERCENTILE(by=(COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last'))), r=RANKING(by=(key.DESC(na_pos='last'), COUNT($1).DESC(na_pos='last'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.DESC(na_pos='last')))]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  ├─┬─ OrderBy[COUNT($1).ASC(na_pos='first'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.DESC(na_pos='last'), comment.DESC(na_pos='last')]
  │ └─┬─ AccessChild
  │   └─── SubCollection[supply_records]
  └─┬─ TopK[5, key.DESC(na_pos='last'), COUNT($1).DESC(na_pos='last'), name.DESC(na_pos='last'), address.DESC(na_pos='last'), nation_key.DESC(na_pos='last'), phone.DESC(na_pos='last'), account_balance.ASC(na_pos='first'), comment.DESC(na_pos='last')]
    └─┬─ AccessChild
      └─── SubCollection[supply_records]
            """,
            False,
            False,
            id="desc-no_propagation",
        ),
    ],
)
def test_qualify_node_collation(
    impl: Callable[[], UnqualifiedNode],
    answer_tree_str: str,
    collation_default_asc: bool,
    propagate_collation: bool,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    custom_config: PyDoughConfigs = PyDoughConfigs()
    custom_config.collation_default_asc = collation_default_asc
    custom_config.propagate_collation = propagate_collation
    graph: GraphMetadata = get_sample_graph("TPCH")
    unqualified: UnqualifiedNode = init_pydough_context(graph)(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph, custom_config)
    assert isinstance(qualified, PyDoughCollectionQDAG), (
        "Expected qualified answer to be a collection, not an expression"
    )
    assert qualified.to_tree_string() == answer_tree_str.strip(), (
        "Mismatch between tree string representation of qualified node and expected QDAG tree string"
    )
