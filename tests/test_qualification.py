"""
Unit tests the PyDough qualification process that transforms unqualified nodes
into qualified DAG nodes.
"""

from collections.abc import Callable

import pytest

from pydough import init_pydough_context
from pydough.configs import PyDoughConfigs, PyDoughSession
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.unqualified import (
    UnqualifiedNode,
    qualify_node,
)
from tests.test_pydough_functions.simple_pydough_functions import (
    absurd_partition_window_per,
    absurd_window_per,
    customer_most_recent_orders,
    n_orders_first_day,
    orders_versus_first_orders,
    partition_as_child,
    region_orders_from_nations_richest,
    regional_first_order_best_line_part,
    richest_customer_per_region,
    simple_collation,
    simple_cross_1,
    simple_cross_2,
    simple_cross_3,
    simple_cross_4,
    simple_cross_5,
    simple_cross_6,
    singular1,
    singular2,
    singular3,
    singular4,
    supplier_best_part,
    wealthiest_supplier,
)
from tests.test_pydough_functions.tpch_test_functions import (
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


@pytest.mark.parametrize(
    "impl, answer_tree_str",
    [
        pytest.param(
            partition_as_child,
            """
┌─── TPCH
├─┬─ Calculate[avg_n_parts=AVG($1.n_parts)]
│ └─┬─ AccessChild
│   ├─┬─ Partition[name='sizes', by=size]
│   │ └─┬─ AccessChild
│   │   └─── TableCollection[parts]
│   └─┬─ Calculate[n_parts=COUNT($1)]
│     └─┬─ AccessChild
│       └─── PartitionChild[parts]
└─┬─ Calculate[n_parts=COUNT($1)]
  └─┬─ AccessChild
    ├─┬─ Partition[name='sizes', by=size]
    │ └─┬─ AccessChild
    │   └─── TableCollection[parts]
    ├─┬─ Calculate[n_parts=COUNT($1)]
    │ └─┬─ AccessChild
    │   └─── PartitionChild[parts]
    └─── Where[n_parts > avg_n_parts]
""",
            id="partition_as_child",
        ),
        pytest.param(
            impl_tpch_q1,
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(return_flag, status)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   └─── Where[ship_date <= datetime.date(1998, 12, 1)]
  ├─┬─ Calculate[L_RETURNFLAG=return_flag, L_LINESTATUS=status, SUM_QTY=SUM($1.quantity), SUM_BASE_PRICE=SUM($1.extended_price), SUM_DISC_PRICE=SUM($1.extended_price * (1 - $1.discount)), SUM_CHARGE=SUM(($1.extended_price * (1 - $1.discount)) * (1 + $1.tax)), AVG_QTY=AVG($1.quantity), AVG_PRICE=AVG($1.extended_price), AVG_DISC=AVG($1.discount), COUNT_ORDER=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[lines]
  └─── OrderBy[L_RETURNFLAG.ASC(na_pos='first'), L_LINESTATUS.ASC(na_pos='first')]
""",
            id="tpch_q1",
        ),
        pytest.param(
            impl_tpch_q2,
            """
──┬─ TPCH
  ├─── TableCollection[parts]
  ├─── Where[ENDSWITH(part_type, 'BRASS') & (size == 15)]
  └─┬─ Calculate[P_PARTKEY=key, P_MFGR=manufacturer]
    ├─── SubCollection[supply_records]
    ├─┬─ Where[$1.name == 'EUROPE']
    │ └─┬─ AccessChild
    │   └─┬─ SubCollection[supplier]
    │     └─┬─ SubCollection[nation]
    │       └─── SubCollection[region]
    ├─── Where[RANKING(by=(supply_cost.ASC(na_pos='first')), levels=1, allow_ties=True) == 1]
    ├─┬─ Calculate[S_ACCTBAL=$1.account_balance, S_NAME=$1.name, N_NAME=$2.name, P_PARTKEY=P_PARTKEY, P_MFGR=P_MFGR, S_ADDRESS=$1.address, S_PHONE=$1.phone, S_COMMENT=$1.comment]
    │ ├─┬─ AccessChild
    │ │ └─── SubCollection[supplier]
    │ └─┬─ AccessChild
    │   └─┬─ SubCollection[supplier]
    │     └─── SubCollection[nation]
    └─── TopK[10, S_ACCTBAL.DESC(na_pos='last'), N_NAME.ASC(na_pos='first'), S_NAME.ASC(na_pos='first'), P_PARTKEY.ASC(na_pos='first')]
""",
            id="tpch_q2",
        ),
        pytest.param(
            impl_tpch_q3,
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(order_key, order_date, ship_priority)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[orders]
  │   ├─── Calculate[order_date=order_date, ship_priority=ship_priority]
  │   └─┬─ Where[($1.market_segment == 'BUILDING') & (order_date < datetime.date(1995, 3, 15))]
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[customer]
  │     ├─── SubCollection[lines]
  │     └─── Where[ship_date > datetime.date(1995, 3, 15)]
  ├─┬─ Calculate[L_ORDERKEY=order_key, REVENUE=SUM($1.extended_price * (1 - $1.discount)), O_ORDERDATE=order_date, O_SHIPPRIORITY=ship_priority]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[lines]
  └─── TopK[10, REVENUE.DESC(na_pos='last'), O_ORDERDATE.ASC(na_pos='first'), L_ORDERKEY.ASC(na_pos='first')]
""",
            id="tpch_q3",
        ),
        pytest.param(
            impl_tpch_q4,
            """
──┬─ TPCH
  ├─┬─ Partition[name='priorities', by=order_priority]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[orders]
  │   └─┬─ Where[(YEAR(order_date) == 1993) & (QUARTER(order_date) == 3) & HAS($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[lines]
  │       └─── Where[commit_date < receipt_date]
  ├─┬─ Calculate[O_ORDERPRIORITY=order_priority, ORDER_COUNT=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[orders]
  └─── OrderBy[O_ORDERPRIORITY.ASC(na_pos='first')]
""",
            id="tpch_q4",
        ),
        pytest.param(
            impl_tpch_q5,
            """
──┬─ TPCH
  ├─── TableCollection[nations]
  ├─── Calculate[nation_name=name]
  ├─┬─ Where[$1.name == 'ASIA']
  │ └─┬─ AccessChild
  │   └─── SubCollection[region]
  ├─┬─ Where[HAS($1)]
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
    ├─── TableCollection[lines]
    ├─── Where[(ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1)) & (discount >= 0.05) & (discount <= 0.07) & (quantity < 24)]
    └─── Calculate[amt=extended_price * discount]
""",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q7,
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(supp_nation, cust_nation, l_year)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   ├─┬─ Calculate[supp_nation=$1.name, cust_nation=$2.name, l_year=YEAR(ship_date), volume=extended_price * (1 - discount)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─┬─ SubCollection[supplier]
  │   │ │   └─── SubCollection[nation]
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[order]
  │   │     └─┬─ SubCollection[customer]
  │   │       └─── SubCollection[nation]
  │   └─── Where[ISIN(YEAR(ship_date), [1995, 1996]) & (((supp_nation == 'FRANCE') & (cust_nation == 'GERMANY')) | ((supp_nation == 'GERMANY') & (cust_nation == 'FRANCE')))]
  ├─┬─ Calculate[SUPP_NATION=supp_nation, CUST_NATION=cust_nation, L_YEAR=l_year, REVENUE=SUM($1.volume)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[lines]
  └─── OrderBy[SUPP_NATION.ASC(na_pos='first'), CUST_NATION.ASC(na_pos='first'), L_YEAR.ASC(na_pos='first')]
""",
            id="tpch_q7",
        ),
        pytest.param(
            impl_tpch_q8,
            """
──┬─ TPCH
  ├─┬─ Partition[name='years', by=O_YEAR]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   ├─┬─ Where[($1.part_type == 'ECONOMY ANODIZED STEEL') & ISIN(YEAR($2.order_date), [1995, 1996]) & ($3.name == 'AMERICA')]
  │   │ ├─┬─ AccessChild
  │   │ │ └─── SubCollection[part]
  │   │ ├─┬─ AccessChild
  │   │ │ └─── SubCollection[order]
  │   │ └─┬─ AccessChild
  │   │   └─┬─ SubCollection[order]
  │   │     └─┬─ SubCollection[customer]
  │   │       └─┬─ SubCollection[nation]
  │   │         └─── SubCollection[region]
  │   └─┬─ Calculate[O_YEAR=YEAR($1.order_date), volume=extended_price * (1 - discount), brazil_volume=IFF($2.name == 'BRAZIL', extended_price * (1 - discount), 0)]
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[order]
  │     └─┬─ AccessChild
  │       └─┬─ SubCollection[supplier]
  │         └─── SubCollection[nation]
  └─┬─ Calculate[O_YEAR=O_YEAR, MKT_SHARE=SUM($1.brazil_volume) / SUM($1.volume)]
    └─┬─ AccessChild
      └─── PartitionChild[lines]
""",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(nation_name, o_year)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   ├─┬─ Where[CONTAINS($1.name, 'green')]
  │   │ └─┬─ AccessChild
  │   │   └─── SubCollection[part]
  │   └─┬─ Calculate[nation_name=$1.name, o_year=YEAR($2.order_date), value=(extended_price * (1 - discount)) - ($3.supply_cost * quantity)]
  │     ├─┬─ AccessChild
  │     │ └─┬─ SubCollection[supplier]
  │     │   └─── SubCollection[nation]
  │     ├─┬─ AccessChild
  │     │ └─── SubCollection[order]
  │     └─┬─ AccessChild
  │       └─── SubCollection[part_and_supplier]
  ├─┬─ Calculate[NATION=nation_name, O_YEAR=o_year, AMOUNT=SUM($1.value)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[lines]
  └─── TopK[10, NATION.ASC(na_pos='first'), O_YEAR.DESC(na_pos='last')]
""",
            id="tpch_q9",
        ),
        pytest.param(
            impl_tpch_q10,
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  ├─┬─ Calculate[C_CUSTKEY=key, C_NAME=name, REVENUE=SUM($1.extended_price * (1 - $1.discount)), C_ACCTBAL=account_balance, N_NAME=$2.name, C_ADDRESS=address, C_PHONE=phone, C_COMMENT=comment]
  │ ├─┬─ AccessChild
  │ │ ├─── SubCollection[orders]
  │ │ └─┬─ Where[(YEAR(order_date) == 1993) & (QUARTER(order_date) == 4)]
  │ │   ├─── SubCollection[lines]
  │ │   └─── Where[return_flag == 'R']
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
  │ ├─── TableCollection[supply_records]
  │ ├─┬─ Where[$1.name == 'GERMANY']
  │ │ └─┬─ AccessChild
  │ │   └─┬─ SubCollection[supplier]
  │ │     └─── SubCollection[nation]
  │ └─── Calculate[metric=supply_cost * available_quantity]
  ├─┬─ Partition[name='parts', by=part_key]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[supply_records]
  │   └─┬─ Where[$1.name == 'GERMANY']
  │     └─┬─ AccessChild
  │       └─┬─ SubCollection[supplier]
  │         └─── SubCollection[nation]
  ├─┬─ Calculate[PS_PARTKEY=part_key, VALUE=SUM($1.supply_cost * $1.available_quantity)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[supply_records]
  ├─── Where[VALUE > min_market_share]
  └─── TopK[10, VALUE.DESC(na_pos='last')]
""",
            id="tpch_q11",
        ),
        pytest.param(
            impl_tpch_q12,
            """
──┬─ TPCH
  ├─┬─ Partition[name='modes', by=ship_mode]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[lines]
  │   ├─── Where[((ship_mode == 'MAIL') | (ship_mode == 'SHIP')) & (ship_date < commit_date) & (commit_date < receipt_date) & (YEAR(receipt_date) == 1994)]
  │   └─┬─ Calculate[is_high_priority=ISIN($1.order_priority, ['1-URGENT', '2-HIGH'])]
  │     └─┬─ AccessChild
  │       └─── SubCollection[order]
  ├─┬─ Calculate[L_SHIPMODE=ship_mode, HIGH_LINE_COUNT=SUM($1.is_high_priority), LOW_LINE_COUNT=SUM(NOT($1.is_high_priority))]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[lines]
  └─── OrderBy[L_SHIPMODE.ASC(na_pos='first')]
""",
            id="tpch_q12",
        ),
        pytest.param(
            impl_tpch_q13,
            """
──┬─ TPCH
  ├─┬─ Partition[name='num_order_groups', by=num_non_special_orders]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[customers]
  │   └─┬─ Calculate[num_non_special_orders=COUNT($1)]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[orders]
  │       └─── Where[NOT(LIKE(comment, '%special%requests%'))]
  ├─┬─ Calculate[C_COUNT=num_non_special_orders, CUSTDIST=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[customers]
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
    ├─── TableCollection[lines]
    ├─── Where[(YEAR(ship_date) == 1995) & (MONTH(ship_date) == 9)]
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
  │ ├─── TableCollection[suppliers]
  │ ├─┬─ Where[HAS($1)]
  │ │ └─┬─ AccessChild
  │ │   ├─── SubCollection[lines]
  │ │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  │ └─┬─ Calculate[total_revenue=SUM($1.extended_price * (1 - $1.discount))]
  │   └─┬─ AccessChild
  │     ├─── SubCollection[lines]
  │     └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─── TableCollection[suppliers]
  ├─┬─ Where[HAS($1) & (SUM($1.extended_price * (1 - $1.discount)) == max_revenue)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  ├─┬─ Calculate[S_SUPPKEY=key, S_NAME=name, S_ADDRESS=address, S_PHONE=phone, TOTAL_REVENUE=SUM($1.extended_price * (1 - $1.discount))]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[lines]
  │   └─── Where[(ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))]
  └─── OrderBy[S_SUPPKEY.ASC(na_pos='first')]
""",
            id="tpch_q15",
        ),
        pytest.param(
            impl_tpch_q16,
            """
──┬─ TPCH
  ├─┬─ Partition[name='groups', by=(P_BRAND, P_TYPE, P_SIZE)]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[supply_records]
  │   ├─┬─ Where[NOT(LIKE($1.comment, '%Customer%Complaints%')) & HAS($2)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─── SubCollection[supplier]
  │   │ └─┬─ AccessChild
  │   │   ├─── SubCollection[part]
  │   │   └─── Where[(brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%')) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])]
  │   └─┬─ Calculate[P_BRAND=$1.brand, P_TYPE=$1.part_type, P_SIZE=$1.size]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[part]
  │       └─── Where[(brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%')) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])]
  ├─┬─ Calculate[P_BRAND=P_BRAND, P_TYPE=P_TYPE, P_SIZE=P_SIZE, SUPPLIER_COUNT=NDISTINCT($1.supplier_key)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[supply_records]
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
    ├─── TableCollection[parts]
    └─┬─ Where[(brand == 'Brand#23') & (container == 'MED BOX')]
      ├─── SubCollection[lines]
      └─── Where[quantity < (0.2 * RELAVG(quantity, by=(), levels=1))]
""",
            id="tpch_q17",
        ),
        pytest.param(
            impl_tpch_q18,
            """
──┬─ TPCH
  ├─── TableCollection[orders]
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
    ├─── TableCollection[lines]
    └─┬─ Where[ISIN(ship_mode, ['AIR', 'AIR REG']) & (ship_instruct == 'DELIVER IN PERSON') & (((MONOTONIC(1, $1.size, 5) & MONOTONIC(1, quantity, 11) & ISIN($1.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']) & ($1.brand == 'Brand#12')) | (MONOTONIC(1, $1.size, 10) & MONOTONIC(10, quantity, 20) & ISIN($1.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG']) & ($1.brand == 'Brand#23'))) | (MONOTONIC(1, $1.size, 15) & MONOTONIC(20, quantity, 30) & ISIN($1.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']) & ($1.brand == 'Brand#34')))]
      └─┬─ AccessChild
        └─── SubCollection[part]
""",
            id="tpch_q19",
        ),
        pytest.param(
            impl_tpch_q20,
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  ├─── Calculate[S_NAME=name, S_ADDRESS=address]
  ├─┬─ Where[($1.name == 'CANADA') & (COUNT($2) > 0) & HAS($2)]
  │ ├─┬─ AccessChild
  │ │ └─── SubCollection[nation]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[supply_records]
  │   └─┬─ Where[HAS($1) & (available_quantity > (0.5 * SUM($1.part_qty)))]
  │     └─┬─ AccessChild
  │       ├─── SubCollection[part]
  │       ├─┬─ Where[STARTSWITH(name, 'forest') & HAS($1)]
  │       │ └─┬─ AccessChild
  │       │   ├─── SubCollection[lines]
  │       │   └─── Where[YEAR(ship_date) == 1994]
  │       └─┬─ Calculate[part_qty=SUM($1.quantity)]
  │         └─┬─ AccessChild
  │           ├─── SubCollection[lines]
  │           └─── Where[YEAR(ship_date) == 1994]
  └─── TopK[10, S_NAME.ASC(na_pos='first')]
""",
            id="tpch_q20",
        ),
        pytest.param(
            impl_tpch_q21,
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
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
└─┬─ Calculate[global_avg_balance=AVG($1.account_balance)]
  ├─┬─ AccessChild
  │ ├─── TableCollection[customers]
  │ ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │ ├─── Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17'])]
  │ └─── Where[account_balance > 0.0]
  ├─┬─ Partition[name='countries', by=cntry_code]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[customers]
  │   ├─── Calculate[cntry_code=SLICE(phone, None, 2, None)]
  │   └─┬─ Where[ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & (account_balance > global_avg_balance) & (COUNT($1) == 0)]
  │     └─┬─ AccessChild
  │       └─── SubCollection[orders]
  ├─┬─ Calculate[CNTRY_CODE=cntry_code, NUM_CUSTS=COUNT($1), TOTACCTBAL=SUM($1.account_balance)]
  │ └─┬─ AccessChild
  │   └─── PartitionChild[customers]
  └─── OrderBy[CNTRY_CODE.ASC(na_pos='first')]
""",
            id="tpch_q22",
        ),
        pytest.param(
            singular1,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
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
  ├─── TableCollection[nations]
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
  ├─── TableCollection[customers]
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
  ├─── TableCollection[customers]
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
        pytest.param(
            customer_most_recent_orders,
            """
──┬─ TPCH
  ├─── TableCollection[customers]
  ├─┬─ Where[HAS($1)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[orders]
  │   └─── Where[RANKING(by=(order_date.DESC(na_pos='last'), key.ASC(na_pos='first')), levels=1, allow_ties=False) <= 5]
  ├─┬─ Calculate[name=name, total_recent_value=SUM($1.total_price)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[orders]
  │   └─── Where[RANKING(by=(order_date.DESC(na_pos='last'), key.ASC(na_pos='first')), levels=1, allow_ties=False) <= 5]
  └─── TopK[3, total_recent_value.DESC(na_pos='last')] 
        """,
            id="customer_most_recent_orders",
        ),
        pytest.param(
            richest_customer_per_region,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Calculate[region_name=name]
    ├─── SubCollection[nations]
    └─┬─ Calculate[nation_name=name]
      ├─── SubCollection[customers]
      ├─── Where[RANKING(by=(account_balance.DESC(na_pos='last'), name.ASC(na_pos='first')), levels=2, allow_ties=False) == 1]
      └─── Calculate[region_name=region_name, nation_name=nation_name, customer_name=name, balance=account_balance]   
        """,
            id="richest_customer_per_region",
        ),
        pytest.param(
            wealthiest_supplier,
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  ├─── Where[RANKING(by=(account_balance.DESC(na_pos='last'), name.ASC(na_pos='first')), allow_ties=False) == 1]
  └─── Calculate[name=name, account_balance=account_balance] 
        """,
            id="wealthiest_supplier",
        ),
        pytest.param(
            n_orders_first_day,
            """
┌─── TPCH
└─┬─ Calculate[n_orders=COUNT($1)]
  └─┬─ AccessChild
    ├─── TableCollection[orders]
    └─── Where[RANKING(by=(order_date.ASC(na_pos='first')), allow_ties=True) == 1]   
        """,
            id="n_orders_first_day",
        ),
        pytest.param(
            supplier_best_part,
            """
──┬─ TPCH
  ├─── TableCollection[suppliers]
  ├─┬─ Where[$1.name == 'FRANCE']
  │ └─┬─ AccessChild
  │   └─── SubCollection[nation]
  ├─┬─ Where[HAS($1)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[supply_records]
  │   ├─┬─ Where[HAS($1)]
  │   │ └─┬─ AccessChild
  │   │   ├─── SubCollection[lines]
  │   │   └─── Where[(YEAR(ship_date) == 1994) & (tax == 0)]
  │   ├─┬─ Calculate[part_name=$1.name, quantity=SUM($2.quantity), n_shipments=COUNT($2)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─── SubCollection[part]
  │   │ └─┬─ AccessChild
  │   │   ├─── SubCollection[lines]
  │   │   └─── Where[(YEAR(ship_date) == 1994) & (tax == 0)]
  │   ├─── Where[RANKING(by=(quantity.DESC(na_pos='last')), levels=1, allow_ties=False) == 1]
  │   └─── Singular
  ├─┬─ Calculate[supplier_name=name, part_name=$1.part_name, total_quantity=$1.quantity, n_shipments=$1.n_shipments]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[supply_records]
  │   ├─┬─ Where[HAS($1)]
  │   │ └─┬─ AccessChild
  │   │   ├─── SubCollection[lines]
  │   │   └─── Where[(YEAR(ship_date) == 1994) & (tax == 0)]
  │   ├─┬─ Calculate[part_name=$1.name, quantity=SUM($2.quantity), n_shipments=COUNT($2)]
  │   │ ├─┬─ AccessChild
  │   │ │ └─── SubCollection[part]
  │   │ └─┬─ AccessChild
  │   │   ├─── SubCollection[lines]
  │   │   └─── Where[(YEAR(ship_date) == 1994) & (tax == 0)]
  │   ├─── Where[RANKING(by=(quantity.DESC(na_pos='last')), levels=1, allow_ties=False) == 1]
  │   └─── Singular
  └─── TopK[3, total_quantity.DESC(na_pos='last'), supplier_name.ASC(na_pos='first')]  
        """,
            id="supplier_best_part",
        ),
        pytest.param(
            region_orders_from_nations_richest,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  ├─┬─ Calculate[region_name=name, n_orders=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[nations]
  │     ├─── SubCollection[customers]
  │     └─┬─ Where[RANKING(by=(account_balance.DESC(na_pos='last'), name.ASC(na_pos='first')), levels=1, allow_ties=False) == 1]
  │       └─── SubCollection[orders]
  └─── OrderBy[region_name.ASC(na_pos='first')]
        """,
            id="region_orders_from_nations_richest",
        ),
        pytest.param(
            regional_first_order_best_line_part,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  ├─┬─ Calculate[region_name=name, part_name=$1.name]
  │ └─┬─ AccessChild
  │   └─┬─ SubCollection[nations]
  │     └─┬─ SubCollection[customers]
  │       ├─── SubCollection[orders]
  │       ├─── Where[YEAR(order_date) == 1992]
  │       ├─── Where[RANKING(by=(order_date.ASC(na_pos='first'), key.ASC(na_pos='first')), levels=3, allow_ties=False) == 1]
  │       └─┬─ Singular
  │         ├─── SubCollection[lines]
  │         ├─── Where[YEAR(ship_date) == 1992]
  │         ├─── Where[RANKING(by=(quantity.DESC(na_pos='last'), line_number.ASC(na_pos='first')), levels=4, allow_ties=False) == 1]
  │         └─┬─ Singular
  │           └─── SubCollection[part]
  └─── OrderBy[region_name.ASC(na_pos='first')]
        """,
            id="regional_first_order_best_line_part",
        ),
        pytest.param(
            orders_versus_first_orders,
            """
──┬─ TPCH
  ├─── TableCollection[orders]
  ├─┬─ Calculate[customer_name=$1.customer_name, order_key=key, days_since_first_order=DATEDIFF('days', $1.order_date, order_date)]
  │ └─┬─ AccessChild
  │   ├─── SubCollection[customer]
  │   ├─┬─ Where[$1.name == 'VIETNAM']
  │   │ └─┬─ AccessChild
  │   │   └─── SubCollection[nation]
  │   └─┬─ Calculate[customer_name=name]
  │     ├─── SubCollection[orders]
  │     ├─── Where[RANKING(by=(order_date.ASC(na_pos='first'), key.ASC(na_pos='first')), levels=1, allow_ties=False) == 1]
  │     └─── Singular
  └─── TopK[5, days_since_first_order.DESC(na_pos='last'), customer_name.ASC(na_pos='first')]
        """,
            id="orders_versus_first_orders",
        ),
        pytest.param(
            absurd_window_per,
            """
──┬─ TPCH
  └─┬─ TableCollection[regions]
    └─┬─ SubCollection[nations]
      └─┬─ SubCollection[customers]
        └─┬─ SubCollection[orders]
          └─┬─ SubCollection[lines]
            └─┬─ SubCollection[supplier]
              └─┬─ SubCollection[nation]
                └─┬─ SubCollection[suppliers]
                  └─┬─ SubCollection[nation]
                    └─┬─ SubCollection[customers]
                      └─┬─ SubCollection[nation]
                        ├─── SubCollection[region]
                        ├─── Calculate[w1=RELSIZE(by=(), levels=11), w2=RELSIZE(by=(), levels=10)]
                        ├─── Calculate[w3=RELSIZE(by=(), levels=9), w4=RELSIZE(by=(), levels=8)]
                        ├─── Calculate[w5=RELSIZE(by=(), levels=7), w6=RELSIZE(by=(), levels=6)]
                        ├─── Calculate[w7=RELSIZE(by=(), levels=5), w8=RELSIZE(by=(), levels=4)]
                        ├─── Calculate[w9=RELSIZE(by=(), levels=3), w10=RELSIZE(by=(), levels=2)]
                        └─── Calculate[w11=RELSIZE(by=(), levels=1), w12=RELSIZE(by=())]
""",
            id="absurd_window_per",
        ),
        pytest.param(
            absurd_partition_window_per,
            """
──┬─ TPCH
  └─┬─ Partition[name='groups', by=ship_mode]
    ├─┬─ AccessChild
    │ └─┬─ Partition[name='groups', by=(ship_mode, ship_date)]
    │   └─┬─ AccessChild
    │     └─┬─ Partition[name='groups', by=(ship_mode, ship_date, status)]
    │       └─┬─ AccessChild
    │         └─┬─ Partition[name='groups', by=(ship_mode, ship_date, status, return_flag)]
    │           └─┬─ AccessChild
    │             └─┬─ Partition[name='groups', by=(ship_mode, ship_date, status, return_flag, part_key)]
    │               └─┬─ AccessChild
    │                 └─── TableCollection[lines]
    └─┬─ PartitionChild[groups]
      └─┬─ PartitionChild[groups]
        └─┬─ PartitionChild[groups]
          └─┬─ PartitionChild[groups]
            └─┬─ PartitionChild[lines]
              ├─── SubCollection[order]
              └─── Calculate[w1=RELSIZE(by=(), levels=2), w2=RELSIZE(by=(), levels=3), w3=RELSIZE(by=(), levels=4), w4=RELSIZE(by=(), levels=5), w5=RELSIZE(by=(), levels=6)]
""",
            id="absurd_partition_window_per",
        ),
        pytest.param(
            simple_cross_1,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Calculate[r1=name]
    └─┬─ TPCH
      ├─── TableCollection[regions]
      ├─── Calculate[r1=r1, r2=name]
      └─── OrderBy[r1.ASC(na_pos='first'), r2.ASC(na_pos='first')]
            """,
            id="simple_cross_1",
        ),
        pytest.param(
            simple_cross_2,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Calculate[r1=name]
    └─┬─ TPCH
      ├─── TableCollection[regions]
      ├─── Calculate[r1=r1, r2=name]
      ├─── Where[r1 != r2]
      └─── OrderBy[r1.ASC(na_pos='first'), r2.ASC(na_pos='first')]
            """,
            id="simple_cross_2",
        ),
        pytest.param(
            simple_cross_3,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  └─┬─ Where[name == 'ASIA']
    ├─── SubCollection[nations]
    └─┬─ Calculate[s_key=key, supplier_nation=name]
      └─┬─ TPCH
        ├─── TableCollection[regions]
        └─┬─ Where[name == 'AMERICA']
          ├─── SubCollection[nations]
          ├─── Calculate[customer_nation=name]
          ├─┬─ Calculate[supplier_nation=supplier_nation, customer_nation=customer_nation, nation_combinations=COUNT($1)]
          │ └─┬─ AccessChild
          │   ├─── SubCollection[customers]
          │   └─┬─ Where[account_balance < 0]
          │     ├─── SubCollection[orders]
          │     └─┬─ Where[(YEAR(order_date) == 1992) & (MONTH(order_date) == 4)]
          │       ├─── SubCollection[lines]
          │       └─┬─ Where[($1.nation_key == s_key) & (ship_mode == 'SHIP')]
          │         └─┬─ AccessChild
          │           └─── SubCollection[supplier]
          └─┬─ Where[HAS($1)]
            └─┬─ AccessChild
              ├─── SubCollection[customers]
              └─┬─ Where[account_balance < 0]
                ├─── SubCollection[orders]
                └─┬─ Where[(YEAR(order_date) == 1992) & (MONTH(order_date) == 4)]
                  ├─── SubCollection[lines]
                  └─┬─ Where[($1.nation_key == s_key) & (ship_mode == 'SHIP')]
                    └─┬─ AccessChild
                      └─── SubCollection[supplier]            
            """,
            id="simple_cross_3",
        ),
        pytest.param(
            simple_cross_4,
            """
──┬─ TPCH
  ├─── TableCollection[regions]
  ├─── Calculate[region_name=name]
  ├─┬─ Calculate[region_name=region_name, n_other_regions=COUNT($1)]
  │ └─┬─ AccessChild
  │   └─┬─ TPCH
  │     ├─── TableCollection[regions]
  │     └─── Where[(name != region_name) & (SLICE(name, None, 1, None) == SLICE(region_name, None, 1, None))]
  └─── OrderBy[region_name.ASC(na_pos='first')]
            """,
            id="simple_cross_4",
        ),
        pytest.param(
            simple_cross_5,
            """
──┬─ TPCH
  ├─┬─ Partition[name='sizes', by=size]
  │ └─┬─ AccessChild
  │   ├─── TableCollection[parts]
  │   └─── Where[STARTSWITH(container, 'LG')]
  ├─── Calculate[part_size=size]
  ├─── TopK[10, size.ASC(na_pos='first')]
  └─┬─ Calculate[part_size=part_size, best_order_priority=$1.order_priority, best_order_priority_qty=$1.total_qty]
    └─┬─ AccessChild
      └─┬─ TPCH
        ├─┬─ Partition[name='priorities', by=order_priority]
        │ └─┬─ AccessChild
        │   ├─── TableCollection[orders]
        │   ├─── Calculate[order_priority=order_priority]
        │   └─┬─ Where[(YEAR(order_date) == 1998) & (MONTH(order_date) == 1)]
        │     ├─── SubCollection[lines]
        │     └─┬─ Where[($1.size == part_size) & (tax == 0) & (discount == 0) & (ship_mode == 'SHIP') & STARTSWITH($1.container, 'LG')]
        │       └─┬─ AccessChild
        │         └─── SubCollection[part]
        ├─┬─ Calculate[total_qty=SUM($1.quantity)]
        │ └─┬─ AccessChild
        │   └─── PartitionChild[lines]
        ├─── Where[RANKING(by=(total_qty.DESC(na_pos='last')), levels=2, allow_ties=False) == 1]
        └─── Singular
            """,
            id="simple_cross_5",
        ),
        pytest.param(
            simple_cross_6,
            """
┌─── TPCH
└─┬─ Calculate[n_pairs=COUNT($1)]
  └─┬─ AccessChild
    ├─── TableCollection[orders]
    ├─── Calculate[original_customer_key=customer_key, original_order_key=key, original_order_date=order_date]
    └─┬─ Where[INTEGER(SLICE(clerk, 6, None, None)) >= 900]
      └─┬─ TPCH
        ├─── TableCollection[orders]
        ├─── Where[INTEGER(SLICE(clerk, 6, None, None)) >= 900]
        └─── Where[(customer_key == original_customer_key) & (key > original_order_key) & (order_date == original_order_date)]
  """,
            id="simple_cross_6",
        ),
    ],
)
def test_qualify_node_to_ast_string(
    impl: Callable[..., UnqualifiedNode],
    answer_tree_str: str,
    empty_sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    assert empty_sqlite_tpch_session.metadata is not None
    unqualified: UnqualifiedNode = init_pydough_context(
        empty_sqlite_tpch_session.metadata
    )(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, empty_sqlite_tpch_session)
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
  ├─── TableCollection[suppliers]
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
  ├─── TableCollection[suppliers]
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
  ├─── TableCollection[suppliers]
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
  ├─── TableCollection[suppliers]
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
    impl: Callable[..., UnqualifiedNode],
    answer_tree_str: str,
    collation_default_asc: bool,
    propagate_collation: bool,
    empty_sqlite_tpch_session: PyDoughSession,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation.
    """
    custom_config: PyDoughConfigs = PyDoughConfigs()
    custom_config.collation_default_asc = collation_default_asc
    custom_config.propagate_collation = propagate_collation
    assert empty_sqlite_tpch_session.metadata is not None
    empty_sqlite_tpch_session.config = custom_config
    unqualified: UnqualifiedNode = init_pydough_context(
        empty_sqlite_tpch_session.metadata
    )(impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, empty_sqlite_tpch_session)
    assert isinstance(qualified, PyDoughCollectionQDAG), (
        "Expected qualified answer to be a collection, not an expression"
    )
    assert qualified.to_tree_string() == answer_tree_str.strip(), (
        "Mismatch between tree string representation of qualified node and expected QDAG tree string"
    )
