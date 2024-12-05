"""
TODO: add file-level docstring.
"""

from collections.abc import Callable

import pytest
from test_qualification import (
    pydough_impl_tpch_q1,
    pydough_impl_tpch_q2,
    pydough_impl_tpch_q3,
    pydough_impl_tpch_q4,
    pydough_impl_tpch_q5,
    pydough_impl_tpch_q6,
    pydough_impl_tpch_q7,
    pydough_impl_tpch_q8,
    pydough_impl_tpch_q9,
    pydough_impl_tpch_q10,
    pydough_impl_tpch_q11,
    pydough_impl_tpch_q12,
    pydough_impl_tpch_q13,
    pydough_impl_tpch_q14,
    pydough_impl_tpch_q15,
    pydough_impl_tpch_q16,
    pydough_impl_tpch_q17,
    pydough_impl_tpch_q18,
    pydough_impl_tpch_q19,
    pydough_impl_tpch_q20,
    pydough_impl_tpch_q21,
    pydough_impl_tpch_q22,
)
from test_utils import (
    graph_fetcher,
)

from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.metadata import GraphMetadata
from pydough.pydough_ast import PyDoughCollectionAST
from pydough.relational import Relational
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                pydough_impl_tpch_q1,
                """
ROOT(columns=[('l_returnflag', l_returnflag), ('l_linestatus', l_linestatus), ('sum_qty', sum_qty), ('sum_base_price', sum_base_price), ('sum_disc_price', sum_disc_price), ('sum_charge', sum_charge), ('avg_qty', avg_qty), ('avg_price', avg_price), ('avg_disc', avg_disc), ('count_order', count_order)], orderings=[(ordering_8):asc_last, (ordering_9):asc_last])
 PROJECT(columns={'avg_disc': avg_disc, 'avg_price': avg_price, 'avg_qty': avg_qty, 'count_order': count_order, 'l_linestatus': l_linestatus, 'l_returnflag': l_returnflag, 'ordering_8': return_flag, 'ordering_9': status, 'sum_base_price': sum_base_price, 'sum_charge': sum_charge, 'sum_disc_price': sum_disc_price, 'sum_qty': sum_qty})
  PROJECT(columns={'avg_disc': agg_0, 'avg_price': agg_1, 'avg_qty': agg_2, 'count_order': DEFAULT_TO(agg_3, 0:int64), 'l_linestatus': status, 'l_returnflag': return_flag, 'return_flag': return_flag, 'status': status, 'sum_base_price': DEFAULT_TO(agg_4, 0:int64), 'sum_charge': DEFAULT_TO(agg_5, 0:int64), 'sum_disc_price': DEFAULT_TO(agg_6, 0:int64), 'sum_qty': DEFAULT_TO(agg_7, 0:int64)})
   AGGREGATE(keys={'return_flag': return_flag, 'status': status}, aggregations={'agg_0': AVG(discount), 'agg_1': AVG(extended_price), 'agg_2': AVG(quantity), 'agg_3': COUNT(), 'agg_4': SUM(extended_price), 'agg_5': SUM(extended_price * 1:int64 - discount * 1:int64 + tax), 'agg_6': SUM(extended_price * 1:int64 - discount), 'agg_7': SUM(quantity)})
    FILTER(condition=ship_date <= datetime.date(1998, 12, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'quantity': quantity, 'return_flag': return_flag, 'status': status, 'tax': tax})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'quantity': l_quantity, 'return_flag': l_returnflag, 'ship_date': l_shipdate, 'status': l_linestatus, 'tax': l_tax})
""",
            ),
            id="tpch_q1",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q2,
                """
ROOT(columns=[('s_acctbal', s_acctbal), ('s_name', s_name), ('n_name', n_name), ('p_partkey', p_partkey), ('p_mfgr', p_mfgr), ('s_address', s_address), ('s_phone', s_phone), ('s_comment', s_comment)], orderings=[(ordering_1):desc_last, (ordering_2):asc_last, (ordering_3):asc_last, (ordering_4):asc_last])
 PROJECT(columns={'n_name': n_name, 'ordering_1': s_acctbal, 'ordering_2': n_name, 'ordering_3': s_name, 'ordering_4': p_partkey, 'p_mfgr': p_mfgr, 'p_partkey': p_partkey, 's_acctbal': s_acctbal, 's_address': s_address, 's_comment': s_comment, 's_name': s_name, 's_phone': s_phone})
  PROJECT(columns={'n_name': n_name, 'p_mfgr': manufacturer, 'p_partkey': key_19, 's_acctbal': s_acctbal, 's_address': s_address, 's_comment': s_comment, 's_name': s_name, 's_phone': s_phone})
   FILTER(condition=supplycost_21 == best_cost & ENDSWITH(part_type, 'BRASS':string) & size == 15:int64, columns={'key_19': key_19, 'manufacturer': manufacturer, 'n_name': n_name, 's_acctbal': s_acctbal, 's_address': s_address, 's_comment': s_comment, 's_name': s_name, 's_phone': s_phone})
    JOIN(conditions=[t0.key_9 == t1.key_19], types=['inner'], columns={'best_cost': t0.best_cost, 'key_19': t1.key_19, 'manufacturer': t1.manufacturer, 'n_name': t1.n_name, 'part_type': t1.part_type, 's_acctbal': t1.s_acctbal, 's_address': t1.s_address, 's_comment': t1.s_comment, 's_name': t1.s_name, 's_phone': t1.s_phone, 'size': t1.size, 'supplycost_21': t1.supplycost})
     PROJECT(columns={'best_cost': agg_0, 'key_9': key_9})
      AGGREGATE(keys={'key_9': key_9}, aggregations={'agg_0': MIN(supplycost)})
       JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'key_9': t1.key, 'supplycost': t0.supplycost})
        JOIN(conditions=[t0.key_5 == t1.supplier_key], types=['inner'], columns={'part_key': t1.part_key, 'supplycost': t1.supplycost})
         JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'key_5': t1.key})
          FILTER(condition=name_3 == 'EUROPE':string, columns={'key': key})
           JOIN(conditions=[t0.region_key == t1.key], types=['left'], columns={'key': t0.key, 'name_3': t1.name})
            SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
            SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
          SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
         SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
        SCAN(table=tpch.PART, columns={'key': p_partkey})
     PROJECT(columns={'key_19': key_19, 'manufacturer': manufacturer, 'n_name': name, 'part_type': part_type, 's_acctbal': account_balance, 's_address': address, 's_comment': comment_14, 's_name': name_16, 's_phone': phone, 'size': size, 'supplycost': supplycost})
      JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'account_balance': t0.account_balance, 'address': t0.address, 'comment_14': t0.comment_14, 'key_19': t1.key, 'manufacturer': t1.manufacturer, 'name': t0.name, 'name_16': t0.name_16, 'part_type': t1.part_type, 'phone': t0.phone, 'size': t1.size, 'supplycost': t0.supplycost})
       JOIN(conditions=[t0.key_15 == t1.supplier_key], types=['inner'], columns={'account_balance': t0.account_balance, 'address': t0.address, 'comment_14': t0.comment_14, 'name': t0.name, 'name_16': t0.name_16, 'part_key': t1.part_key, 'phone': t0.phone, 'supplycost': t1.supplycost})
        JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'account_balance': t1.account_balance, 'address': t1.address, 'comment_14': t1.comment, 'key_15': t1.key, 'name': t0.name, 'name_16': t1.name, 'phone': t1.phone})
         FILTER(condition=name_13 == 'EUROPE':string, columns={'key': key, 'name': name})
          JOIN(conditions=[t0.region_key == t1.key], types=['left'], columns={'key': t0.key, 'name': t0.name, 'name_13': t1.name})
           SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
           SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
         SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'address': s_address, 'comment': s_comment, 'key': s_suppkey, 'name': s_name, 'nation_key': s_nationkey, 'phone': s_phone})
        SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
       SCAN(table=tpch.PART, columns={'key': p_partkey, 'manufacturer': p_mfgr, 'part_type': p_type, 'size': p_size})
""",
            ),
            id="tpch_q2",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q3,
                """
ROOT(columns=[('l_orderkey', l_orderkey), ('revenue', revenue), ('o_orderdate', o_orderdate), ('o_shippriority', o_shippriority)], orderings=[(ordering_1):desc_last, (ordering_2):asc_last, (ordering_3):asc_last])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'l_orderkey': l_orderkey, 'o_orderdate': o_orderdate, 'o_shippriority': o_shippriority, 'ordering_1': ordering_1, 'ordering_2': ordering_2, 'ordering_3': ordering_3, 'revenue': revenue}, orderings=[(ordering_1):desc_last, (ordering_2):asc_last, (ordering_3):asc_last])
  PROJECT(columns={'l_orderkey': l_orderkey, 'o_orderdate': o_orderdate, 'o_shippriority': o_shippriority, 'ordering_1': revenue, 'ordering_2': o_orderdate, 'ordering_3': l_orderkey, 'revenue': revenue})
   PROJECT(columns={'l_orderkey': order_key, 'o_orderdate': order_date, 'o_shippriority': ship_priority, 'revenue': DEFAULT_TO(agg_0, 0:int64)})
    AGGREGATE(keys={'order_date': order_date, 'order_key': order_key, 'ship_priority': ship_priority}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
     FILTER(condition=ship_date > datetime.date(1995, 3, 15):date, columns={'discount': discount, 'extended_price': extended_price, 'order_date': order_date, 'order_key': order_key, 'ship_priority': ship_priority})
      JOIN(conditions=[t0.key == t1.order_key], types=['inner'], columns={'discount': t1.discount, 'extended_price': t1.extended_price, 'order_date': t0.order_date, 'order_key': t1.order_key, 'ship_date': t1.ship_date, 'ship_priority': t0.ship_priority})
       FILTER(condition=mktsegment == 'BUILDING':string & order_date < datetime.date(1995, 3, 15):date, columns={'key': key, 'order_date': order_date, 'ship_priority': ship_priority})
        JOIN(conditions=[t0.customer_key == t1.key], types=['left'], columns={'key': t0.key, 'mktsegment': t1.mktsegment, 'order_date': t0.order_date, 'ship_priority': t0.ship_priority})
         SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate, 'ship_priority': o_shipriority})
         SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'mktsegment': c_mktsegment})
       SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'ship_date': l_shipdate})
""",
            ),
            id="tpch_q3",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q4,
                """
ROOT(columns=[('order_priority', order_priority), ('order_count', order_count)], orderings=[(ordering_3):asc_last])
 PROJECT(columns={'order_count': order_count, 'order_priority': order_priority, 'ordering_3': order_priority})
  PROJECT(columns={'order_count': DEFAULT_TO(agg_2, 0:int64), 'order_priority': order_priority})
   JOIN(conditions=[True:bool], types=['left'], columns={'agg_2': t1.agg_2, 'order_priority': t0.order_priority})
    AGGREGATE(keys={'order_priority': order_priority}, aggregations={})
     FILTER(condition=order_date >= datetime.date(1993, 7, 1):date & order_date < datetime.date(1993, 10, 1):date & DEFAULT_TO(agg_0, 0:int64) > 0:int64, columns={'order_priority': order_priority})
      JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'order_date': t0.order_date, 'order_priority': t0.order_priority})
       SCAN(table=tpch.ORDER, columns={'key': o_orderkey, 'order_date': o_orderdate, 'order_priority': o_orderpriority})
       AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': COUNT()})
        FILTER(condition=commit_date < receipt_date, columns={'order_key': order_key})
         SCAN(table=tpch.LINEITEM, columns={'commit_date': l_commitdate, 'order_key': l_orderkey, 'receipt_date': l_receiptdate})
    AGGREGATE(keys={'order_priority': order_priority}, aggregations={'agg_2': COUNT()})
     FILTER(condition=order_date >= datetime.date(1993, 7, 1):date & order_date < datetime.date(1993, 10, 1):date & DEFAULT_TO(agg_1, 0:int64) > 0:int64, columns={'order_priority': order_priority})
      JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_1': t1.agg_1, 'order_date': t0.order_date, 'order_priority': t0.order_priority})
       SCAN(table=tpch.ORDER, columns={'key': o_orderkey, 'order_date': o_orderdate, 'order_priority': o_orderpriority})
       AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_1': COUNT()})
        FILTER(condition=commit_date < receipt_date, columns={'order_key': order_key})
         SCAN(table=tpch.LINEITEM, columns={'commit_date': l_commitdate, 'order_key': l_orderkey, 'receipt_date': l_receiptdate})
""",
            ),
            id="tpch_q4",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q5,
                """
""",
            ),
            id="tpch_q5",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                pydough_impl_tpch_q6,
                """
ROOT(columns=[('revenue', revenue)], orderings=[])
 PROJECT(columns={'revenue': DEFAULT_TO(agg_0, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(amt)})
   PROJECT(columns={'amt': extended_price * discount})
    FILTER(condition=ship_date >= datetime.date(1994, 1, 1):date & ship_date < datetime.date(1995, 1, 1):date & discount > 0.05:float64 & discount < 0.07:float64 & quantity < 24:int64, columns={'discount': discount, 'extended_price': extended_price})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'quantity': l_quantity, 'ship_date': l_shipdate})
""",
            ),
            id="tpch_q6",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q7,
                """
ROOT(columns=[('supp_nation', supp_nation), ('cust_nation', cust_nation), ('l_year', l_year), ('revenue', revenue)], orderings=[(ordering_1):asc_last, (ordering_2):asc_last, (ordering_3):asc_last])
 PROJECT(columns={'cust_nation': cust_nation, 'l_year': l_year, 'ordering_1': supp_nation, 'ordering_2': cust_nation, 'ordering_3': l_year, 'revenue': revenue, 'supp_nation': supp_nation})
  PROJECT(columns={'cust_nation': cust_nation, 'l_year': l_year, 'revenue': DEFAULT_TO(agg_0, 0:int64), 'supp_nation': supp_nation})
   AGGREGATE(keys={'cust_nation': cust_nation, 'l_year': l_year, 'supp_nation': supp_nation}, aggregations={'agg_0': SUM(volume)})
    FILTER(condition=ship_date >= datetime.date(1995, 1, 1):date & ship_date <= datetime.date(1996, 12, 31):date & supp_nation == 'France':string & cust_nation == 'Germany':string | supp_nation == 'Germany':string & cust_nation == 'France':string, columns={'cust_nation': cust_nation, 'l_year': l_year, 'supp_nation': supp_nation, 'volume': volume})
     PROJECT(columns={'cust_nation': name_8, 'l_year': YEAR(ship_date), 'ship_date': ship_date, 'supp_nation': name_3, 'volume': extended_price * 1:int64 - discount})
      JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_3': t0.name_3, 'name_8': t1.name_8, 'ship_date': t0.ship_date})
       JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_3': t1.name_3, 'order_key': t0.order_key, 'ship_date': t0.ship_date})
        SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
        JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_3': t1.name})
         SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
         SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
       JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_8': t1.name})
        JOIN(conditions=[t0.customer_key == t1.key], types=['inner'], columns={'key': t0.key, 'nation_key': t1.nation_key})
         SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
         SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
        SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
            ),
            id="tpch_q7",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q8,
                """
ROOT(columns=[('o_year', o_year), ('mkt_share', mkt_share)], orderings=[])
 PROJECT(columns={'mkt_share': DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64), 'o_year': o_year})
  AGGREGATE(keys={'o_year': o_year}, aggregations={'agg_0': SUM(brazil_volume), 'agg_1': SUM(volume)})
   FILTER(condition=order_date >= datetime.date(1995, 1, 1):date & order_date <= datetime.date(1996, 12, 31):date & name_18 == 'AMERICA':string, columns={'brazil_volume': brazil_volume, 'o_year': o_year, 'volume': volume})
    JOIN(conditions=[t0.customer_key == t1.key], types=['left'], columns={'brazil_volume': t0.brazil_volume, 'name_18': t1.name_18, 'o_year': t0.o_year, 'order_date': t0.order_date, 'volume': t0.volume})
     PROJECT(columns={'brazil_volume': IFF(name == 'BRAZIL':string, volume, 0:int64), 'customer_key': customer_key, 'o_year': YEAR(order_date), 'order_date': order_date, 'volume': volume})
      JOIN(conditions=[t0.order_key == t1.key], types=['inner'], columns={'customer_key': t1.customer_key, 'name': t0.name, 'order_date': t1.order_date, 'volume': t0.volume})
       PROJECT(columns={'name': name, 'order_key': order_key, 'volume': extended_price * 1:int64 - discount})
        JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['inner'], columns={'discount': t1.discount, 'extended_price': t1.extended_price, 'name': t0.name, 'order_key': t1.order_key})
         FILTER(condition=part_type == 'ECONOMY ANODIZED STEEL':string, columns={'name': name, 'part_key': part_key, 'supplier_key': supplier_key})
          JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'name': t0.name, 'part_key': t0.part_key, 'part_type': t1.part_type, 'supplier_key': t0.supplier_key})
           JOIN(conditions=[t0.key_2 == t1.supplier_key], types=['inner'], columns={'name': t0.name, 'part_key': t1.part_key, 'supplier_key': t1.supplier_key})
            JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'key_2': t1.key, 'name': t0.name})
             SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
             SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
            SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
           SCAN(table=tpch.PART, columns={'key': p_partkey, 'part_type': p_type})
         SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'part_key': l_partkey, 'supplier_key': l_suppkey})
       SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate})
     JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_18': t1.name})
      JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'region_key': t1.region_key})
       SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
       SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
      SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            ),
            id="tpch_q8",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q9,
                """
ROOT(columns=[('nation', nation), ('o_year', o_year), ('amount', amount)], orderings=[(ordering_1):asc_last, (ordering_2):desc_last])
 PROJECT(columns={'amount': amount, 'nation': nation, 'o_year': o_year, 'ordering_1': nation, 'ordering_2': o_year})
  PROJECT(columns={'amount': DEFAULT_TO(agg_0, 0:int64), 'nation': nation, 'o_year': o_year})
   AGGREGATE(keys={'nation': nation, 'o_year': o_year}, aggregations={'agg_0': SUM(value)})
    PROJECT(columns={'nation': name, 'o_year': YEAR(order_date), 'value': extended_price * 1:int64 - discount - supplycost * quantity})
     JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name': t0.name, 'order_date': t1.order_date, 'quantity': t0.quantity, 'supplycost': t0.supplycost})
      JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['inner'], columns={'discount': t1.discount, 'extended_price': t1.extended_price, 'name': t0.name, 'order_key': t1.order_key, 'quantity': t1.quantity, 'supplycost': t0.supplycost})
       FILTER(condition=CONTAINS(name_7, 'green':string), columns={'name': name, 'part_key': part_key, 'supplier_key': supplier_key, 'supplycost': supplycost})
        JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'name': t0.name, 'name_7': t1.name, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key, 'supplycost': t0.supplycost})
         JOIN(conditions=[t0.key_2 == t1.supplier_key], types=['inner'], columns={'name': t0.name, 'part_key': t1.part_key, 'supplier_key': t1.supplier_key, 'supplycost': t1.supplycost})
          JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'key_2': t1.key, 'name': t0.name})
           SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
           SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
          SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
         SCAN(table=tpch.PART, columns={'key': p_partkey, 'name': p_name})
       SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'part_key': l_partkey, 'quantity': l_quantity, 'supplier_key': l_suppkey})
      SCAN(table=tpch.ORDER, columns={'key': o_orderkey, 'order_date': o_orderdate})
""",
            ),
            id="tpch_q9",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q10,
                """
ROOT(columns=[('c_key', c_key), ('c_name', c_name), ('revenue', revenue), ('c_acctbal', c_acctbal), ('n_name', n_name), ('c_address', c_address), ('c_phone', c_phone), ('c_comment', c_comment)], orderings=[(ordering_1):desc_last, (ordering_2):asc_last])
 LIMIT(limit=Literal(value=20, type=Int64Type()), columns={'c_acctbal': c_acctbal, 'c_address': c_address, 'c_comment': c_comment, 'c_key': c_key, 'c_name': c_name, 'c_phone': c_phone, 'n_name': n_name, 'ordering_1': ordering_1, 'ordering_2': ordering_2, 'revenue': revenue}, orderings=[(ordering_1):desc_last, (ordering_2):asc_last])
  PROJECT(columns={'c_acctbal': c_acctbal, 'c_address': c_address, 'c_comment': c_comment, 'c_key': c_key, 'c_name': c_name, 'c_phone': c_phone, 'n_name': n_name, 'ordering_1': revenue, 'ordering_2': c_key, 'revenue': revenue})
   PROJECT(columns={'c_acctbal': acctbal, 'c_address': address, 'c_comment': comment, 'c_key': key, 'c_name': name, 'c_phone': phone, 'n_name': name_4, 'revenue': DEFAULT_TO(agg_0, 0:int64)})
    JOIN(conditions=[t0.nation_key == t1.key], types=['left'], columns={'acctbal': t0.acctbal, 'address': t0.address, 'agg_0': t0.agg_0, 'comment': t0.comment, 'key': t0.key, 'name': t0.name, 'name_4': t1.name, 'phone': t0.phone})
     JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'acctbal': t0.acctbal, 'address': t0.address, 'agg_0': t1.agg_0, 'comment': t0.comment, 'key': t0.key, 'name': t0.name, 'nation_key': t0.nation_key, 'phone': t0.phone})
      SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'address': c_address, 'comment': c_comment, 'key': c_custkey, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
      AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_0': SUM(amt)})
       PROJECT(columns={'amt': extended_price * 1:int64 - discount, 'customer_key': customer_key})
        FILTER(condition=return_flag == 'R':string, columns={'customer_key': customer_key, 'discount': discount, 'extended_price': extended_price})
         JOIN(conditions=[t0.key == t1.order_key], types=['inner'], columns={'customer_key': t0.customer_key, 'discount': t1.discount, 'extended_price': t1.extended_price, 'return_flag': t1.return_flag})
          FILTER(condition=order_date >= datetime.date(1993, 10, 1):date & order_date < datetime.date(1994, 1, 1):date, columns={'customer_key': customer_key, 'key': key})
           SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate})
          SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'return_flag': l_returnflag})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
            ),
            id="tpch_q10",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q11,
                """
ROOT(columns=[('ps_partkey', ps_partkey), ('val', val)], orderings=[(ordering_2):desc_last])
 PROJECT(columns={'ordering_2': val, 'ps_partkey': ps_partkey, 'val': val})
  FILTER(condition=val > min_market_share, columns={'ps_partkey': ps_partkey, 'val': val})
   PROJECT(columns={'min_market_share': min_market_share, 'ps_partkey': part_key, 'val': DEFAULT_TO(agg_1, 0:int64)})
    JOIN(conditions=[True:bool], types=['left'], columns={'agg_1': t1.agg_1, 'min_market_share': t0.min_market_share, 'part_key': t1.part_key})
     PROJECT(columns={'min_market_share': DEFAULT_TO(agg_0, 0:int64) * 0.0001:float64})
      AGGREGATE(keys={}, aggregations={'agg_0': SUM(metric)})
       PROJECT(columns={'metric': supplycost * availqty})
        FILTER(condition=name_3 == 'GERMANY':string, columns={'availqty': availqty, 'supplycost': supplycost})
         JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'availqty': t0.availqty, 'name_3': t1.name_3, 'supplycost': t0.supplycost})
          SCAN(table=tpch.PARTSUPP, columns={'availqty': ps_availqty, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
          JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_3': t1.name})
           SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
           SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
     AGGREGATE(keys={'part_key': part_key}, aggregations={'agg_1': SUM(metric)})
      PROJECT(columns={'metric': supplycost * availqty, 'part_key': part_key})
       FILTER(condition=name_6 == 'GERMANY':string, columns={'availqty': availqty, 'part_key': part_key, 'supplycost': supplycost})
        JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'availqty': t0.availqty, 'name_6': t1.name_6, 'part_key': t0.part_key, 'supplycost': t0.supplycost})
         SCAN(table=tpch.PARTSUPP, columns={'availqty': ps_availqty, 'part_key': ps_partkey, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
         JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_6': t1.name})
          SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
          SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
            ),
            id="tpch_q11",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q12,
                """
ROOT(columns=[('ship_mode', ship_mode), ('high_line_count', high_line_count), ('low_line_count', low_line_count)], orderings=[(ordering_2):asc_last])
 PROJECT(columns={'high_line_count': high_line_count, 'low_line_count': low_line_count, 'ordering_2': ship_mode, 'ship_mode': ship_mode})
  PROJECT(columns={'high_line_count': DEFAULT_TO(agg_0, 0:int64), 'low_line_count': DEFAULT_TO(agg_1, 0:int64), 'ship_mode': ship_mode})
   AGGREGATE(keys={'ship_mode': ship_mode}, aggregations={'agg_0': SUM(is_high_priority), 'agg_1': SUM(NOT(is_high_priority))})
    PROJECT(columns={'is_high_priority': order_priority == '1-URGENT':string | order_priority == '2-HIGH':string, 'ship_mode': ship_mode})
     JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'order_priority': t1.order_priority, 'ship_mode': t0.ship_mode})
      FILTER(condition=ship_mode == 'MAIL':string | ship_mode == 'SHIP':string & ship_date < commit_date & commit_date < receipt_date & receipt_date >= datetime.date(1994, 1, 1):date & receipt_date < datetime.date(1995, 1, 1):date, columns={'order_key': order_key, 'ship_mode': ship_mode})
       SCAN(table=tpch.LINEITEM, columns={'commit_date': l_commitdate, 'order_key': l_orderkey, 'receipt_date': l_receiptdate, 'ship_date': l_shipdate, 'ship_mode': l_mode})
      SCAN(table=tpch.ORDER, columns={'key': o_orderkey, 'order_priority': o_orderpriority})
""",
            ),
            id="tpch_q12",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q13,
                """
ROOT(columns=[('c_count', c_count), ('custdist', custdist)], orderings=[])
 PROJECT(columns={'c_count': num_non_special_orders, 'custdist': DEFAULT_TO(agg_2, 0:int64)})
  JOIN(conditions=[True:bool], types=['left'], columns={'agg_2': t1.agg_2, 'num_non_special_orders': t0.num_non_special_orders})
   AGGREGATE(keys={'num_non_special_orders': num_non_special_orders}, aggregations={})
    PROJECT(columns={'num_non_special_orders': DEFAULT_TO(agg_0, 0:int64)})
     JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'agg_0': t1.agg_0})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey})
      AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_0': COUNT()})
       FILTER(condition=NOT(LIKE(comment, '%special%requests%':string)), columns={'customer_key': customer_key})
        SCAN(table=tpch.ORDER, columns={'comment': o_comment, 'customer_key': o_custkey})
   AGGREGATE(keys={'num_non_special_orders': num_non_special_orders}, aggregations={'agg_2': COUNT()})
    PROJECT(columns={'num_non_special_orders': DEFAULT_TO(agg_1, 0:int64)})
     JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'agg_1': t1.agg_1})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey})
      AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_1': COUNT()})
       FILTER(condition=NOT(LIKE(comment, '%special%requests%':string)), columns={'customer_key': customer_key})
        SCAN(table=tpch.ORDER, columns={'comment': o_comment, 'customer_key': o_custkey})
""",
            ),
            id="tpch_q13",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q14,
                """
ROOT(columns=[('promo_revenue', promo_revenue)], orderings=[])
 PROJECT(columns={'promo_revenue': 100.0:float64 * DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(promo_value), 'agg_1': SUM(value)})
   PROJECT(columns={'promo_value': IFF(STARTSWITH(part_type, 'PROMO':string), extended_price * 1:int64 - discount, 0:int64), 'value': extended_price * 1:int64 - discount})
    JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'part_type': t1.part_type})
     FILTER(condition=ship_date >= datetime.date(1995, 9, 1):date & ship_date < datetime.date(1995, 10, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'part_key': part_key})
      SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'ship_date': l_shipdate})
     SCAN(table=tpch.PART, columns={'key': p_partkey, 'part_type': p_type})
""",
            ),
            id="tpch_q14",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q15,
                """
ROOT(columns=[('s_suppkey', s_suppkey), ('s_name', s_name), ('s_address', s_address), ('s_phone', s_phone), ('total_revenue', total_revenue)], orderings=[(ordering_3):asc_last])
 PROJECT(columns={'ordering_3': s_suppkey, 's_address': s_address, 's_name': s_name, 's_phone': s_phone, 's_suppkey': s_suppkey, 'total_revenue': total_revenue})
  FILTER(condition=total_revenue == max_revenue, columns={'s_address': s_address, 's_name': s_name, 's_phone': s_phone, 's_suppkey': s_suppkey, 'total_revenue': total_revenue})
   PROJECT(columns={'max_revenue': max_revenue, 's_address': address, 's_name': name, 's_phone': phone, 's_suppkey': key, 'total_revenue': DEFAULT_TO(agg_2, 0:int64)})
    JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'address': t0.address, 'agg_2': t1.agg_2, 'key': t0.key, 'max_revenue': t0.max_revenue, 'name': t0.name, 'phone': t0.phone})
     JOIN(conditions=[True:bool], types=['inner'], columns={'address': t1.address, 'key': t1.key, 'max_revenue': t0.max_revenue, 'name': t1.name, 'phone': t1.phone})
      PROJECT(columns={'max_revenue': agg_1})
       AGGREGATE(keys={}, aggregations={'agg_1': MAX(total_revenue)})
        PROJECT(columns={'total_revenue': DEFAULT_TO(agg_0, 0:int64)})
         JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'agg_0': t1.agg_0})
          SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey})
          AGGREGATE(keys={'supplier_key': supplier_key}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
           FILTER(condition=ship_date >= datetime.date(1996, 1, 1):date & ship_date < datetime.date(1996, 3, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'supplier_key': supplier_key})
            SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
      SCAN(table=tpch.SUPPLIER, columns={'address': s_address, 'key': s_suppkey, 'name': s_name, 'phone': s_phone})
     AGGREGATE(keys={'supplier_key': supplier_key}, aggregations={'agg_2': SUM(extended_price * 1:int64 - discount)})
      FILTER(condition=ship_date >= datetime.date(1996, 1, 1):date & ship_date < datetime.date(1996, 3, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'supplier_key': supplier_key})
       SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
""",
            ),
            id="tpch_q15",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q16,
                """
ROOT(columns=[('p_brand', p_brand), ('p_type', p_type), ('p_size', p_size), ('supplier_cnt', supplier_cnt)], orderings=[])
 PROJECT(columns={'p_brand': p_brand, 'p_size': p_size, 'p_type': p_type, 'supplier_cnt': agg_0})
  AGGREGATE(keys={'p_brand': p_brand, 'p_size': p_size, 'p_type': p_type}, aggregations={'agg_0': NDISTINCT(supplier_key)})
   FILTER(condition=NOT(LIKE(comment_2, '%Customer%Complaints%':string)), columns={'p_brand': p_brand, 'p_size': p_size, 'p_type': p_type, 'supplier_key': supplier_key})
    JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'comment_2': t1.comment, 'p_brand': t0.p_brand, 'p_size': t0.p_size, 'p_type': t0.p_type, 'supplier_key': t0.supplier_key})
     PROJECT(columns={'p_brand': brand, 'p_size': size, 'p_type': part_type, 'supplier_key': supplier_key})
      JOIN(conditions=[t0.key == t1.part_key], types=['inner'], columns={'brand': t0.brand, 'part_type': t0.part_type, 'size': t0.size, 'supplier_key': t1.supplier_key})
       FILTER(condition=brand != 'BRAND#45':string & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%':string)) & ISIN(size, [49:Int64Type(), 14:Int64Type(), 23:Int64Type(), 45:Int64Type(), 19:Int64Type(), 3:Int64Type(), 36:Int64Type(), 9:Int64Type()]:array[unknown]), columns={'brand': brand, 'key': key, 'part_type': part_type, 'size': size})
        SCAN(table=tpch.PART, columns={'brand': p_brand, 'key': p_partkey, 'part_type': p_type, 'size': p_size})
       SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
     SCAN(table=tpch.SUPPLIER, columns={'comment': s_comment, 'key': s_suppkey})
""",
            ),
            id="tpch_q16",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q17,
                """
ROOT(columns=[('avg_yearly', avg_yearly)], orderings=[])
 PROJECT(columns={'avg_yearly': DEFAULT_TO(agg_1, 0:int64) / 7.0:float64})
  AGGREGATE(keys={}, aggregations={'agg_1': SUM(extended_price)})
   FILTER(condition=quantity < 0.2:float64 * avg_quantity, columns={'extended_price': extended_price})
    JOIN(conditions=[t0.key == t1.part_key], types=['inner'], columns={'avg_quantity': t0.avg_quantity, 'extended_price': t1.extended_price, 'quantity': t1.quantity})
     PROJECT(columns={'avg_quantity': agg_0, 'key': key})
      JOIN(conditions=[t0.key == t1.part_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
       FILTER(condition=brand == 'Brand#23':string & container == 'MED BOX':string, columns={'key': key})
        SCAN(table=tpch.PART, columns={'brand': p_brand, 'container': p_container, 'key': p_partkey})
       AGGREGATE(keys={'part_key': part_key}, aggregations={'agg_0': AVG(quantity)})
        SCAN(table=tpch.LINEITEM, columns={'part_key': l_partkey, 'quantity': l_quantity})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'part_key': l_partkey, 'quantity': l_quantity})
""",
            ),
            id="tpch_q17",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q18,
                """
ROOT(columns=[('c_name', c_name), ('c_custkey', c_custkey), ('o_orderkey', o_orderkey), ('o_orderdate', o_orderdate), ('o_totalprice', o_totalprice), ('total_quantity', total_quantity)], orderings=[(ordering_1):desc_last, (ordering_2):asc_last])
 PROJECT(columns={'c_custkey': c_custkey, 'c_name': c_name, 'o_orderdate': o_orderdate, 'o_orderkey': o_orderkey, 'o_totalprice': o_totalprice, 'ordering_1': o_totalprice, 'ordering_2': o_orderdate, 'total_quantity': total_quantity})
  FILTER(condition=total_quantity > 300:int64, columns={'c_custkey': c_custkey, 'c_name': c_name, 'o_orderdate': o_orderdate, 'o_orderkey': o_orderkey, 'o_totalprice': o_totalprice, 'total_quantity': total_quantity})
   PROJECT(columns={'c_custkey': key_2, 'c_name': name, 'o_orderdate': order_date, 'o_orderkey': key, 'o_totalprice': total_price, 'total_quantity': DEFAULT_TO(agg_0, 0:int64)})
    JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key, 'key_2': t0.key_2, 'name': t0.name, 'order_date': t0.order_date, 'total_price': t0.total_price})
     JOIN(conditions=[t0.customer_key == t1.key], types=['left'], columns={'key': t0.key, 'key_2': t1.key, 'name': t1.name, 'order_date': t0.order_date, 'total_price': t0.total_price})
      SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate, 'total_price': o_totalprice})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'name': c_name})
     AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': SUM(quantity)})
      SCAN(table=tpch.LINEITEM, columns={'order_key': l_orderkey, 'quantity': l_quantity})
""",
            ),
            id="tpch_q18",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q19,
                """
ROOT(columns=[('revenue', revenue)], orderings=[])
 PROJECT(columns={'revenue': DEFAULT_TO(agg_0, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
   FILTER(condition=True:bool & ship_instruct == 'DELIVER IN PERSON':string & size >= 1:int64 & size < 5:int64 & quantity >= 1:int64 & quantity <= 11:int64 & ISIN(container, ['SM CASE':StringType(), 'SM BOX':StringType(), 'SM PACK':StringType(), 'SM PKG':StringType()]:array[unknown]) & brand == 'Brand#12':string | size < 10:int64 & quantity >= 10:int64 & quantity <= 21:int64 & ISIN(container, ['MED CASE':StringType(), 'MED BOX':StringType(), 'MED PACK':StringType(), 'MED PKG':StringType()]:array[unknown]) & brand == 'Brand#23':string | size < 15:int64 & quantity >= 20:int64 & quantity <= 31:int64 & ISIN(container, ['LG CASE':StringType(), 'LG BOX':StringType(), 'LG PACK':StringType(), 'LG PKG':StringType()]:array[unknown]) & brand == 'Brand#34':string, columns={'discount': discount, 'extended_price': extended_price})
    JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'brand': t1.brand, 'container': t1.container, 'discount': t0.discount, 'extended_price': t0.extended_price, 'quantity': t0.quantity, 'ship_instruct': t0.ship_instruct, 'size': t1.size})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'quantity': l_quantity, 'ship_instruct': l_shipinstruct})
     SCAN(table=tpch.PART, columns={'brand': p_brand, 'container': p_container, 'key': p_partkey, 'size': p_size})
""",
            ),
            id="tpch_q19",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q20,
                """
ROOT(columns=[('s_name', s_name), ('s_address', s_address)], orderings=[])
 FILTER(condition=name_3 == 'CANADA':string & DEFAULT_TO(agg_1, 0:int64) > 0:int64, columns={'s_address': s_address, 's_name': s_name})
  JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'agg_1': t1.agg_1, 'name_3': t0.name_3, 's_address': t0.s_address, 's_name': t0.s_name})
   JOIN(conditions=[t0.nation_key == t1.key], types=['left'], columns={'key': t0.key, 'name_3': t1.name, 's_address': t0.s_address, 's_name': t0.s_name})
    PROJECT(columns={'key': key, 'nation_key': nation_key, 's_address': address, 's_name': name})
     SCAN(table=tpch.SUPPLIER, columns={'address': s_address, 'key': s_suppkey, 'name': s_name, 'nation_key': s_nationkey})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
   AGGREGATE(keys={'supplier_key': supplier_key}, aggregations={'agg_1': COUNT()})
    FILTER(condition=STARTSWITH(name, 'forest':string) & availqty > DEFAULT_TO(agg_0, 0:int64) * 0.5:float64, columns={'supplier_key': supplier_key})
     JOIN(conditions=[t0.key == t1.part_key], types=['left'], columns={'agg_0': t1.agg_0, 'availqty': t0.availqty, 'name': t0.name, 'supplier_key': t0.supplier_key})
      JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'availqty': t0.availqty, 'key': t1.key, 'name': t1.name, 'supplier_key': t0.supplier_key})
       SCAN(table=tpch.PARTSUPP, columns={'availqty': ps_availqty, 'part_key': ps_partkey, 'supplier_key': ps_suppkey})
       SCAN(table=tpch.PART, columns={'key': p_partkey, 'name': p_name})
      AGGREGATE(keys={'part_key': part_key}, aggregations={'agg_0': SUM(quantity)})
       FILTER(condition=ship_date >= datetime.date(1994, 1, 1):date & ship_date < datetime.date(1995, 1, 1):date, columns={'part_key': part_key, 'quantity': quantity})
        SCAN(table=tpch.LINEITEM, columns={'part_key': l_partkey, 'quantity': l_quantity, 'ship_date': l_shipdate})
""",
            ),
            id="tpch_q20",
        ),
        pytest.param(
            (
                pydough_impl_tpch_q21,
                """
""",
            ),
            id="tpch_q21",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                pydough_impl_tpch_q22,
                """
""",
            ),
            id="tpch_q22",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
    ],
)
def pydough_pipeline_test_data(request):
    """
    Test data for test_pydough_pipeline.
    """
    return request.param


def test_pydough_pipeline(
    pydough_pipeline_test_data: tuple[
        Callable[[UnqualifiedNode], UnqualifiedNode], str
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    impl, answer_str = pydough_pipeline_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = impl(root)
    qualified: PyDoughCollectionAST = qualify_node(unqualified, graph)
    relational: Relational = convert_ast_to_relational(qualified, default_config)
    assert (
        relational.to_tree_string() == answer_str.strip()
    ), "Mismatch between tree string representation of relational node and expected Relational tree string"
