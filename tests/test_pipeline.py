"""
Integration tests for the PyDough workflow on the TPC-H queries.

Copyright (C) 2024 Bodo Inc. All rights reserved.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from simple_pydough_functions import simple_filter_top_five, simple_scan_top_five
from test_utils import (
    graph_fetcher,
)
from tpch_outputs import (
    tpch_q1_output,
    tpch_q2_output,
    tpch_q3_output,
    tpch_q4_output,
    tpch_q5_output,
    tpch_q6_output,
    tpch_q7_output,
    tpch_q8_output,
    tpch_q9_output,
    tpch_q10_output,
    tpch_q11_output,
    tpch_q12_output,
    tpch_q13_output,
    tpch_q14_output,
    tpch_q15_output,
    tpch_q16_output,
    tpch_q17_output,
    tpch_q18_output,
    tpch_q19_output,
    tpch_q20_output,
    tpch_q21_output,
    tpch_q22_output,
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

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.metadata import GraphMetadata
from pydough.pydough_ast import PyDoughAST, PyDoughCollectionAST
from pydough.relational import RelationalRoot
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)

pytestmark = [pytest.mark.execute]


@pytest.fixture(
    params=[
        pytest.param(
            (
                impl_tpch_q1,
                """
ROOT(columns=[('L_RETURNFLAG', L_RETURNFLAG), ('L_LINESTATUS', L_LINESTATUS), ('SUM_QTY', SUM_QTY), ('SUM_BASE_PRICE', SUM_BASE_PRICE), ('SUM_DISC_PRICE', SUM_DISC_PRICE), ('SUM_CHARGE', SUM_CHARGE), ('AVG_QTY', AVG_QTY), ('AVG_PRICE', AVG_PRICE), ('AVG_DISC', AVG_DISC), ('COUNT_ORDER', COUNT_ORDER)], orderings=[(ordering_8):asc_first, (ordering_9):asc_first])
 PROJECT(columns={'AVG_DISC': AVG_DISC, 'AVG_PRICE': AVG_PRICE, 'AVG_QTY': AVG_QTY, 'COUNT_ORDER': COUNT_ORDER, 'L_LINESTATUS': L_LINESTATUS, 'L_RETURNFLAG': L_RETURNFLAG, 'SUM_BASE_PRICE': SUM_BASE_PRICE, 'SUM_CHARGE': SUM_CHARGE, 'SUM_DISC_PRICE': SUM_DISC_PRICE, 'SUM_QTY': SUM_QTY, 'ordering_8': L_RETURNFLAG, 'ordering_9': L_LINESTATUS})
  PROJECT(columns={'AVG_DISC': agg_0, 'AVG_PRICE': agg_1, 'AVG_QTY': agg_2, 'COUNT_ORDER': DEFAULT_TO(agg_3, 0:int64), 'L_LINESTATUS': status, 'L_RETURNFLAG': return_flag, 'SUM_BASE_PRICE': DEFAULT_TO(agg_4, 0:int64), 'SUM_CHARGE': DEFAULT_TO(agg_5, 0:int64), 'SUM_DISC_PRICE': DEFAULT_TO(agg_6, 0:int64), 'SUM_QTY': DEFAULT_TO(agg_7, 0:int64)})
   AGGREGATE(keys={'return_flag': return_flag, 'status': status}, aggregations={'agg_0': AVG(discount), 'agg_1': AVG(extended_price), 'agg_2': AVG(quantity), 'agg_3': COUNT(), 'agg_4': SUM(extended_price), 'agg_5': SUM(extended_price * 1:int64 - discount * 1:int64 + tax), 'agg_6': SUM(extended_price * 1:int64 - discount), 'agg_7': SUM(quantity)})
    FILTER(condition=ship_date <= datetime.date(1998, 12, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'quantity': quantity, 'return_flag': return_flag, 'status': status, 'tax': tax})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'quantity': l_quantity, 'return_flag': l_returnflag, 'ship_date': l_shipdate, 'status': l_linestatus, 'tax': l_tax})""",
                tpch_q1_output,
            ),
            id="tpch_q1",
        ),
        pytest.param(
            (
                impl_tpch_q2,
                """
ROOT(columns=[('S_ACCTBAL', S_ACCTBAL), ('S_NAME', S_NAME), ('N_NAME', N_NAME), ('P_PARTKEY', P_PARTKEY), ('P_MFGR', P_MFGR), ('S_ADDRESS', S_ADDRESS), ('S_PHONE', S_PHONE), ('S_COMMENT', S_COMMENT)], orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first, (ordering_4):asc_first])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'N_NAME': N_NAME, 'P_MFGR': P_MFGR, 'P_PARTKEY': P_PARTKEY, 'S_ACCTBAL': S_ACCTBAL, 'S_ADDRESS': S_ADDRESS, 'S_COMMENT': S_COMMENT, 'S_NAME': S_NAME, 'S_PHONE': S_PHONE, 'ordering_1': ordering_1, 'ordering_2': ordering_2, 'ordering_3': ordering_3, 'ordering_4': ordering_4}, orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first, (ordering_4):asc_first])
  PROJECT(columns={'N_NAME': N_NAME, 'P_MFGR': P_MFGR, 'P_PARTKEY': P_PARTKEY, 'S_ACCTBAL': S_ACCTBAL, 'S_ADDRESS': S_ADDRESS, 'S_COMMENT': S_COMMENT, 'S_NAME': S_NAME, 'S_PHONE': S_PHONE, 'ordering_1': S_ACCTBAL, 'ordering_2': N_NAME, 'ordering_3': S_NAME, 'ordering_4': P_PARTKEY})
   PROJECT(columns={'N_NAME': n_name, 'P_MFGR': manufacturer, 'P_PARTKEY': key_19, 'S_ACCTBAL': s_acctbal, 'S_ADDRESS': s_address, 'S_COMMENT': s_comment, 'S_NAME': s_name, 'S_PHONE': s_phone})
    FILTER(condition=supplycost_21 == best_cost & ENDSWITH(part_type, 'BRASS':string) & size == 15:int64, columns={'key_19': key_19, 'manufacturer': manufacturer, 'n_name': n_name, 's_acctbal': s_acctbal, 's_address': s_address, 's_comment': s_comment, 's_name': s_name, 's_phone': s_phone})
     JOIN(conditions=[t0.key_9 == t1.key_19], types=['inner'], columns={'best_cost': t0.best_cost, 'key_19': t1.key_19, 'manufacturer': t1.manufacturer, 'n_name': t1.n_name, 'part_type': t1.part_type, 's_acctbal': t1.s_acctbal, 's_address': t1.s_address, 's_comment': t1.s_comment, 's_name': t1.s_name, 's_phone': t1.s_phone, 'size': t1.size, 'supplycost_21': t1.supplycost})
      PROJECT(columns={'best_cost': agg_0, 'key_9': key_9})
       AGGREGATE(keys={'key_9': key_9}, aggregations={'agg_0': MIN(supplycost)})
        FILTER(condition=ENDSWITH(part_type, 'BRASS':string) & size == 15:int64, columns={'key_9': key_9, 'supplycost': supplycost})
         JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'key_9': t1.key, 'part_type': t1.part_type, 'size': t1.size, 'supplycost': t0.supplycost})
          JOIN(conditions=[t0.key_5 == t1.supplier_key], types=['inner'], columns={'part_key': t1.part_key, 'supplycost': t1.supplycost})
           JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'key_5': t1.key})
            FILTER(condition=name_3 == 'EUROPE':string, columns={'key': key})
             JOIN(conditions=[t0.region_key == t1.key], types=['left'], columns={'key': t0.key, 'name_3': t1.name})
              SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
              SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
            SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
           SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey, 'supplycost': ps_supplycost})
          SCAN(table=tpch.PART, columns={'key': p_partkey, 'part_type': p_type, 'size': p_size})
      FILTER(condition=ENDSWITH(part_type, 'BRASS':string) & size == 15:int64, columns={'key_19': key_19, 'manufacturer': manufacturer, 'n_name': n_name, 'part_type': part_type, 's_acctbal': s_acctbal, 's_address': s_address, 's_comment': s_comment, 's_name': s_name, 's_phone': s_phone, 'size': size, 'supplycost': supplycost})
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
         SCAN(table=tpch.PART, columns={'key': p_partkey, 'manufacturer': p_mfgr, 'part_type': p_type, 'size': p_size})""",
                tpch_q2_output,
            ),
            id="tpch_q2",
        ),
        pytest.param(
            (
                impl_tpch_q3,
                """
ROOT(columns=[('L_ORDERKEY', L_ORDERKEY), ('REVENUE', REVENUE), ('O_ORDERDATE', O_ORDERDATE), ('O_SHIPPRIORITY', O_SHIPPRIORITY)], orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'L_ORDERKEY': L_ORDERKEY, 'O_ORDERDATE': O_ORDERDATE, 'O_SHIPPRIORITY': O_SHIPPRIORITY, 'REVENUE': REVENUE, 'ordering_1': ordering_1, 'ordering_2': ordering_2, 'ordering_3': ordering_3}, orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first])
  PROJECT(columns={'L_ORDERKEY': L_ORDERKEY, 'O_ORDERDATE': O_ORDERDATE, 'O_SHIPPRIORITY': O_SHIPPRIORITY, 'REVENUE': REVENUE, 'ordering_1': REVENUE, 'ordering_2': O_ORDERDATE, 'ordering_3': L_ORDERKEY})
   PROJECT(columns={'L_ORDERKEY': order_key, 'O_ORDERDATE': order_date, 'O_SHIPPRIORITY': ship_priority, 'REVENUE': DEFAULT_TO(agg_0, 0:int64)})
    AGGREGATE(keys={'order_date': order_date, 'order_key': order_key, 'ship_priority': ship_priority}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
     FILTER(condition=ship_date > datetime.date(1995, 3, 15):date, columns={'discount': discount, 'extended_price': extended_price, 'order_date': order_date, 'order_key': order_key, 'ship_priority': ship_priority})
      JOIN(conditions=[t0.key == t1.order_key], types=['inner'], columns={'discount': t1.discount, 'extended_price': t1.extended_price, 'order_date': t0.order_date, 'order_key': t1.order_key, 'ship_date': t1.ship_date, 'ship_priority': t0.ship_priority})
       FILTER(condition=mktsegment == 'BUILDING':string & order_date < datetime.date(1995, 3, 15):date, columns={'key': key, 'order_date': order_date, 'ship_priority': ship_priority})
        JOIN(conditions=[t0.customer_key == t1.key], types=['left'], columns={'key': t0.key, 'mktsegment': t1.mktsegment, 'order_date': t0.order_date, 'ship_priority': t0.ship_priority})
         SCAN(table=tpch.ORDERS, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate, 'ship_priority': o_shippriority})
         SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'mktsegment': c_mktsegment})
       SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'ship_date': l_shipdate})
""",
                tpch_q3_output,
            ),
            id="tpch_q3",
        ),
        pytest.param(
            (
                impl_tpch_q4,
                """
ROOT(columns=[('O_ORDERPRIORITY', O_ORDERPRIORITY), ('ORDER_COUNT', ORDER_COUNT)], orderings=[(ordering_1):asc_first])
 PROJECT(columns={'ORDER_COUNT': ORDER_COUNT, 'O_ORDERPRIORITY': O_ORDERPRIORITY, 'ordering_1': O_ORDERPRIORITY})
  PROJECT(columns={'ORDER_COUNT': DEFAULT_TO(agg_0, 0:int64), 'O_ORDERPRIORITY': order_priority})
   AGGREGATE(keys={'order_priority': order_priority}, aggregations={'agg_0': COUNT()})
    FILTER(condition=order_date >= datetime.date(1993, 7, 1):date & order_date < datetime.date(1993, 10, 1):date & True:bool, columns={'order_priority': order_priority})
     JOIN(conditions=[t0.key == t1.order_key], types=['semi'], columns={'order_date': t0.order_date, 'order_priority': t0.order_priority})
      SCAN(table=tpch.ORDERS, columns={'key': o_orderkey, 'order_date': o_orderdate, 'order_priority': o_orderpriority})
      FILTER(condition=commit_date < receipt_date, columns={'order_key': order_key})
       SCAN(table=tpch.LINEITEM, columns={'commit_date': l_commitdate, 'order_key': l_orderkey, 'receipt_date': l_receiptdate})
""",
                tpch_q4_output,
            ),
            id="tpch_q4",
        ),
        pytest.param(
            (
                impl_tpch_q5,
                """
""",
                tpch_q5_output,
            ),
            id="tpch_q5",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                impl_tpch_q6,
                """
ROOT(columns=[('REVENUE', REVENUE)], orderings=[])
 PROJECT(columns={'REVENUE': DEFAULT_TO(agg_0, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(amt)})
   PROJECT(columns={'amt': extended_price * discount})
    FILTER(condition=ship_date >= datetime.date(1994, 1, 1):date & ship_date < datetime.date(1995, 1, 1):date & discount >= 0.05:float64 & discount <= 0.07:float64 & quantity < 24:int64, columns={'discount': discount, 'extended_price': extended_price})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'quantity': l_quantity, 'ship_date': l_shipdate})
""",
                tpch_q6_output,
            ),
            id="tpch_q6",
        ),
        pytest.param(
            (
                impl_tpch_q7,
                """
ROOT(columns=[('SUPP_NATION', SUPP_NATION), ('CUST_NATION', CUST_NATION), ('L_YEAR', L_YEAR), ('REVENUE', REVENUE)], orderings=[(ordering_1):asc_first, (ordering_2):asc_first, (ordering_3):asc_first])
 PROJECT(columns={'CUST_NATION': CUST_NATION, 'L_YEAR': L_YEAR, 'REVENUE': REVENUE, 'SUPP_NATION': SUPP_NATION, 'ordering_1': SUPP_NATION, 'ordering_2': CUST_NATION, 'ordering_3': L_YEAR})
  PROJECT(columns={'CUST_NATION': cust_nation, 'L_YEAR': l_year, 'REVENUE': DEFAULT_TO(agg_0, 0:int64), 'SUPP_NATION': supp_nation})
   AGGREGATE(keys={'cust_nation': cust_nation, 'l_year': l_year, 'supp_nation': supp_nation}, aggregations={'agg_0': SUM(volume)})
    FILTER(condition=ship_date >= datetime.date(1995, 1, 1):date & ship_date <= datetime.date(1996, 12, 31):date & supp_nation == 'FRANCE':string & cust_nation == 'GERMANY':string | supp_nation == 'GERMANY':string & cust_nation == 'FRANCE':string, columns={'cust_nation': cust_nation, 'l_year': l_year, 'supp_nation': supp_nation, 'volume': volume})
     PROJECT(columns={'cust_nation': name_8, 'l_year': YEAR(ship_date), 'ship_date': ship_date, 'supp_nation': name_3, 'volume': extended_price * 1:int64 - discount})
      JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_3': t0.name_3, 'name_8': t1.name_8, 'ship_date': t0.ship_date})
       JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_3': t1.name_3, 'order_key': t0.order_key, 'ship_date': t0.ship_date})
        SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
        JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_3': t1.name})
         SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
         SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
       JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_8': t1.name})
        JOIN(conditions=[t0.customer_key == t1.key], types=['inner'], columns={'key': t0.key, 'nation_key': t1.nation_key})
         SCAN(table=tpch.ORDERS, columns={'customer_key': o_custkey, 'key': o_orderkey})
         SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
        SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
                tpch_q7_output,
            ),
            id="tpch_q7",
        ),
        pytest.param(
            (
                impl_tpch_q8,
                """
ROOT(columns=[('O_YEAR', O_YEAR), ('MKT_SHARE', MKT_SHARE)], orderings=[])
 PROJECT(columns={'MKT_SHARE': DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64), 'O_YEAR': o_year})
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
       SCAN(table=tpch.ORDERS, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate})
     JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_18': t1.name})
      JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'region_key': t1.region_key})
       SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
       SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
      SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
                tpch_q8_output,
            ),
            id="tpch_q8",
        ),
        pytest.param(
            (
                impl_tpch_q9,
                """
ROOT(columns=[('NATION', NATION), ('O_YEAR', O_YEAR), ('AMOUNT', AMOUNT)], orderings=[(ordering_1):asc_first, (ordering_2):desc_last])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'AMOUNT': AMOUNT, 'NATION': NATION, 'O_YEAR': O_YEAR, 'ordering_1': ordering_1, 'ordering_2': ordering_2}, orderings=[(ordering_1):asc_first, (ordering_2):desc_last])
  PROJECT(columns={'AMOUNT': AMOUNT, 'NATION': NATION, 'O_YEAR': O_YEAR, 'ordering_1': NATION, 'ordering_2': O_YEAR})
   PROJECT(columns={'AMOUNT': DEFAULT_TO(agg_0, 0:int64), 'NATION': nation, 'O_YEAR': o_year})
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
       SCAN(table=tpch.ORDERS, columns={'key': o_orderkey, 'order_date': o_orderdate})
""",
                tpch_q9_output,
            ),
            id="tpch_q9",
        ),
        pytest.param(
            (
                impl_tpch_q10,
                """
ROOT(columns=[('C_CUSTKEY', C_CUSTKEY), ('C_NAME', C_NAME), ('REVENUE', REVENUE), ('C_ACCTBAL', C_ACCTBAL), ('N_NAME', N_NAME), ('C_ADDRESS', C_ADDRESS), ('C_PHONE', C_PHONE), ('C_COMMENT', C_COMMENT)], orderings=[(ordering_1):desc_last, (ordering_2):asc_first])
 LIMIT(limit=Literal(value=20, type=Int64Type()), columns={'C_ACCTBAL': C_ACCTBAL, 'C_ADDRESS': C_ADDRESS, 'C_COMMENT': C_COMMENT, 'C_CUSTKEY': C_CUSTKEY, 'C_NAME': C_NAME, 'C_PHONE': C_PHONE, 'N_NAME': N_NAME, 'REVENUE': REVENUE, 'ordering_1': ordering_1, 'ordering_2': ordering_2}, orderings=[(ordering_1):desc_last, (ordering_2):asc_first])
  PROJECT(columns={'C_ACCTBAL': C_ACCTBAL, 'C_ADDRESS': C_ADDRESS, 'C_COMMENT': C_COMMENT, 'C_CUSTKEY': C_CUSTKEY, 'C_NAME': C_NAME, 'C_PHONE': C_PHONE, 'N_NAME': N_NAME, 'REVENUE': REVENUE, 'ordering_1': REVENUE, 'ordering_2': C_CUSTKEY})
   PROJECT(columns={'C_ACCTBAL': acctbal, 'C_ADDRESS': address, 'C_COMMENT': comment, 'C_CUSTKEY': key, 'C_NAME': name, 'C_PHONE': phone, 'N_NAME': name_4, 'REVENUE': DEFAULT_TO(agg_0, 0:int64)})
    JOIN(conditions=[t0.nation_key == t1.key], types=['left'], columns={'acctbal': t0.acctbal, 'address': t0.address, 'agg_0': t0.agg_0, 'comment': t0.comment, 'key': t0.key, 'name': t0.name, 'name_4': t1.name, 'phone': t0.phone})
     JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'acctbal': t0.acctbal, 'address': t0.address, 'agg_0': t1.agg_0, 'comment': t0.comment, 'key': t0.key, 'name': t0.name, 'nation_key': t0.nation_key, 'phone': t0.phone})
      SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'address': c_address, 'comment': c_comment, 'key': c_custkey, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
      AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_0': SUM(amt)})
       PROJECT(columns={'amt': extended_price * 1:int64 - discount, 'customer_key': customer_key})
        FILTER(condition=return_flag == 'R':string, columns={'customer_key': customer_key, 'discount': discount, 'extended_price': extended_price})
         JOIN(conditions=[t0.key == t1.order_key], types=['inner'], columns={'customer_key': t0.customer_key, 'discount': t1.discount, 'extended_price': t1.extended_price, 'return_flag': t1.return_flag})
          FILTER(condition=order_date >= datetime.date(1993, 10, 1):date & order_date < datetime.date(1994, 1, 1):date, columns={'customer_key': customer_key, 'key': key})
           SCAN(table=tpch.ORDERS, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate})
          SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'return_flag': l_returnflag})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
                tpch_q10_output,
            ),
            id="tpch_q10",
        ),
        pytest.param(
            (
                impl_tpch_q11,
                """
ROOT(columns=[('PS_PARTKEY', PS_PARTKEY), ('VALUE', VALUE)], orderings=[(ordering_2):desc_last])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'PS_PARTKEY': PS_PARTKEY, 'VALUE': VALUE, 'ordering_2': ordering_2}, orderings=[(ordering_2):desc_last])
  PROJECT(columns={'PS_PARTKEY': PS_PARTKEY, 'VALUE': VALUE, 'ordering_2': VALUE})
   FILTER(condition=VALUE > min_market_share, columns={'PS_PARTKEY': PS_PARTKEY, 'VALUE': VALUE})
    PROJECT(columns={'PS_PARTKEY': part_key, 'VALUE': DEFAULT_TO(agg_1, 0:int64), 'min_market_share': min_market_share})
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
                tpch_q11_output,
            ),
            id="tpch_q11",
        ),
        pytest.param(
            (
                impl_tpch_q12,
                """
ROOT(columns=[('L_SHIPMODE', L_SHIPMODE), ('HIGH_LINE_COUNT', HIGH_LINE_COUNT), ('LOW_LINE_COUNT', LOW_LINE_COUNT)], orderings=[(ordering_2):asc_first])
 PROJECT(columns={'HIGH_LINE_COUNT': HIGH_LINE_COUNT, 'LOW_LINE_COUNT': LOW_LINE_COUNT, 'L_SHIPMODE': L_SHIPMODE, 'ordering_2': L_SHIPMODE})
  PROJECT(columns={'HIGH_LINE_COUNT': DEFAULT_TO(agg_0, 0:int64), 'LOW_LINE_COUNT': DEFAULT_TO(agg_1, 0:int64), 'L_SHIPMODE': ship_mode})
   AGGREGATE(keys={'ship_mode': ship_mode}, aggregations={'agg_0': SUM(is_high_priority), 'agg_1': SUM(NOT(is_high_priority))})
    PROJECT(columns={'is_high_priority': order_priority == '1-URGENT':string | order_priority == '2-HIGH':string, 'ship_mode': ship_mode})
     JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'order_priority': t1.order_priority, 'ship_mode': t0.ship_mode})
      FILTER(condition=ship_mode == 'MAIL':string | ship_mode == 'SHIP':string & ship_date < commit_date & commit_date < receipt_date & receipt_date >= datetime.date(1994, 1, 1):date & receipt_date < datetime.date(1995, 1, 1):date, columns={'order_key': order_key, 'ship_mode': ship_mode})
       SCAN(table=tpch.LINEITEM, columns={'commit_date': l_commitdate, 'order_key': l_orderkey, 'receipt_date': l_receiptdate, 'ship_date': l_shipdate, 'ship_mode': l_shipmode})
      SCAN(table=tpch.ORDERS, columns={'key': o_orderkey, 'order_priority': o_orderpriority})
""",
                tpch_q12_output,
            ),
            id="tpch_q12",
        ),
        pytest.param(
            (
                impl_tpch_q13,
                """
ROOT(columns=[('C_COUNT', C_COUNT), ('CUSTDIST', CUSTDIST)], orderings=[(ordering_3):desc_last, (ordering_4):desc_last])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'CUSTDIST': CUSTDIST, 'C_COUNT': C_COUNT, 'ordering_3': ordering_3, 'ordering_4': ordering_4}, orderings=[(ordering_3):desc_last, (ordering_4):desc_last])
  PROJECT(columns={'CUSTDIST': CUSTDIST, 'C_COUNT': C_COUNT, 'ordering_3': CUSTDIST, 'ordering_4': C_COUNT})
   PROJECT(columns={'CUSTDIST': DEFAULT_TO(agg_2, 0:int64), 'C_COUNT': num_non_special_orders})
    JOIN(conditions=[t0.num_non_special_orders == t1.num_non_special_orders], types=['left'], columns={'agg_2': t1.agg_2, 'num_non_special_orders': t0.num_non_special_orders})
     AGGREGATE(keys={'num_non_special_orders': num_non_special_orders}, aggregations={})
      PROJECT(columns={'num_non_special_orders': DEFAULT_TO(agg_0, 0:int64)})
       JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'agg_0': t1.agg_0})
        SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey})
        AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_0': COUNT()})
         FILTER(condition=NOT(LIKE(comment, '%special%requests%':string)), columns={'customer_key': customer_key})
          SCAN(table=tpch.ORDERS, columns={'comment': o_comment, 'customer_key': o_custkey})
     AGGREGATE(keys={'num_non_special_orders': num_non_special_orders}, aggregations={'agg_2': COUNT()})
      PROJECT(columns={'num_non_special_orders': DEFAULT_TO(agg_1, 0:int64)})
       JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'agg_1': t1.agg_1})
        SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey})
        AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_1': COUNT()})
         FILTER(condition=NOT(LIKE(comment, '%special%requests%':string)), columns={'customer_key': customer_key})
          SCAN(table=tpch.ORDERS, columns={'comment': o_comment, 'customer_key': o_custkey})
""",
                tpch_q13_output,
            ),
            id="tpch_q13",
        ),
        pytest.param(
            (
                impl_tpch_q14,
                """
ROOT(columns=[('PROMO_REVENUE', PROMO_REVENUE)], orderings=[])
 PROJECT(columns={'PROMO_REVENUE': 100.0:float64 * DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(promo_value), 'agg_1': SUM(value)})
   PROJECT(columns={'promo_value': IFF(STARTSWITH(part_type, 'PROMO':string), extended_price * 1:int64 - discount, 0:int64), 'value': extended_price * 1:int64 - discount})
    JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'part_type': t1.part_type})
     FILTER(condition=ship_date >= datetime.date(1995, 9, 1):date & ship_date < datetime.date(1995, 10, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'part_key': part_key})
      SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'ship_date': l_shipdate})
     SCAN(table=tpch.PART, columns={'key': p_partkey, 'part_type': p_type})
""",
                tpch_q14_output,
            ),
            id="tpch_q14",
        ),
        pytest.param(
            (
                impl_tpch_q15,
                """
ROOT(columns=[('S_SUPPKEY', S_SUPPKEY), ('S_NAME', S_NAME), ('S_ADDRESS', S_ADDRESS), ('S_PHONE', S_PHONE), ('TOTAL_REVENUE', TOTAL_REVENUE)], orderings=[(ordering_3):asc_first])
 PROJECT(columns={'S_ADDRESS': S_ADDRESS, 'S_NAME': S_NAME, 'S_PHONE': S_PHONE, 'S_SUPPKEY': S_SUPPKEY, 'TOTAL_REVENUE': TOTAL_REVENUE, 'ordering_3': S_SUPPKEY})
  FILTER(condition=TOTAL_REVENUE == max_revenue, columns={'S_ADDRESS': S_ADDRESS, 'S_NAME': S_NAME, 'S_PHONE': S_PHONE, 'S_SUPPKEY': S_SUPPKEY, 'TOTAL_REVENUE': TOTAL_REVENUE})
   PROJECT(columns={'S_ADDRESS': address, 'S_NAME': name, 'S_PHONE': phone, 'S_SUPPKEY': key, 'TOTAL_REVENUE': DEFAULT_TO(agg_2, 0:int64), 'max_revenue': max_revenue})
    JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'address': t0.address, 'agg_2': t1.agg_2, 'key': t0.key, 'max_revenue': t0.max_revenue, 'name': t0.name, 'phone': t0.phone})
     JOIN(conditions=[True:bool], types=['inner'], columns={'address': t1.address, 'key': t1.key, 'max_revenue': t0.max_revenue, 'name': t1.name, 'phone': t1.phone})
      PROJECT(columns={'max_revenue': agg_1})
       AGGREGATE(keys={}, aggregations={'agg_1': MAX(total_revenue)})
        PROJECT(columns={'total_revenue': DEFAULT_TO(agg_0, 0:int64)})
         JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'agg_0': t1.agg_0})
          SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey})
          AGGREGATE(keys={'supplier_key': supplier_key}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
           FILTER(condition=ship_date >= datetime.date(1996, 1, 1):date & ship_date < datetime.date(1996, 4, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'supplier_key': supplier_key})
            SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
      SCAN(table=tpch.SUPPLIER, columns={'address': s_address, 'key': s_suppkey, 'name': s_name, 'phone': s_phone})
     AGGREGATE(keys={'supplier_key': supplier_key}, aggregations={'agg_2': SUM(extended_price * 1:int64 - discount)})
      FILTER(condition=ship_date >= datetime.date(1996, 1, 1):date & ship_date < datetime.date(1996, 4, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'supplier_key': supplier_key})
       SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
""",
                tpch_q15_output,
            ),
            id="tpch_q15",
        ),
        pytest.param(
            (
                impl_tpch_q16,
                """
ROOT(columns=[('P_BRAND', P_BRAND), ('P_TYPE', P_TYPE), ('P_SIZE', P_SIZE), ('SUPPLIER_COUNT', SUPPLIER_COUNT)], orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first, (ordering_4):asc_first])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'P_BRAND': P_BRAND, 'P_SIZE': P_SIZE, 'P_TYPE': P_TYPE, 'SUPPLIER_COUNT': SUPPLIER_COUNT, 'ordering_1': ordering_1, 'ordering_2': ordering_2, 'ordering_3': ordering_3, 'ordering_4': ordering_4}, orderings=[(ordering_1):desc_last, (ordering_2):asc_first, (ordering_3):asc_first, (ordering_4):asc_first])
  PROJECT(columns={'P_BRAND': P_BRAND, 'P_SIZE': P_SIZE, 'P_TYPE': P_TYPE, 'SUPPLIER_COUNT': SUPPLIER_COUNT, 'ordering_1': SUPPLIER_COUNT, 'ordering_2': P_BRAND, 'ordering_3': P_TYPE, 'ordering_4': P_SIZE})
   PROJECT(columns={'P_BRAND': p_brand, 'P_SIZE': p_size, 'P_TYPE': p_type, 'SUPPLIER_COUNT': agg_0})
    AGGREGATE(keys={'p_brand': p_brand, 'p_size': p_size, 'p_type': p_type}, aggregations={'agg_0': NDISTINCT(supplier_key)})
     FILTER(condition=NOT(LIKE(comment_2, '%Customer%Complaints%':string)), columns={'p_brand': p_brand, 'p_size': p_size, 'p_type': p_type, 'supplier_key': supplier_key})
      JOIN(conditions=[t0.supplier_key == t1.key], types=['left'], columns={'comment_2': t1.comment, 'p_brand': t0.p_brand, 'p_size': t0.p_size, 'p_type': t0.p_type, 'supplier_key': t0.supplier_key})
       PROJECT(columns={'p_brand': brand, 'p_size': size, 'p_type': part_type, 'supplier_key': supplier_key})
        JOIN(conditions=[t0.key == t1.part_key], types=['inner'], columns={'brand': t0.brand, 'part_type': t0.part_type, 'size': t0.size, 'supplier_key': t1.supplier_key})
         FILTER(condition=brand != 'BRAND#45':string & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%':string)) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9]:array[unknown]), columns={'brand': brand, 'key': key, 'part_type': part_type, 'size': size})
          SCAN(table=tpch.PART, columns={'brand': p_brand, 'key': p_partkey, 'part_type': p_type, 'size': p_size})
         SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
       SCAN(table=tpch.SUPPLIER, columns={'comment': s_comment, 'key': s_suppkey})
""",
                tpch_q16_output,
            ),
            id="tpch_q16",
        ),
        pytest.param(
            (
                impl_tpch_q17,
                """
ROOT(columns=[('AVG_YEARLY', AVG_YEARLY)], orderings=[])
 PROJECT(columns={'AVG_YEARLY': DEFAULT_TO(agg_1, 0:int64) / 7.0:float64})
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
                tpch_q17_output,
            ),
            id="tpch_q17",
        ),
        pytest.param(
            (
                impl_tpch_q18,
                """
ROOT(columns=[('C_NAME', C_NAME), ('C_CUSTKEY', C_CUSTKEY), ('O_ORDERKEY', O_ORDERKEY), ('O_ORDERDATE', O_ORDERDATE), ('O_TOTALPRICE', O_TOTALPRICE), ('TOTAL_QUANTITY', TOTAL_QUANTITY)], orderings=[(ordering_1):desc_last, (ordering_2):asc_first])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'C_CUSTKEY': C_CUSTKEY, 'C_NAME': C_NAME, 'O_ORDERDATE': O_ORDERDATE, 'O_ORDERKEY': O_ORDERKEY, 'O_TOTALPRICE': O_TOTALPRICE, 'TOTAL_QUANTITY': TOTAL_QUANTITY, 'ordering_1': ordering_1, 'ordering_2': ordering_2}, orderings=[(ordering_1):desc_last, (ordering_2):asc_first])
  PROJECT(columns={'C_CUSTKEY': C_CUSTKEY, 'C_NAME': C_NAME, 'O_ORDERDATE': O_ORDERDATE, 'O_ORDERKEY': O_ORDERKEY, 'O_TOTALPRICE': O_TOTALPRICE, 'TOTAL_QUANTITY': TOTAL_QUANTITY, 'ordering_1': O_TOTALPRICE, 'ordering_2': O_ORDERDATE})
   FILTER(condition=TOTAL_QUANTITY > 300:int64, columns={'C_CUSTKEY': C_CUSTKEY, 'C_NAME': C_NAME, 'O_ORDERDATE': O_ORDERDATE, 'O_ORDERKEY': O_ORDERKEY, 'O_TOTALPRICE': O_TOTALPRICE, 'TOTAL_QUANTITY': TOTAL_QUANTITY})
    PROJECT(columns={'C_CUSTKEY': key_2, 'C_NAME': name, 'O_ORDERDATE': order_date, 'O_ORDERKEY': key, 'O_TOTALPRICE': total_price, 'TOTAL_QUANTITY': DEFAULT_TO(agg_0, 0:int64)})
     JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key, 'key_2': t0.key_2, 'name': t0.name, 'order_date': t0.order_date, 'total_price': t0.total_price})
      JOIN(conditions=[t0.customer_key == t1.key], types=['left'], columns={'key': t0.key, 'key_2': t1.key, 'name': t1.name, 'order_date': t0.order_date, 'total_price': t0.total_price})
       SCAN(table=tpch.ORDERS, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate, 'total_price': o_totalprice})
       SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'name': c_name})
      AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': SUM(quantity)})
       SCAN(table=tpch.LINEITEM, columns={'order_key': l_orderkey, 'quantity': l_quantity})
""",
                tpch_q18_output,
            ),
            id="tpch_q18",
        ),
        pytest.param(
            (
                impl_tpch_q19,
                """
ROOT(columns=[('REVENUE', REVENUE)], orderings=[])
 PROJECT(columns={'REVENUE': DEFAULT_TO(agg_0, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(extended_price * 1:int64 - discount)})
   FILTER(condition=ISIN(ship_mode, ['AIR', 'AIR REG']:array[unknown]) & ship_instruct == 'DELIVER IN PERSON':string & size >= 1:int64 & size <= 5:int64 & quantity >= 1:int64 & quantity <= 11:int64 & ISIN(container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']:array[unknown]) & brand == 'Brand#12':string | size <= 10:int64 & quantity >= 10:int64 & quantity <= 20:int64 & ISIN(container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG']:array[unknown]) & brand == 'Brand#23':string | size <= 15:int64 & quantity >= 20:int64 & quantity <= 30:int64 & ISIN(container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']:array[unknown]) & brand == 'Brand#34':string, columns={'discount': discount, 'extended_price': extended_price})
    JOIN(conditions=[t0.part_key == t1.key], types=['left'], columns={'brand': t1.brand, 'container': t1.container, 'discount': t0.discount, 'extended_price': t0.extended_price, 'quantity': t0.quantity, 'ship_instruct': t0.ship_instruct, 'ship_mode': t0.ship_mode, 'size': t1.size})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'quantity': l_quantity, 'ship_instruct': l_shipinstruct, 'ship_mode': l_shipmode})
     SCAN(table=tpch.PART, columns={'brand': p_brand, 'container': p_container, 'key': p_partkey, 'size': p_size})
""",
                tpch_q19_output,
            ),
            id="tpch_q19",
        ),
        pytest.param(
            (
                impl_tpch_q20,
                """
ROOT(columns=[('S_NAME', S_NAME), ('S_ADDRESS', S_ADDRESS)], orderings=[(ordering_2):asc_first])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'S_ADDRESS': S_ADDRESS, 'S_NAME': S_NAME, 'ordering_2': ordering_2}, orderings=[(ordering_2):asc_first])
  PROJECT(columns={'S_ADDRESS': S_ADDRESS, 'S_NAME': S_NAME, 'ordering_2': S_NAME})
   FILTER(condition=name_3 == 'CANADA':string & DEFAULT_TO(agg_1, 0:int64) > 0:int64, columns={'S_ADDRESS': S_ADDRESS, 'S_NAME': S_NAME})
    JOIN(conditions=[t0.key == t1.supplier_key], types=['left'], columns={'S_ADDRESS': t0.S_ADDRESS, 'S_NAME': t0.S_NAME, 'agg_1': t1.agg_1, 'name_3': t0.name_3})
     JOIN(conditions=[t0.nation_key == t1.key], types=['left'], columns={'S_ADDRESS': t0.S_ADDRESS, 'S_NAME': t0.S_NAME, 'key': t0.key, 'name_3': t1.name})
      PROJECT(columns={'S_ADDRESS': address, 'S_NAME': name, 'key': key, 'nation_key': nation_key})
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
                tpch_q20_output,
            ),
            id="tpch_q20",
        ),
        pytest.param(
            (
                impl_tpch_q21,
                """
""",
                tpch_q21_output,
            ),
            id="tpch_q21",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                impl_tpch_q22,
                """
""",
                tpch_q22_output,
            ),
            id="tpch_q22",
            marks=pytest.mark.skip("TODO: support correlated back references"),
        ),
        pytest.param(
            (
                simple_scan_top_five,
                """
ROOT(columns=[('key', key)], orderings=[(ordering_0):asc_first])
 LIMIT(limit=Literal(value=5, type=Int64Type()), columns={'key': key, 'ordering_0': ordering_0}, orderings=[(ordering_0):asc_first])
  PROJECT(columns={'key': key, 'ordering_0': key})
   SCAN(table=tpch.ORDERS, columns={'key': o_orderkey})
""",
                lambda: pd.DataFrame(
                    {
                        "key": [1, 2, 3, 4, 5],
                    }
                ),
            ),
            id="simple_scan_top_five",
        ),
        pytest.param(
            (
                simple_filter_top_five,
                """
ROOT(columns=[('key', key), ('total_price', total_price)], orderings=[(ordering_0):desc_last])
 LIMIT(limit=Literal(value=5, type=Int64Type()), columns={'key': key, 'ordering_0': ordering_0, 'total_price': total_price}, orderings=[(ordering_0):desc_last])
  PROJECT(columns={'key': key, 'ordering_0': key, 'total_price': total_price})
   FILTER(condition=total_price < 1000.0:float64, columns={'key': key, 'total_price': total_price})
    SCAN(table=tpch.ORDERS, columns={'key': o_orderkey, 'total_price': o_totalprice})
""",
                lambda: pd.DataFrame(
                    {
                        "key": [5989315, 5935174, 5881093, 5876066, 5866437],
                        "total_price": [947.81, 974.01, 995.6, 967.55, 916.41],
                    }
                ),
            ),
            id="simple_filter_top_five",
        ),
    ],
)
def pydough_pipeline_test_data(
    request,
) -> tuple[
    Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
]:
    """
    Test data for test_pydough_pipeline. Returns a tuple of the following
    arguments:
    1. `unqualified_impl`: a function that takes in an unqualified root and
    creates the unqualified node for the TPCH query.
    2. `relational_str`: the string representation of the relational plan
    produced for the TPCH query.
    3. `answer_impl`: a function that takes in nothing and returns the answer
    to a TPCH query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational(
    pydough_pipeline_test_data: tuple[
        Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    sqlite_tpch_db_context: DatabaseContext,
) -> None:
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified AST version, with the correct string representation.
    """
    # Run the query through the stages from unqualified node to qualified node
    # to relational tree, and confirm the tree string matches the expected
    # structure.
    unqualified_impl, relational_string, _ = pydough_pipeline_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughAST = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionAST
    ), "Expected qualified answer to be a collection, not an expression"
    relational: RelationalRoot = convert_ast_to_relational(qualified, default_config)
    assert (
        relational.to_tree_string() == relational_string.strip()
    ), "Mismatch between tree string representation of relational node and expected Relational tree string"


def test_pipeline_e2e(
    pydough_pipeline_test_data: tuple[
        Callable[[UnqualifiedRoot], UnqualifiedNode], str, Callable[[], pd.DataFrame]
    ],
    get_sample_graph: graph_fetcher,
    default_config: PyDoughConfigs,
    sqlite_tpch_db_context: DatabaseContext,
):
    """
    Test executing the TPC-H queries from the original code generation.
    """
    unqualified_impl, _, answer_impl = pydough_pipeline_test_data
    graph: GraphMetadata = get_sample_graph("TPCH")
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(root, metadata=graph, database=sqlite_tpch_db_context)
    pd.testing.assert_frame_equal(result, answer_impl())
