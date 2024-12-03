"""
TODO: add file-level docstring.
"""

import pytest
from test_utils import (
    BackReferenceExpressionInfo,
    CalcInfo,
    ChildReferenceCollectionInfo,
    ChildReferenceExpressionInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    TopKInfo,
    WhereInfo,
)

from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from pydough.types import (
    Float64Type,
    Int64Type,
    StringType,
)


@pytest.mark.parametrize(
    "calc_pipeline, expected_relational_string",
    [
        pytest.param(
            TableCollectionInfo("Regions"),
            """
ROOT(columns=[('key', key), ('name', name), ('comment', comment)], orderings=[])
 SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
""",
            id="scan_regions",
        ),
        pytest.param(
            TableCollectionInfo("Nations"),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="scan_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [],
                region_name=ReferenceInfo("name"),
                magic_word=LiteralInfo("foo", StringType()),
            ),
            """
ROOT(columns=[('region_name', region_name), ('magic_word', magic_word)], orderings=[])
 PROJECT(columns={'magic_word': 'foo':string, 'region_name': name})
  SCAN(table=tpch.REGION, columns={'name': r_name})
""",
            id="scan_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo([], name=LiteralInfo("foo", StringType()))
            ** CalcInfo([], fizz=ReferenceInfo("name"), buzz=ReferenceInfo("key")),
            """
ROOT(columns=[('fizz', fizz), ('buzz', buzz)], orderings=[])
 PROJECT(columns={'buzz': key, 'fizz': name_0})
  PROJECT(columns={'key': key, 'name_0': 'foo':string})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey})
""",
            id="scan_calc_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 PROJECT(columns={'comment': comment_1, 'key': key_2, 'name': name_3, 'region_key': region_key})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment_1': t1.comment, 'key_2': t1.key, 'name_3': t1.name, 'region_key': t1.region_key})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey})
   SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="join_region_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** SubCollectionInfo("customers"),
            """
ROOT(columns=[('key', key), ('name', name), ('address', address), ('nation_key', nation_key), ('phone', phone), ('acctbal', acctbal), ('mktsegment', mktsegment), ('comment', comment)], orderings=[])
 PROJECT(columns={'acctbal': acctbal, 'address': address, 'comment': comment_4, 'key': key_5, 'mktsegment': mktsegment, 'name': name_6, 'nation_key': nation_key, 'phone': phone})
  JOIN(conditions=[t0.key_2 == t1.nation_key], types=['inner'], columns={'acctbal': t1.acctbal, 'address': t1.address, 'comment_4': t1.comment, 'key_5': t1.key, 'mktsegment': t1.mktsegment, 'name_6': t1.name, 'nation_key': t1.nation_key, 'phone': t1.phone})
   JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_2': t1.key})
    SCAN(table=tpch.REGION, columns={'key': r_regionkey})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
   SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'address': c_address, 'comment': c_comment, 'key': c_custkey, 'mktsegment': c_mktsegment, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
""",
            id="join_region_nations_customers",
        ),
        pytest.param(
            TableCollectionInfo("Customers")
            ** CalcInfo(
                [],
                name=FunctionInfo("LOWER", [ReferenceInfo("name")]),
                country_code=FunctionInfo(
                    "SLICE",
                    [
                        ReferenceInfo("phone"),
                        LiteralInfo(0, Int64Type()),
                        LiteralInfo(3, Int64Type()),
                        LiteralInfo(1, Int64Type()),
                    ],
                ),
                adjusted_account_balance=FunctionInfo(
                    "IFF",
                    [
                        FunctionInfo(
                            "LET",
                            [
                                ReferenceInfo("acctbal"),
                                LiteralInfo(0, Int64Type()),
                            ],
                        ),
                        LiteralInfo(0, Int64Type()),
                        ReferenceInfo("acctbal"),
                    ],
                ),
                is_named_john=FunctionInfo(
                    "LET",
                    [
                        FunctionInfo("LOWER", [ReferenceInfo("name")]),
                        LiteralInfo("john", StringType()),
                    ],
                ),
            ),
            """
ROOT(columns=[('name', name_0), ('country_code', country_code), ('adjusted_account_balance', adjusted_account_balance), ('is_named_john', is_named_john)], orderings=[])
 PROJECT(columns={'adjusted_account_balance': IFF(acctbal < 0:int64, 0:int64, acctbal), 'country_code': SLICE(phone, 0:int64, 3:int64, 1:int64), 'is_named_john': LOWER(name) < 'john':string, 'name_0': LOWER(name)})
  SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'name': c_name, 'phone': c_phone})
""",
            id="scan_customer_call_functions",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("region")],
                nation_name=ReferenceInfo("name"),
                region_name=ChildReferenceExpressionInfo("name", 0),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('region_name', region_name)], orderings=[])
 PROJECT(columns={'nation_name': name, 'region_name': name_3})
  JOIN(conditions=[t0.region_key == t1.key], types=['left'], columns={'name': t0.name, 'name_3': t1.name})
   SCAN(table=tpch.NATION, columns={'name': n_name, 'region_key': n_regionkey})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            id="nations_access_region",
        ),
        pytest.param(
            TableCollectionInfo("Lineitems")
            ** CalcInfo(
                [
                    SubCollectionInfo("part_and_supplier")
                    ** SubCollectionInfo("supplier")
                    ** SubCollectionInfo("nation"),
                    SubCollectionInfo("order")
                    ** SubCollectionInfo("customer")
                    ** SubCollectionInfo("nation"),
                ],
                ship_year=FunctionInfo("YEAR", [ReferenceInfo("ship_date")]),
                supplier_nation=ChildReferenceExpressionInfo("name", 0),
                customer_nation=ChildReferenceExpressionInfo("name", 1),
                value=FunctionInfo(
                    "MUL",
                    [
                        ReferenceInfo("extended_price"),
                        FunctionInfo(
                            "SUB",
                            [
                                LiteralInfo(1.0, Float64Type()),
                                ReferenceInfo("discount"),
                            ],
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('ship_year', ship_year), ('supplier_nation', supplier_nation), ('customer_nation', customer_nation), ('value', value)], orderings=[])
 PROJECT(columns={'customer_nation': name_9, 'ship_year': YEAR(ship_date), 'supplier_nation': name_4, 'value': extended_price * 1.0:float64 - discount})
  JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_4': t0.name_4, 'name_9': t1.name_9, 'ship_date': t0.ship_date})
   JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'name_4': t1.name_4, 'order_key': t0.order_key, 'ship_date': t0.ship_date})
    SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'order_key': l_orderkey, 'part_key': l_partkey, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
    JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'name_4': t1.name, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
     JOIN(conditions=[t0.supplier_key == t1.key], types=['inner'], columns={'nation_key': t1.nation_key, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
      SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
      SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
   JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_9': t1.name})
    JOIN(conditions=[t0.customer_key == t1.key], types=['inner'], columns={'key': t0.key, 'nation_key': t1.nation_key})
     SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
     SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
""",
            id="lineitems_access_cust_supplier_nations",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo([], key=LiteralInfo(-1, Int64Type()))
            ** SubCollectionInfo("nations")
            ** CalcInfo([], key=LiteralInfo(-2, Int64Type()))
            ** SubCollectionInfo("customers")
            ** CalcInfo(
                [],
                key=LiteralInfo(-3, Int64Type()),
                name=ReferenceInfo("name"),
                phone=ReferenceInfo("phone"),
                mktsegment=ReferenceInfo("mktsegment"),
            ),
            """
ROOT(columns=[('key', key_0), ('name', name), ('phone', phone), ('mktsegment', mktsegment)], orderings=[])
 PROJECT(columns={'key_0': -3:int64, 'mktsegment': mktsegment, 'name': name_6, 'phone': phone})
  JOIN(conditions=[t0.key_2 == t1.nation_key], types=['inner'], columns={'mktsegment': t1.mktsegment, 'name_6': t1.name, 'phone': t1.phone})
   JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_2': t1.key})
    SCAN(table=tpch.REGION, columns={'key': r_regionkey})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
   SCAN(table=tpch.CUSTOMER, columns={'mktsegment': c_mktsegment, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
""",
            id="join_regions_nations_calc_override",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            """
ROOT(columns=[('region_name', region_name), ('nation_name', nation_name)], orderings=[])
 PROJECT(columns={'nation_name': name_3, 'region_name': name})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'name': t0.name, 'name_3': t1.name})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
   SCAN(table=tpch.NATION, columns={'name': n_name, 'region_key': n_regionkey})
""",
            id="region_nations_backref",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** TableCollectionInfo("nations")
            ** TableCollectionInfo("customers")
            ** TableCollectionInfo("orders")
            ** SubCollectionInfo("lines")
            ** CalcInfo(
                [
                    SubCollectionInfo("part_and_supplier")
                    ** SubCollectionInfo("supplier")
                    ** SubCollectionInfo("nation")
                    ** SubCollectionInfo("region")
                    ** CalcInfo([], nation_name=BackReferenceExpressionInfo("name", 1))
                ],
                order_year=FunctionInfo(
                    "YEAR", [BackReferenceExpressionInfo("order_date", 1)]
                ),
                customer_region=BackReferenceExpressionInfo("name", 4),
                customer_nation=BackReferenceExpressionInfo("name", 3),
                supplier_region=ChildReferenceExpressionInfo("name", 0),
                nation_name=ChildReferenceExpressionInfo("nation_name", 0),
            ),
            """
ROOT(columns=[('order_year', order_year), ('customer_region', customer_region), ('customer_nation', customer_nation), ('supplier_region', supplier_region), ('nation_name', nation_name)], orderings=[])
 PROJECT(columns={'customer_nation': name_3, 'customer_region': name, 'nation_name': nation_name, 'order_year': YEAR(order_date), 'supplier_region': name_16})
  JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'name': t0.name, 'name_16': t1.name_16, 'name_3': t0.name_3, 'nation_name': t1.nation_name, 'order_date': t0.order_date})
   JOIN(conditions=[t0.key_8 == t1.order_key], types=['inner'], columns={'name': t0.name, 'name_3': t0.name_3, 'order_date': t0.order_date, 'part_key': t1.part_key, 'supplier_key': t1.supplier_key})
    JOIN(conditions=[t0.key_5 == t1.customer_key], types=['inner'], columns={'key_8': t1.key, 'name': t0.name, 'name_3': t0.name_3, 'order_date': t1.order_date})
     JOIN(conditions=[t0.key_2 == t1.nation_key], types=['inner'], columns={'key_5': t1.key, 'name': t0.name, 'name_3': t0.name_3})
      JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_2': t1.key, 'name': t0.name, 'name_3': t1.name})
       SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
       SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
     SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey, 'order_date': o_orderdate})
    SCAN(table=tpch.LINEITEM, columns={'order_key': l_orderkey, 'part_key': l_partkey, 'supplier_key': l_suppkey})
   PROJECT(columns={'name_16': name_16, 'nation_name': name_13, 'part_key': part_key, 'supplier_key': supplier_key})
    JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'name_13': t0.name_13, 'name_16': t1.name, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
     JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'name_13': t1.name, 'part_key': t0.part_key, 'region_key': t1.region_key, 'supplier_key': t0.supplier_key})
      JOIN(conditions=[t0.supplier_key == t1.key], types=['inner'], columns={'nation_key': t1.nation_key, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
       SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
       SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
      SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
     SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            id="lines_shipping_vs_customer_region",
        ),
        pytest.param(
            TableCollectionInfo("Orders")
            ** CalcInfo(
                [SubCollectionInfo("lines")],
                okey=ReferenceInfo("key"),
                lsum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                ),
            ),
            """
ROOT(columns=[('okey', okey), ('lsum', lsum)], orderings=[])
 PROJECT(columns={'lsum': DEFAULT_TO(agg_0, 0:int64), 'okey': key})
  JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.ORDER, columns={'key': o_orderkey})
   AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': SUM(extended_price)})
    SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey})
""",
            id="orders_sum_line_price",
        ),
        pytest.param(
            TableCollectionInfo("Customers")
            ** CalcInfo(
                [SubCollectionInfo("orders") ** SubCollectionInfo("lines")],
                okey=ReferenceInfo("key"),
                lsum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                ),
            ),
            """
ROOT(columns=[('okey', okey), ('lsum', lsum)], orderings=[])
 PROJECT(columns={'lsum': DEFAULT_TO(agg_0, 0:int64), 'okey': key})
  JOIN(conditions=[t0.key == t1.customer_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey})
   AGGREGATE(keys={'customer_key': customer_key}, aggregations={'agg_0': SUM(extended_price)})
    JOIN(conditions=[t0.key == t1.order_key], types=['inner'], columns={'customer_key': t0.customer_key, 'extended_price': t1.extended_price})
     SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey})
""",
            id="customers_sum_line_price",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("customers")
                    ** SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines")
                ],
                okey=ReferenceInfo("key"),
                lsum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                ),
            ),
            """
ROOT(columns=[('okey', okey), ('lsum', lsum)], orderings=[])
 PROJECT(columns={'lsum': DEFAULT_TO(agg_0, 0:int64), 'okey': key})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.NATION, columns={'key': n_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': SUM(extended_price)})
    JOIN(conditions=[t0.key_2 == t1.order_key], types=['inner'], columns={'extended_price': t1.extended_price, 'nation_key': t0.nation_key})
     JOIN(conditions=[t0.key == t1.customer_key], types=['inner'], columns={'key_2': t1.key, 'nation_key': t0.nation_key})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
      SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey})
""",
            id="nations_sum_line_price",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalcInfo(
                [
                    SubCollectionInfo("nations")
                    ** SubCollectionInfo("customers")
                    ** SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines")
                ],
                okey=ReferenceInfo("key"),
                lsum=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                ),
            ),
            """
ROOT(columns=[('okey', okey), ('lsum', lsum)], orderings=[])
 PROJECT(columns={'lsum': DEFAULT_TO(agg_0, 0:int64), 'okey': key})
  JOIN(conditions=[t0.key == t1.region_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey})
   AGGREGATE(keys={'region_key': region_key}, aggregations={'agg_0': SUM(extended_price)})
    JOIN(conditions=[t0.key_5 == t1.order_key], types=['inner'], columns={'extended_price': t1.extended_price, 'region_key': t0.region_key})
     JOIN(conditions=[t0.key_2 == t1.customer_key], types=['inner'], columns={'key_5': t1.key, 'region_key': t0.region_key})
      JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'key_2': t1.key, 'region_key': t0.region_key})
       SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
       SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
      SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey})
""",
            id="regions_sum_line_price",
        ),
        pytest.param(
            TableCollectionInfo("Orders")
            ** CalcInfo(
                [SubCollectionInfo("lines")],
                okey=ReferenceInfo("key"),
                lavg=FunctionInfo(
                    "DIV",
                    [
                        FunctionInfo(
                            "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                        ),
                        FunctionInfo(
                            "COUNT", [ChildReferenceExpressionInfo("extended_price", 0)]
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('okey', okey), ('lavg', lavg)], orderings=[])
 PROJECT(columns={'lavg': DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64), 'okey': key})
  JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'agg_1': t1.agg_1, 'key': t0.key})
   SCAN(table=tpch.ORDER, columns={'key': o_orderkey})
   AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': SUM(extended_price), 'agg_1': COUNT(extended_price)})
    SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey})
""",
            id="orders_sum_vs_count_line_price",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("customers"),
                    SubCollectionInfo("suppliers"),
                ],
                nation_name=ReferenceInfo("key"),
                consumer_value=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                producer_value=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 1)]
                ),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('consumer_value', consumer_value), ('producer_value', producer_value)], orderings=[])
 PROJECT(columns={'consumer_value': DEFAULT_TO(agg_0, 0:int64), 'nation_name': key, 'producer_value': DEFAULT_TO(agg_1, 0:int64)})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'key': t0.key})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': SUM(acctbal)})
     SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'nation_key': c_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': SUM(account_balance)})
    SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'nation_key': s_nationkey})
""",
            id="multiple_simple_aggregations_single_calc",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("customers")],
                total_consumer_value=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                avg_consumer_value=FunctionInfo(
                    "AVG", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
            )
            ** CalcInfo(
                [SubCollectionInfo("suppliers")],
                nation_name=ReferenceInfo("key"),
                total_supplier_value=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
                avg_supplier_value=FunctionInfo(
                    "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
            )
            ** CalcInfo(
                [SubCollectionInfo("suppliers"), SubCollectionInfo("customers")],
                nation_name=ReferenceInfo("key"),
                total_consumer_value=ReferenceInfo("total_consumer_value"),
                total_supplier_value=ReferenceInfo("total_supplier_value"),
                avg_consumer_value=ReferenceInfo("avg_consumer_value"),
                avg_supplier_value=ReferenceInfo("avg_supplier_value"),
                best_consumer_value=FunctionInfo(
                    "MAX", [ChildReferenceExpressionInfo("acctbal", 1)]
                ),
                best_supplier_value=FunctionInfo(
                    "MAX", [ChildReferenceExpressionInfo("account_balance", 0)]
                ),
            ),
            """
ROOT(columns=[('nation_name', nation_name_0), ('total_consumer_value', total_consumer_value), ('total_supplier_value', total_supplier_value), ('avg_consumer_value', avg_consumer_value), ('avg_supplier_value', avg_supplier_value), ('best_consumer_value', best_consumer_value), ('best_supplier_value', best_supplier_value)], orderings=[])
 PROJECT(columns={'avg_consumer_value': avg_consumer_value, 'avg_supplier_value': avg_supplier_value, 'best_consumer_value': agg_4, 'best_supplier_value': agg_5, 'nation_name_0': key, 'total_consumer_value': total_consumer_value, 'total_supplier_value': total_supplier_value})
  PROJECT(columns={'agg_4': agg_4, 'agg_5': agg_5, 'avg_consumer_value': avg_consumer_value, 'avg_supplier_value': agg_2, 'key': key, 'total_consumer_value': total_consumer_value, 'total_supplier_value': DEFAULT_TO(agg_3, 0:int64)})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_2': t1.agg_2, 'agg_3': t1.agg_3, 'agg_4': t0.agg_4, 'agg_5': t1.agg_5, 'avg_consumer_value': t0.avg_consumer_value, 'key': t0.key, 'total_consumer_value': t0.total_consumer_value})
    PROJECT(columns={'agg_4': agg_4, 'avg_consumer_value': agg_0, 'key': key, 'total_consumer_value': DEFAULT_TO(agg_1, 0:int64)})
     JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'agg_1': t1.agg_1, 'agg_4': t1.agg_4, 'key': t0.key})
      SCAN(table=tpch.NATION, columns={'key': n_nationkey})
      AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': AVG(acctbal), 'agg_1': SUM(acctbal), 'agg_4': MAX(acctbal)})
       SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'nation_key': c_nationkey})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_2': AVG(account_balance), 'agg_3': SUM(account_balance), 'agg_5': MAX(account_balance)})
     SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'nation_key': s_nationkey})
""",
            id="multiple_simple_aggregations_multiple_calcs",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("customers")],
                nation_name=ReferenceInfo("key"),
                num_customers=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('num_customers', num_customers)], orderings=[])
 PROJECT(columns={'nation_name': key, 'num_customers': DEFAULT_TO(agg_0, 0:int64)})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.NATION, columns={'key': n_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT()})
    SCAN(table=tpch.CUSTOMER, columns={'nation_key': c_nationkey})
""",
            id="count_single_subcollection",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("customers"), SubCollectionInfo("suppliers")],
                nation_name=ReferenceInfo("key"),
                num_customers=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                num_suppliers=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(1)]),
                customer_to_supplier_wealth_ratio=FunctionInfo(
                    "DIV",
                    [
                        FunctionInfo(
                            "SUM", [ChildReferenceExpressionInfo("acctbal", 0)]
                        ),
                        FunctionInfo(
                            "SUM", [ChildReferenceExpressionInfo("account_balance", 1)]
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('num_customers', num_customers), ('num_suppliers', num_suppliers), ('customer_to_supplier_wealth_ratio', customer_to_supplier_wealth_ratio)], orderings=[])
 PROJECT(columns={'customer_to_supplier_wealth_ratio': DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64), 'nation_name': key, 'num_customers': DEFAULT_TO(agg_2, 0:int64), 'num_suppliers': DEFAULT_TO(agg_3, 0:int64)})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'agg_2': t0.agg_2, 'agg_3': t1.agg_3, 'key': t0.key})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'agg_2': t1.agg_2, 'key': t0.key})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': SUM(acctbal), 'agg_2': COUNT()})
     SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'nation_key': c_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': SUM(account_balance), 'agg_3': COUNT()})
    SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'nation_key': s_nationkey})
""",
            id="count_multiple_subcollections_alongside_aggs",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("customers"),
                ],
                nation_name=ReferenceInfo("key"),
                avg_consumer_value=FunctionInfo(
                    "MAX",
                    [
                        FunctionInfo(
                            "IFF",
                            [
                                FunctionInfo(
                                    "LET",
                                    [
                                        ChildReferenceExpressionInfo("acctbal", 0),
                                        LiteralInfo(0.0, Float64Type()),
                                    ],
                                ),
                                LiteralInfo(0.0, Float64Type()),
                                ChildReferenceExpressionInfo("acctbal", 0),
                            ],
                        )
                    ],
                ),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('avg_consumer_value', avg_consumer_value)], orderings=[])
 PROJECT(columns={'avg_consumer_value': agg_0, 'nation_name': key})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.NATION, columns={'key': n_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': MAX(IFF(acctbal < 0.0:float64, 0.0:float64, acctbal))})
    SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'nation_key': c_nationkey})
""",
            id="aggregate_on_function_call",
        ),
        pytest.param(
            TableCollectionInfo("Orders")
            ** CalcInfo(
                [
                    SubCollectionInfo("lines")
                    ** SubCollectionInfo("part_and_supplier")
                    ** CalcInfo(
                        [],
                        ratio=FunctionInfo(
                            "DIV",
                            [
                                BackReferenceExpressionInfo("quantity", 1),
                                ReferenceInfo("availqty"),
                            ],
                        ),
                    ),
                ],
                order_key=ReferenceInfo("key"),
                max_ratio=FunctionInfo(
                    "MAX", [ChildReferenceExpressionInfo("ratio", 0)]
                ),
            ),
            """
ROOT(columns=[('order_key', order_key), ('max_ratio', max_ratio)], orderings=[])
 PROJECT(columns={'max_ratio': agg_0, 'order_key': key})
  JOIN(conditions=[t0.key == t1.order_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key})
   SCAN(table=tpch.ORDER, columns={'key': o_orderkey})
   AGGREGATE(keys={'order_key': order_key}, aggregations={'agg_0': MAX(ratio)})
    PROJECT(columns={'order_key': order_key, 'ratio': quantity / availqty})
     JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['inner'], columns={'availqty': t1.availqty, 'order_key': t0.order_key, 'quantity': t0.quantity})
      SCAN(table=tpch.LINEITEM, columns={'order_key': l_orderkey, 'part_key': l_partkey, 'quantity': l_quantity, 'supplier_key': l_suppkey})
      SCAN(table=tpch.PARTSUPP, columns={'availqty': ps_availqty, 'part_key': ps_partkey, 'supplier_key': ps_suppkey})
""",
            id="aggregate_mixed_levels_simple",
        ),
        pytest.param(
            TableCollectionInfo("Parts")
            ** CalcInfo(
                [SubCollectionInfo("supply_records")],
                name=ReferenceInfo("name"),
                total_delta=FunctionInfo(
                    "SUM",
                    [
                        FunctionInfo(
                            "SUB",
                            [
                                ReferenceInfo("retail_price"),
                                ChildReferenceExpressionInfo("supplycost", 0),
                            ],
                        )
                    ],
                ),
            ),
            """
""",
            id="aggregate_mixed_levels_advanced",
            marks=pytest.mark.skip("TODO"),
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "EQU", [ReferenceInfo("name"), LiteralInfo("ASIA", StringType())]
                ),
            )
            ** SubCollectionInfo("nations"),
            """\
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 PROJECT(columns={'comment': comment_1, 'key': key_2, 'name': name_3, 'region_key': region_key})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment_1': t1.comment, 'key_2': t1.key, 'name_3': t1.name, 'region_key': t1.region_key})
   FILTER(condition=name == 'ASIA':string, columns={'key': key})
    SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
   SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="join_asia_region_nations",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** WhereInfo(
                [SubCollectionInfo("region")],
                FunctionInfo(
                    "EQU",
                    [
                        ChildReferenceExpressionInfo("name", 0),
                        LiteralInfo("ASIA", StringType()),
                    ],
                ),
            ),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 FILTER(condition=name_3 == 'ASIA':string, columns={'comment': comment, 'key': key, 'name': name, 'region_key': region_key})
  JOIN(conditions=[t0.region_key == t1.key], types=['left'], columns={'comment': t0.comment, 'key': t0.key, 'name': t0.name, 'name_3': t1.name, 'region_key': t0.region_key})
   SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            id="asian_regions",
        ),
        pytest.param(
            TableCollectionInfo("Lineitems")
            ** WhereInfo(
                [
                    SubCollectionInfo("supplier") ** SubCollectionInfo("nation"),
                    SubCollectionInfo("part"),
                ],
                FunctionInfo(
                    "BAN",
                    [
                        FunctionInfo(
                            "EQU",
                            [
                                ChildReferenceExpressionInfo("name", 0),
                                LiteralInfo("GERMANY", StringType()),
                            ],
                        ),
                        FunctionInfo(
                            "STARTSWITH",
                            [
                                ChildReferenceExpressionInfo("part_type", 1),
                                LiteralInfo("ECONOMY", StringType()),
                            ],
                        ),
                    ],
                ),
            ),
            """

""",
            id="lines_german_supplier_economy_part",
            marks=pytest.mark.skip("TODO"),
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "CONTAINS",
                    [ReferenceInfo("name"), BackReferenceExpressionInfo("name", 1)],
                ),
            ),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 PROJECT(columns={'comment': comment_1, 'key': key_2, 'name': name_3, 'region_key': region_key})
  FILTER(condition=CONTAINS(name_3, name), columns={'comment_1': comment_1, 'key_2': key_2, 'name_3': name_3, 'region_key': region_key})
   JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment_1': t1.comment, 'key_2': t1.key, 'name': t0.name, 'name_3': t1.name, 'region_key': t1.region_key})
    SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
    SCAN(table=tpch.NATION, columns={'comment': n_comment, 'key': n_nationkey, 'name': n_name, 'region_key': n_regionkey})
""",
            id="nation_name_contains_region_name",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** SubCollectionInfo("customers")
            ** SubCollectionInfo("orders")
            ** SubCollectionInfo("lines")
            ** WhereInfo(
                [
                    SubCollectionInfo("part_and_supplier")
                    ** SubCollectionInfo("supplier")
                    ** SubCollectionInfo("nation")
                    ** SubCollectionInfo("region")
                ],
                FunctionInfo(
                    "EQU",
                    [
                        BackReferenceExpressionInfo("name", 4),
                        ChildReferenceExpressionInfo("name", 0),
                    ],
                ),
            )
            ** CalcInfo(
                [],
                rname=BackReferenceExpressionInfo("name", 4),
                price=ReferenceInfo("extended_price"),
            ),
            """
ROOT(columns=[('rname', rname), ('price', price)], orderings=[])
 PROJECT(columns={'price': extended_price, 'rname': name})
  FILTER(condition=name == name_16, columns={'extended_price': extended_price, 'name': name})
   JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'extended_price': t0.extended_price, 'name': t0.name, 'name_16': t1.name_16})
    JOIN(conditions=[t0.key_8 == t1.order_key], types=['inner'], columns={'extended_price': t1.extended_price, 'name': t0.name, 'part_key': t1.part_key, 'supplier_key': t1.supplier_key})
     JOIN(conditions=[t0.key_5 == t1.customer_key], types=['inner'], columns={'key_8': t1.key, 'name': t0.name})
      JOIN(conditions=[t0.key_2 == t1.nation_key], types=['inner'], columns={'key_5': t1.key, 'name': t0.name})
       JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_2': t1.key, 'name': t0.name})
        SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
        SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
       SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
      SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey, 'part_key': l_partkey, 'supplier_key': l_suppkey})
    JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'name_16': t1.name, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
     JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'part_key': t0.part_key, 'region_key': t1.region_key, 'supplier_key': t0.supplier_key})
      JOIN(conditions=[t0.supplier_key == t1.key], types=['inner'], columns={'nation_key': t1.nation_key, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
       SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
       SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
      SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
     SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            id="lineitem_regional_shipments",
        ),
        pytest.param(
            TableCollectionInfo("Lineitems")
            ** WhereInfo(
                [
                    SubCollectionInfo("order")
                    ** SubCollectionInfo("customer")
                    ** SubCollectionInfo("nation")
                    ** SubCollectionInfo("region"),
                    SubCollectionInfo("part_and_supplier")
                    ** SubCollectionInfo("supplier")
                    ** SubCollectionInfo("nation")
                    ** SubCollectionInfo("region"),
                ],
                FunctionInfo(
                    "EQU",
                    [
                        ChildReferenceExpressionInfo("name", 0),
                        ChildReferenceExpressionInfo("name", 1),
                    ],
                ),
            )
            ** CalcInfo(
                [
                    SubCollectionInfo("order")
                    ** SubCollectionInfo("customer")
                    ** SubCollectionInfo("nation")
                    ** SubCollectionInfo("region")
                ],
                rname=ChildReferenceExpressionInfo("name", 0),
                price=ReferenceInfo("extended_price"),
            ),
            """
ROOT(columns=[('rname', rname), ('price', price)], orderings=[])
 PROJECT(columns={'price': extended_price, 'rname': name_8})
  FILTER(condition=name_8 == name_15, columns={'extended_price': extended_price, 'name_8': name_8})
   JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'extended_price': t0.extended_price, 'name_15': t1.name_15, 'name_8': t0.name_8})
    JOIN(conditions=[t0.order_key == t1.key], types=['left'], columns={'extended_price': t0.extended_price, 'name_8': t1.name_8, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
     SCAN(table=tpch.LINEITEM, columns={'extended_price': l_extendedprice, 'order_key': l_orderkey, 'part_key': l_partkey, 'supplier_key': l_suppkey})
     JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'key': t0.key, 'name_8': t1.name})
      JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'key': t0.key, 'region_key': t1.region_key})
       JOIN(conditions=[t0.customer_key == t1.key], types=['inner'], columns={'key': t0.key, 'nation_key': t1.nation_key})
        SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
        SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
       SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
      SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
    JOIN(conditions=[t0.region_key == t1.key], types=['inner'], columns={'name_15': t1.name, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
     JOIN(conditions=[t0.nation_key == t1.key], types=['inner'], columns={'part_key': t0.part_key, 'region_key': t1.region_key, 'supplier_key': t0.supplier_key})
      JOIN(conditions=[t0.supplier_key == t1.key], types=['inner'], columns={'nation_key': t1.nation_key, 'part_key': t0.part_key, 'supplier_key': t0.supplier_key})
       SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
       SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
      SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
     SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
""",
            id="lineitem_regional_shipments2",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** SubCollectionInfo("customers")
            ** SubCollectionInfo("orders")
            ** SubCollectionInfo("lines")
            ** SubCollectionInfo("order")
            ** SubCollectionInfo("customer")
            ** SubCollectionInfo("nation")
            ** SubCollectionInfo("region")
            ** WhereInfo(
                [],
                FunctionInfo(
                    "EQU",
                    [
                        ReferenceInfo("name"),
                        BackReferenceExpressionInfo("name", 8),
                    ],
                ),
            ),
            """
ROOT(columns=[('key', key), ('name', name), ('comment', comment)], orderings=[])
 PROJECT(columns={'comment': comment_31, 'key': key_32, 'name': name_33})
  FILTER(condition=name_33 == name, columns={'comment_31': comment_31, 'key_32': key_32, 'name_33': name_33})
   JOIN(conditions=[t0.region_key_30 == t1.key], types=['inner'], columns={'comment_31': t1.comment, 'key_32': t1.key, 'name': t0.name, 'name_33': t1.name})
    JOIN(conditions=[t0.nation_key_25 == t1.key], types=['inner'], columns={'name': t0.name, 'region_key_30': t1.region_key})
     JOIN(conditions=[t0.customer_key_12 == t1.key], types=['inner'], columns={'name': t0.name, 'nation_key_25': t1.nation_key})
      JOIN(conditions=[t0.order_key == t1.key], types=['inner'], columns={'customer_key_12': t1.customer_key, 'name': t0.name})
       JOIN(conditions=[t0.key_8 == t1.order_key], types=['inner'], columns={'name': t0.name, 'order_key': t1.order_key})
        JOIN(conditions=[t0.key_5 == t1.customer_key], types=['inner'], columns={'key_8': t1.key, 'name': t0.name})
         JOIN(conditions=[t0.key_2 == t1.nation_key], types=['inner'], columns={'key_5': t1.key, 'name': t0.name})
          JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_2': t1.key, 'name': t0.name})
           SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
           SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
          SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
         SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
        SCAN(table=tpch.LINEITEM, columns={'order_key': l_orderkey})
       SCAN(table=tpch.ORDER, columns={'customer_key': o_custkey, 'key': o_orderkey})
      SCAN(table=tpch.CUSTOMER, columns={'key': c_custkey, 'nation_key': c_nationkey})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
    SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
""",
            id="lineitem_regional_shipments3",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("suppliers")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "GRT",
                            [
                                ReferenceInfo("account_balance"),
                                LiteralInfo(0.0, Float64Type()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers"),
                ],
                name=ReferenceInfo("name"),
                suppliers_in_black=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                ),
                total_suppliers=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 1)]
                ),
            ),
            """
ROOT(columns=[('name', name), ('suppliers_in_black', suppliers_in_black), ('total_suppliers', total_suppliers)], orderings=[])
 PROJECT(columns={'name': name, 'suppliers_in_black': DEFAULT_TO(agg_0, 0:int64), 'total_suppliers': DEFAULT_TO(agg_1, 0:int64)})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'name': t0.name})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key, 'name': t0.name})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT(key)})
     FILTER(condition=account_balance > 0.0:float64, columns={'key': key, 'nation_key': nation_key})
      SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'key': s_suppkey, 'nation_key': s_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': COUNT(key)})
    SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
""",
            id="num_positive_accounts_per_nation",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo([], name=ReferenceInfo("name"))
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "GRT",
                            [
                                ReferenceInfo("account_balance"),
                                LiteralInfo(0.0, Float64Type()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers"),
                ],
                FunctionInfo(
                    "GRT",
                    [
                        FunctionInfo("COUNT", [ChildReferenceExpressionInfo("key", 0)]),
                        FunctionInfo(
                            "MUL",
                            [
                                LiteralInfo(0.5, Float64Type()),
                                FunctionInfo(
                                    "COUNT", [ChildReferenceExpressionInfo("key", 1)]
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('name', name)], orderings=[])
 FILTER(condition=DEFAULT_TO(agg_0, 0:int64) > 0.5:float64 * DEFAULT_TO(agg_1, 0:int64), columns={'name': name})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'name': t0.name})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key, 'name': t0.name})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT(key)})
     FILTER(condition=account_balance > 0.0:float64, columns={'key': key, 'nation_key': nation_key})
      SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'key': s_suppkey, 'nation_key': s_nationkey})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': COUNT(key)})
    SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
""",
            id="mostly_positive_accounts_per_nation1",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("suppliers")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "GRT",
                            [
                                ReferenceInfo("account_balance"),
                                LiteralInfo(0.0, Float64Type()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers"),
                ],
                name=ReferenceInfo("name"),
                suppliers_in_black=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                ),
                total_suppliers=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 1)]
                ),
            )
            ** WhereInfo(
                [
                    SubCollectionInfo("suppliers")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "GRT",
                            [
                                ReferenceInfo("account_balance"),
                                LiteralInfo(0.0, Float64Type()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers"),
                ],
                FunctionInfo(
                    "GRT",
                    [
                        FunctionInfo("COUNT", [ChildReferenceExpressionInfo("key", 0)]),
                        FunctionInfo(
                            "MUL",
                            [
                                LiteralInfo(0.5, Float64Type()),
                                FunctionInfo(
                                    "COUNT", [ChildReferenceExpressionInfo("key", 1)]
                                ),
                            ],
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('name', name), ('suppliers_in_black', suppliers_in_black), ('total_suppliers', total_suppliers)], orderings=[])
 FILTER(condition=DEFAULT_TO(agg_2, 0:int64) > 0.5:float64 * DEFAULT_TO(agg_3, 0:int64), columns={'name': name, 'suppliers_in_black': suppliers_in_black, 'total_suppliers': total_suppliers})
  PROJECT(columns={'agg_2': agg_2, 'agg_3': agg_3, 'name': name, 'suppliers_in_black': DEFAULT_TO(agg_0, 0:int64), 'total_suppliers': DEFAULT_TO(agg_1, 0:int64)})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'agg_2': t0.agg_2, 'agg_3': t1.agg_3, 'name': t0.name})
    JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'agg_2': t1.agg_2, 'key': t0.key, 'name': t0.name})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
     AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT(key), 'agg_2': COUNT(key)})
      FILTER(condition=account_balance > 0.0:float64, columns={'key': key, 'nation_key': nation_key})
       SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'key': s_suppkey, 'nation_key': s_nationkey})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': COUNT(key), 'agg_3': COUNT(key)})
     SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
""",
            id="mostly_positive_accounts_per_nation2",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    SubCollectionInfo("suppliers")
                    ** WhereInfo(
                        [],
                        FunctionInfo(
                            "GRT",
                            [
                                ReferenceInfo("account_balance"),
                                LiteralInfo(0.0, Float64Type()),
                            ],
                        ),
                    ),
                    SubCollectionInfo("suppliers"),
                ],
                name=ReferenceInfo("name"),
                suppliers_in_black=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                ),
                total_suppliers=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 1)]
                ),
            )
            ** WhereInfo(
                [],
                FunctionInfo(
                    "GRT",
                    [
                        ReferenceInfo("suppliers_in_black"),
                        FunctionInfo(
                            "MUL",
                            [
                                LiteralInfo(0.5, Float64Type()),
                                ReferenceInfo("total_suppliers"),
                            ],
                        ),
                    ],
                ),
            ),
            """
ROOT(columns=[('name', name), ('suppliers_in_black', suppliers_in_black), ('total_suppliers', total_suppliers)], orderings=[])
 FILTER(condition=suppliers_in_black > 0.5:float64 * total_suppliers, columns={'name': name, 'suppliers_in_black': suppliers_in_black, 'total_suppliers': total_suppliers})
  PROJECT(columns={'name': name, 'suppliers_in_black': DEFAULT_TO(agg_0, 0:int64), 'total_suppliers': DEFAULT_TO(agg_1, 0:int64)})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t0.agg_0, 'agg_1': t1.agg_1, 'name': t0.name})
    JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'key': t0.key, 'name': t0.name})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
     AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT(key)})
      FILTER(condition=account_balance > 0.0:float64, columns={'key': key, 'nation_key': nation_key})
       SCAN(table=tpch.SUPPLIER, columns={'account_balance': s_acctbal, 'key': s_suppkey, 'nation_key': s_nationkey})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_1': COUNT(key)})
     SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
""",
            id="mostly_positive_accounts_per_nation3",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** TopKInfo([], 2),
            """
ROOT(columns=[('key', key), ('name', name), ('comment', comment)], orderings=[])
 LIMIT(limit=Literal(value=2, type=Int64Type()), columns={'comment': comment, 'key': key, 'name': name}, orderings=[])
  SCAN(table=tpch.REGION, columns={'comment': r_comment, 'key': r_regionkey, 'name': r_name})
""",
            id="simple_topk",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** TopKInfo([], 10)
            ** CalcInfo(
                [],
                region_name=BackReferenceExpressionInfo("name", 1),
                nation_name=ReferenceInfo("name"),
            ),
            """
ROOT(columns=[('region_name', region_name), ('nation_name', nation_name)], orderings=[])
 PROJECT(columns={'nation_name': name_3, 'region_name': name})
  LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'name': name, 'name_3': name_3}, orderings=[])
   JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'name': t0.name, 'name_3': t1.name})
    SCAN(table=tpch.REGION, columns={'key': r_regionkey, 'name': r_name})
    SCAN(table=tpch.NATION, columns={'name': n_name, 'region_key': n_regionkey})
""",
            id="join_topk",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [
                    # TODO: Add an ordering to the topk
                    SubCollectionInfo("suppliers") ** TopKInfo([], 100),
                ],
                name=ReferenceInfo("name"),
                n_top_suppliers=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                ),
            )
            ** TopKInfo([], 10),
            """
ROOT(columns=[('name', name), ('n_top_suppliers', n_top_suppliers)], orderings=[])
 LIMIT(limit=Literal(value=10, type=Int64Type()), columns={'n_top_suppliers': n_top_suppliers, 'name': name}, orderings=[])
  PROJECT(columns={'n_top_suppliers': DEFAULT_TO(agg_0, 0:int64), 'name': name})
   JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'name': t0.name})
    SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
    AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': COUNT(key)})
     LIMIT(limit=Literal(value=100, type=Int64Type()), columns={'key': key, 'nation_key': nation_key}, orderings=[])
      SCAN(table=tpch.SUPPLIER, columns={'key': s_suppkey, 'nation_key': s_nationkey})
""",
            id="count_at_most_100_suppliers_per_nation",
        ),
    ],
)
def test_ast_to_relational(
    calc_pipeline: CollectionTestInfo,
    expected_relational_string: str,
    tpch_node_builder: AstNodeBuilder,
    default_config: PyDoughConfigs,
) -> None:
    """
    Tests whether the AST nodes are correctly translated into Relational nodes
    with the expected string representation.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    relational = convert_ast_to_relational(collection, default_config)
    assert (
        relational.to_tree_string() == expected_relational_string.strip()
    ), "Mismatch between full string representation of output Relational node versus expected string"


@pytest.mark.parametrize(
    "calc_pipeline, expected_relational_string",
    [
        pytest.param(
            TableCollectionInfo("Nations")
            ** CalcInfo(
                [SubCollectionInfo("customers")],
                nation_name=ReferenceInfo("name"),
                total_bal=FunctionInfo(
                    "SUM", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                num_bal=FunctionInfo(
                    "COUNT", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                avg_bal=FunctionInfo(
                    "AVG", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                min_bal=FunctionInfo(
                    "MIN", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                max_bal=FunctionInfo(
                    "MAX", [ChildReferenceExpressionInfo("acctbal", 0)]
                ),
                num_cust=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
            ),
            """
ROOT(columns=[('nation_name', nation_name), ('total_bal', total_bal), ('num_bal', num_bal), ('avg_bal', avg_bal), ('min_bal', min_bal), ('max_bal', max_bal), ('num_cust', num_cust)], orderings=[])
 PROJECT(columns={'avg_bal': DEFAULT_TO(agg_0, 0:int64), 'max_bal': agg_1, 'min_bal': agg_2, 'nation_name': name, 'num_bal': DEFAULT_TO(agg_3, 0:int64), 'num_cust': DEFAULT_TO(agg_4, 0:int64), 'total_bal': agg_5})
  JOIN(conditions=[t0.key == t1.nation_key], types=['left'], columns={'agg_0': t1.agg_0, 'agg_1': t1.agg_1, 'agg_2': t1.agg_2, 'agg_3': t1.agg_3, 'agg_4': t1.agg_4, 'agg_5': t1.agg_5, 'name': t0.name})
   SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'name': n_name})
   AGGREGATE(keys={'nation_key': nation_key}, aggregations={'agg_0': AVG(acctbal), 'agg_1': MAX(acctbal), 'agg_2': MIN(acctbal), 'agg_3': COUNT(acctbal), 'agg_4': COUNT(), 'agg_5': SUM(acctbal)})
    SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'nation_key': c_nationkey})
""",
            id="various_aggfuncs_simple",
        ),
    ],
)
def test_ast_to_relational_alternative_aggregation_configs(
    calc_pipeline: CollectionTestInfo,
    expected_relational_string: str,
    tpch_node_builder: AstNodeBuilder,
    default_config: PyDoughConfigs,
) -> None:
    """
    Same as `test_ast_to_relational` but with various alternative aggregation
    configs:
    - `SUM` defaulting to zero is disabled.
    - `COUNT` defaulting to zero is disabled.
    """
    default_config.sum_default_zero = False
    default_config.avg_default_zero = True
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    relational = convert_ast_to_relational(collection, default_config)
    assert (
        relational.to_tree_string() == expected_relational_string.strip()
    ), "Mismatch between full string representation of output Relational node versus expected string"
