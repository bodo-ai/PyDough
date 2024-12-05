"""
TODO: add file-level docstring.
"""

from collections.abc import Callable

import pytest
from test_qualification import (
    pydough_impl_tpch_q6,
    pydough_impl_tpch_q10,
    pydough_impl_tpch_q14,
    pydough_impl_tpch_q18,
    pydough_impl_tpch_q19,
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
                pydough_impl_tpch_q14,
                """
ROOT(columns=[('promo_revenue', promo_revenue)], orderings=[])
 PROJECT(columns={'promo_revenue': 100.0:float64 * DEFAULT_TO(agg_0, 0:int64) / DEFAULT_TO(agg_1, 0:int64)})
  AGGREGATE(keys={}, aggregations={'agg_0': SUM(promo_value), 'agg_1': SUM(value)})
   PROJECT(columns={'promo_value': IFF(STARTSWITH(part_type, 'PROMO':string), extended_price * 1:int64 - discount, 0:int64), 'value': extended_price * 1:int64 - discount})
    JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'discount': t0.discount, 'extended_price': t0.extended_price, 'part_type': t1.part_type})
     FILTER(condition=ship_date >= datetime.date(1995, 9, 1):date & ship_date < datetime.date(1995, 10, 1):date, columns={'discount': discount, 'extended_price': extended_price, 'part_key': part_key, 'supplier_key': supplier_key})
      SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'ship_date': l_shipdate, 'supplier_key': l_suppkey})
     JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'part_key': t0.part_key, 'part_type': t1.part_type, 'supplier_key': t0.supplier_key})
      SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
      SCAN(table=tpch.PART, columns={'key': p_partkey, 'part_type': p_type})
""",
            ),
            id="tpch_q14",
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
    JOIN(conditions=[t0.part_key == t1.part_key & t0.supplier_key == t1.supplier_key], types=['left'], columns={'brand': t1.brand, 'container': t1.container, 'discount': t0.discount, 'extended_price': t0.extended_price, 'quantity': t0.quantity, 'ship_instruct': t0.ship_instruct, 'size': t1.size})
     SCAN(table=tpch.LINEITEM, columns={'discount': l_discount, 'extended_price': l_extendedprice, 'part_key': l_partkey, 'quantity': l_quantity, 'ship_instruct': l_shipinstruct, 'supplier_key': l_suppkey})
     JOIN(conditions=[t0.part_key == t1.key], types=['inner'], columns={'brand': t1.brand, 'container': t1.container, 'part_key': t0.part_key, 'size': t1.size, 'supplier_key': t0.supplier_key})
      SCAN(table=tpch.PARTSUPP, columns={'part_key': ps_partkey, 'supplier_key': ps_suppkey})
      SCAN(table=tpch.PART, columns={'brand': p_brand, 'container': p_container, 'key': p_partkey, 'size': p_size})
""",
            ),
            id="tpch_q19",
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
