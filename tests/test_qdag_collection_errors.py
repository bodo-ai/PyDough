"""
Error-handling unit tests for PyDough QDAG nodes for collections.
"""

import re

import pytest
from test_utils import (
    AstNodeTestInfo,
    BackReferenceExpressionInfo,
    CalculateInfo,
    ChildReferenceExpressionInfo,
    FunctionInfo,
    OrderInfo,
    PartitionInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    TopKInfo,
    WhereInfo,
)

from pydough.qdag import AstNodeBuilder


@pytest.mark.parametrize(
    "calc_pipeline, error_message",
    [
        pytest.param(
            TableCollectionInfo("Rainbows"),
            "Unrecognized term of graph 'TPCH': 'Rainbows'",
            id="table_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("postage_stamps"),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'postage_stamps'",
            id="subcollection_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalculateInfo([], foo=ReferenceInfo("bar")),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'bar'",
            id="reference_dne",
        ),
        pytest.param(
            TableCollectionInfo("Nations")
            ** SubCollectionInfo("suppliers")
            ** CalculateInfo([], foo=ReferenceInfo("region_key")),
            "Unrecognized term of simple table collection 'Suppliers' in graph 'TPCH': 'region_key'",
            id="reference_bad_ancestry",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 0)),
            "Expected number of levels in BACK to be a positive integer, received 0",
            id="back_zero",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of graph 'TPCH': 'foo'",
            id="back_on_root",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 3)),
            "Cannot reference back 3 levels above TPCH.Regions.nations",
            id="back_too_far",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** SubCollectionInfo("nations")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'foo'",
            id="back_dne",
        ),
        pytest.param(
            CalculateInfo([], foo=ChildReferenceExpressionInfo("foo", 0)),
            "Invalid child reference index 0 with 0 children",
            id="child_dne",
        ),
        pytest.param(
            CalculateInfo(
                [TableCollectionInfo("Regions")],
                foo=ChildReferenceExpressionInfo("bar", 0),
            ),
            "Unrecognized term of simple table collection 'Regions' in graph 'TPCH': 'bar'",
            id="child_expr_dne",
        ),
        pytest.param(
            TableCollectionInfo("Regions")
            ** CalculateInfo(
                [SubCollectionInfo("nations")],
                nation_name=ChildReferenceExpressionInfo("name", 0),
            ),
            "Expected all terms in CALCULATE(nation_name=nations.name) to be singular, but encountered a plural expression: nations.name",
            id="bad_plural_a",
        ),
        pytest.param(
            TableCollectionInfo("Customers")
            ** CalculateInfo(
                [
                    SubCollectionInfo("nation")
                    ** SubCollectionInfo("region")
                    ** SubCollectionInfo("nations")
                ],
                nation_name=ChildReferenceExpressionInfo("name", 0),
            ),
            "Expected all terms in CALCULATE(nation_name=nation.region.nations.name) to be singular, but encountered a plural expression: nation.region.nations.name",
            id="bad_plural_b",
        ),
        pytest.param(
            TableCollectionInfo("Parts")
            ** SubCollectionInfo("supply_records")
            ** CalculateInfo(
                [SubCollectionInfo("lines")],
                extended_price=ChildReferenceExpressionInfo("extended_price", 0),
            ),
            "Expected all terms in CALCULATE(extended_price=lines.extended_price) to be singular, but encountered a plural expression: lines.extended_price",
            id="bad_plural_c",
        ),
        pytest.param(
            CalculateInfo(
                [TableCollectionInfo("Customers")],
                cust_name=ChildReferenceExpressionInfo("name", 0),
            ),
            "Expected all terms in CALCULATE(cust_name=Customers.name) to be singular, but encountered a plural expression: Customers.name",
            id="bad_plural_d",
        ),
        pytest.param(
            TableCollectionInfo("Orders")
            ** WhereInfo(
                [SubCollectionInfo("lines")],
                FunctionInfo(
                    "EQU",
                    [
                        ReferenceInfo("order_date"),
                        ChildReferenceExpressionInfo("ship_date", 0),
                    ],
                ),
            ),
            "Expected all terms in WHERE(order_date == lines.ship_date) to be singular, but encountered a plural expression: order_date == lines.ship_date",
            id="bad_plural_e",
        ),
        pytest.param(
            TableCollectionInfo("Customers")
            ** OrderInfo(
                [SubCollectionInfo("orders")],
                (ChildReferenceExpressionInfo("order_date", 0), True, True),
            ),
            "Expected all terms in ORDER_BY(orders.order_date.ASC(na_pos='last')) to be singular, but encountered a plural expression: orders.order_date.ASC(na_pos='last')",
            id="bad_plural_f",
        ),
        pytest.param(
            TableCollectionInfo("Customers")
            ** TopKInfo(
                [SubCollectionInfo("orders")],
                5,
                (ChildReferenceExpressionInfo("order_date", 0), True, True),
            ),
            "Expected all terms in TOP_K(5, orders.order_date.ASC(na_pos='last')) to be singular, but encountered a plural expression: orders.order_date.ASC(na_pos='last')",
            id="bad_plural_g",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("Parts"),
                "parts",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts")],
                container=ReferenceInfo("container"),
                price=ChildReferenceExpressionInfo("retail_price", 0),
            ),
            "Expected all terms in CALCULATE(container=container, price=parts.retail_price) to be singular, but encountered a plural expression: parts.retail_price",
            id="bad_plural_h",
        ),
        pytest.param(
            PartitionInfo(
                TableCollectionInfo("Parts"),
                "parts",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [SubCollectionInfo("parts") ** SubCollectionInfo("suppliers_of_part")],
                container=ReferenceInfo("container"),
                balance=ChildReferenceExpressionInfo("account_balance", 0),
            ),
            "Expected all terms in CALCULATE(container=container, balance=parts.suppliers_of_part.account_balance) to be singular, but encountered a plural expression: parts.suppliers_of_part.account_balance",
            id="bad_plural_i",
        ),
    ],
)
def test_malformed_collection_sequences(
    calc_pipeline: AstNodeTestInfo,
    error_message: str,
    tpch_node_builder: AstNodeBuilder,
) -> None:
    """
    Tests that building a malformed sequence of collections produces the
    expected error message.
    """
    with pytest.raises(Exception, match=re.escape(error_message)):
        calc_pipeline.build(tpch_node_builder)
