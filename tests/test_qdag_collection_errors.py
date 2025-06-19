"""
Error-handling unit tests for PyDough QDAG nodes for collections.
"""

import re

import pytest

from pydough.qdag import AstNodeBuilder
from tests.testing_utilities import (
    AstNodeTestInfo,
    BackReferenceExpressionInfo,
    CalculateInfo,
    ChildReferenceExpressionInfo,
    FunctionInfo,
    OrderInfo,
    PartitionInfo,
    ReferenceInfo,
    SingularInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    TopKInfo,
    WhereInfo,
)


@pytest.mark.parametrize(
    "calc_pipeline, error_message",
    [
        pytest.param(
            TableCollectionInfo("Rainbows"),
            "Unrecognized term of TPCH: 'Rainbows' Did you mean: lines, nations, regions, parts, orders?",
            id="table_dne",
        ),
        pytest.param(
            TableCollectionInfo("regions") ** SubCollectionInfo("postage_stamps"),
            "Unrecognized term of TPCH.regions: 'postage_stamps' Did you mean: comment, nations, name, key?",
            id="subcollection_dne",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo([], foo=ReferenceInfo("bar")),
            "Unrecognized term of TPCH.regions: 'bar' Did you mean: key, name, nations?",
            id="reference_dne",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** SubCollectionInfo("suppliers")
            ** CalculateInfo([], foo=ReferenceInfo("region_key")),
            "Unrecognized term of TPCH.nations.suppliers: 'region_key' Did you mean: nation_key, key, lines?",
            id="reference_bad_ancestry",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 0)),
            "Expected number of levels in BACK to be a positive integer, received 0",
            id="back_zero",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of TPCH: 'foo' Did you mean: lines, parts, nations, orders, regions?",
            id="back_on_root",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** SubCollectionInfo("nations")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 3)),
            "Cannot reference back 3 levels above TPCH.regions.nations",
            id="back_too_far",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** SubCollectionInfo("nations")
            ** CalculateInfo([], foo=BackReferenceExpressionInfo("foo", 1)),
            "Unrecognized term of TPCH.regions: 'foo' Did you mean: key, name, comment?",
            id="back_dne",
        ),
        pytest.param(
            CalculateInfo([], foo=ChildReferenceExpressionInfo("foo", 0)),
            "Invalid child reference index 0 with 0 children",
            id="child_dne",
        ),
        pytest.param(
            CalculateInfo(
                [TableCollectionInfo("regions")],
                foo=ChildReferenceExpressionInfo("bar", 0),
            ),
            "Unrecognized term of TPCH.regions: 'bar' Did you mean: key, name, nations?",
            id="child_expr_dne",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** WhereInfo(
                [SubCollectionInfo("orders")],
                FunctionInfo(
                    "HAS",
                    [
                        ChildReferenceExpressionInfo("order_date", 0),
                    ],
                ),
            ),
            "Invalid operator invocation 'HAS(orders.order_date)': Expected a collection as an argument, received an expression",
            id="has_on_expression",
        ),
        pytest.param(
            TableCollectionInfo("customers")
            ** WhereInfo(
                [SubCollectionInfo("orders")],
                FunctionInfo(
                    "HASNOT",
                    [
                        ChildReferenceExpressionInfo("order_date", 0),
                    ],
                ),
            ),
            "Invalid operator invocation 'HASNOT(orders.order_date)': Expected a collection as an argument, received an expression",
            id="hasnot_on_expression",
        ),
        pytest.param(
            TableCollectionInfo("regions")
            ** CalculateInfo(
                [SubCollectionInfo("nations")],
                nation_name=ChildReferenceExpressionInfo("name", 0),
            ),
            "Expected all terms in CALCULATE(nation_name=nations.name) to be singular, but encountered a plural expression: nations.name",
            id="bad_plural_a",
        ),
        pytest.param(
            TableCollectionInfo("customers")
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
            TableCollectionInfo("parts")
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
                [TableCollectionInfo("customers")],
                cust_name=ChildReferenceExpressionInfo("name", 0),
            ),
            "Expected all terms in CALCULATE(cust_name=customers.name) to be singular, but encountered a plural expression: customers.name",
            id="bad_plural_d",
        ),
        pytest.param(
            TableCollectionInfo("orders")
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
            TableCollectionInfo("customers")
            ** OrderInfo(
                [SubCollectionInfo("orders")],
                (ChildReferenceExpressionInfo("order_date", 0), True, True),
            ),
            "Expected all terms in ORDER_BY(orders.order_date.ASC(na_pos='last')) to be singular, but encountered a plural expression: orders.order_date.ASC(na_pos='last')",
            id="bad_plural_f",
        ),
        pytest.param(
            TableCollectionInfo("customers")
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
                TableCollectionInfo("parts"),
                "containers",
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
                TableCollectionInfo("parts"),
                "containers",
                [ChildReferenceExpressionInfo("container", 0)],
            )
            ** CalculateInfo(
                [
                    SubCollectionInfo("parts")
                    ** SubCollectionInfo("supply_records")
                    ** SubCollectionInfo("supplier")
                ],
                container=ReferenceInfo("container"),
                balance=ChildReferenceExpressionInfo("account_balance", 0),
            ),
            "Expected all terms in CALCULATE(container=container, balance=parts.supply_records.supplier.account_balance) to be singular, but encountered a plural expression: parts.supply_records.supplier.account_balance",
            id="bad_plural_i",
        ),
        pytest.param(
            TableCollectionInfo("nations")
            ** CalculateInfo(
                [
                    SubCollectionInfo("customers")
                    ** SingularInfo()
                    ** SubCollectionInfo("orders")
                ],
                name=ReferenceInfo("name"),
                okey=ChildReferenceExpressionInfo("key", 0),
            ),
            "Expected all terms in CALCULATE(name=name, okey=customers.SINGULAR.orders.key) to be singular, but encountered a plural expression: customers.SINGULAR.orders.key",
            id="bad_plural_j",
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
