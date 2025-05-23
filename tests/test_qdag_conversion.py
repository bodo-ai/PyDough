"""
Unit tests for the process of converting qualified PyDough QDAG nodes into the
relational tree.
"""

from collections.abc import Callable

import pytest
from test_utils import (
    BackReferenceExpressionInfo,
    CalculateInfo,
    ChildReferenceCollectionInfo,
    ChildReferenceExpressionInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    OrderInfo,
    PartitionInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    TopKInfo,
    WhereInfo,
    WindowInfo,
)

from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.qdag import AstNodeBuilder, PyDoughCollectionQDAG
from pydough.types import (
    BooleanType,
    NumericType,
    StringType,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                TableCollectionInfo("regions"),
                "scan_regions",
            ),
            id="scan_regions",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations"),
                "scan_nations",
            ),
            id="scan_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** CalculateInfo(
                    [],
                    region_name=ReferenceInfo("name"),
                    magic_word=LiteralInfo("foo", StringType()),
                ),
                "scan_calc",
            ),
            id="scan_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** CalculateInfo([], hello=LiteralInfo("foo", StringType()))
                ** CalculateInfo(
                    [], fizz=ReferenceInfo("name"), buzz=ReferenceInfo("key")
                ),
                "scan_calc_calc",
            ),
            id="scan_calc_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions") ** SubCollectionInfo("nations"),
                "join_region_nations",
            ),
            id="join_region_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** SubCollectionInfo("customers"),
                "join_region_nations_customers",
            ),
            id="join_region_nations_customers",
        ),
        pytest.param(
            (
                TableCollectionInfo("customers")
                ** CalculateInfo(
                    [],
                    lname=FunctionInfo("LOWER", [ReferenceInfo("name")]),
                    country_code=FunctionInfo(
                        "SLICE",
                        [
                            ReferenceInfo("phone"),
                            LiteralInfo(0, NumericType()),
                            LiteralInfo(3, NumericType()),
                            LiteralInfo(1, NumericType()),
                        ],
                    ),
                    adjusted_account_balance=FunctionInfo(
                        "IFF",
                        [
                            FunctionInfo(
                                "LET",
                                [
                                    ReferenceInfo("account_balance"),
                                    LiteralInfo(0, NumericType()),
                                ],
                            ),
                            LiteralInfo(0, NumericType()),
                            ReferenceInfo("account_balance"),
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
                "scan_customer_call_functions",
            ),
            id="scan_customer_call_functions",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [SubCollectionInfo("region")],
                    nation_name=ReferenceInfo("name"),
                    region_name=ChildReferenceExpressionInfo("name", 0),
                ),
                "nations_access_region",
            ),
            id="nations_access_region",
        ),
        pytest.param(
            (
                TableCollectionInfo("lines")
                ** CalculateInfo(
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
                                    LiteralInfo(1.0, NumericType()),
                                    ReferenceInfo("discount"),
                                ],
                            ),
                        ],
                    ),
                ),
                "lineitems_access_cust_supplier_nations",
            ),
            id="lineitems_access_cust_supplier_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** CalculateInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                ),
                "region_nations_backref",
            ),
            id="region_nations_backref",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** TableCollectionInfo("nations")
                ** TableCollectionInfo("customers")
                ** TableCollectionInfo("orders")
                ** SubCollectionInfo("lines")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("part_and_supplier")
                        ** SubCollectionInfo("supplier")
                        ** SubCollectionInfo("nation")
                        ** SubCollectionInfo("region")
                        ** CalculateInfo(
                            [], nation_name=BackReferenceExpressionInfo("name", 1)
                        )
                    ],
                    order_year=FunctionInfo(
                        "YEAR", [BackReferenceExpressionInfo("order_date", 1)]
                    ),
                    customer_region_name=BackReferenceExpressionInfo("name", 4),
                    customer_nation_name=BackReferenceExpressionInfo("name", 3),
                    supplier_region_name=ChildReferenceExpressionInfo("name", 0),
                    nation_name=ChildReferenceExpressionInfo("nation_name", 0),
                ),
                "lines_shipping_vs_customer_region",
            ),
            id="lines_shipping_vs_customer_region",
        ),
        pytest.param(
            (
                TableCollectionInfo("orders")
                ** CalculateInfo(
                    [SubCollectionInfo("lines")],
                    okey=ReferenceInfo("key"),
                    lsum=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                    ),
                ),
                "orders_sum_line_price",
            ),
            id="orders_sum_line_price",
        ),
        pytest.param(
            (
                TableCollectionInfo("customers")
                ** CalculateInfo(
                    [SubCollectionInfo("orders") ** SubCollectionInfo("lines")],
                    okey=ReferenceInfo("key"),
                    lsum=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("extended_price", 0)]
                    ),
                ),
                "customers_sum_line_price",
            ),
            id="customers_sum_line_price",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
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
                "nations_sum_line_price",
            ),
            id="nations_sum_line_price",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** CalculateInfo(
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
                "regions_sum_line_price",
            ),
            id="regions_sum_line_price",
        ),
        pytest.param(
            (
                TableCollectionInfo("orders")
                ** CalculateInfo(
                    [SubCollectionInfo("lines")],
                    okey=ReferenceInfo("key"),
                    lavg=FunctionInfo(
                        "DIV",
                        [
                            FunctionInfo(
                                "SUM",
                                [ChildReferenceExpressionInfo("extended_price", 0)],
                            ),
                            FunctionInfo(
                                "COUNT",
                                [ChildReferenceExpressionInfo("extended_price", 0)],
                            ),
                        ],
                    ),
                ),
                "orders_sum_vs_count_line_price",
            ),
            id="orders_sum_vs_count_line_price",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("customers"),
                        SubCollectionInfo("suppliers"),
                    ],
                    nation_name=ReferenceInfo("key"),
                    consumer_value=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    producer_value=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 1)]
                    ),
                ),
                "multiple_simple_aggregations_single_calc",
            ),
            id="multiple_simple_aggregations_single_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [SubCollectionInfo("customers")],
                    total_consumer_value_a=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    avg_consumer_value_a=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                )
                ** CalculateInfo(
                    [SubCollectionInfo("suppliers")],
                    nation_name_a=ReferenceInfo("key"),
                    total_supplier_value_a=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    avg_supplier_value_a=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                )
                ** CalculateInfo(
                    [SubCollectionInfo("suppliers"), SubCollectionInfo("customers")],
                    nation_name=ReferenceInfo("key"),
                    total_consumer_value=ReferenceInfo("total_consumer_value_a"),
                    total_supplier_value=ReferenceInfo("total_supplier_value_a"),
                    avg_consumer_value=ReferenceInfo("avg_consumer_value_a"),
                    avg_supplier_value=ReferenceInfo("avg_supplier_value_a"),
                    best_consumer_value=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("account_balance", 1)]
                    ),
                    best_supplier_value=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                ),
                "multiple_simple_aggregations_multiple_calcs",
            ),
            id="multiple_simple_aggregations_multiple_calcs",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [SubCollectionInfo("customers")],
                    nation_name=ReferenceInfo("key"),
                    num_customers=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                ),
                "count_single_subcollection",
            ),
            id="count_single_subcollection",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [SubCollectionInfo("customers"), SubCollectionInfo("suppliers")],
                    nation_name=ReferenceInfo("key"),
                    num_customers=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    num_suppliers=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(1)]
                    ),
                    customer_to_supplier_wealth_ratio=FunctionInfo(
                        "DIV",
                        [
                            FunctionInfo(
                                "SUM",
                                [ChildReferenceExpressionInfo("account_balance", 0)],
                            ),
                            FunctionInfo(
                                "SUM",
                                [ChildReferenceExpressionInfo("account_balance", 1)],
                            ),
                        ],
                    ),
                ),
                "count_multiple_subcollections_alongside_aggs",
            ),
            id="count_multiple_subcollections_alongside_aggs",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
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
                                            ChildReferenceExpressionInfo(
                                                "account_balance", 0
                                            ),
                                            LiteralInfo(0.0, NumericType()),
                                        ],
                                    ),
                                    LiteralInfo(0.0, NumericType()),
                                    ChildReferenceExpressionInfo("account_balance", 0),
                                ],
                            )
                        ],
                    ),
                ),
                "aggregate_on_function_call",
            ),
            id="aggregate_on_function_call",
        ),
        pytest.param(
            (
                TableCollectionInfo("orders")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("lines")
                        ** SubCollectionInfo("part_and_supplier")
                        ** CalculateInfo(
                            [],
                            ratio=FunctionInfo(
                                "DIV",
                                [
                                    BackReferenceExpressionInfo("quantity", 1),
                                    ReferenceInfo("available_quantity"),
                                ],
                            ),
                        ),
                    ],
                    order_key=ReferenceInfo("key"),
                    max_ratio=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("ratio", 0)]
                    ),
                ),
                "aggregate_mixed_levels_simple",
            ),
            id="aggregate_mixed_levels_simple",
        ),
        pytest.param(
            (
                TableCollectionInfo("orders")
                ** CalculateInfo(
                    [SubCollectionInfo("lines")],
                    total_quantity=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("quantity", 0)]
                    ),
                )
                ** SubCollectionInfo("lines")
                ** CalculateInfo(
                    [],
                    part_key=ReferenceInfo("part_key"),
                    supplier_key=ReferenceInfo("supplier_key"),
                    order_key=ReferenceInfo("order_key"),
                    order_quantity_ratio=FunctionInfo(
                        "DIV",
                        [
                            ReferenceInfo("quantity"),
                            BackReferenceExpressionInfo("total_quantity", 1),
                        ],
                    ),
                ),
                "aggregate_then_backref",
            ),
            id="aggregate_then_backref",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [],
                    a=LiteralInfo(0, NumericType()),
                    b=LiteralInfo("X", StringType()),
                    c=LiteralInfo(3.14, NumericType()),
                    d=LiteralInfo(True, BooleanType()),
                ),
                "global_calc_simple",
            ),
            id="global_calc_simple",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [],
                    a=LiteralInfo(0, NumericType()),
                    b=LiteralInfo("X", StringType()),
                )
                ** CalculateInfo(
                    [],
                    a=ReferenceInfo("a"),
                    b=ReferenceInfo("b"),
                    c=LiteralInfo(3.14, NumericType()),
                    d=LiteralInfo(True, BooleanType()),
                ),
                "global_calc_multiple",
            ),
            id="global_calc_multiple",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [TableCollectionInfo("customers")],
                    total_bal=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_bal=FunctionInfo(
                        "COUNT", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    avg_bal=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    min_bal=FunctionInfo(
                        "MIN", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    max_bal=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_cust=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                ),
                "global_aggfuncs",
            ),
            id="global_aggfuncs",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [
                        TableCollectionInfo("customers"),
                        TableCollectionInfo("suppliers"),
                        TableCollectionInfo("parts"),
                    ],
                    num_cust=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                    num_supp=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(1)]),
                    num_part=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(2)]),
                ),
                "global_aggfuncs_multiple_children",
            ),
            id="global_aggfuncs_multiple_children",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [],
                    a=LiteralInfo(28.15, NumericType()),
                    b=LiteralInfo("NICKEL", StringType()),
                )
                ** TableCollectionInfo("parts")
                ** CalculateInfo(
                    [],
                    part_name=ReferenceInfo("name"),
                    is_above_cutoff=FunctionInfo(
                        "GRT",
                        [
                            ReferenceInfo("retail_price"),
                            BackReferenceExpressionInfo("a", 1),
                        ],
                    ),
                    is_nickel=FunctionInfo(
                        "CONTAINS",
                        [
                            ReferenceInfo("part_type"),
                            BackReferenceExpressionInfo("b", 1),
                        ],
                    ),
                ),
                "global_calc_backref",
            ),
            id="global_calc_backref",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [TableCollectionInfo("parts")],
                    avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** TableCollectionInfo("parts")
                ** CalculateInfo(
                    [],
                    part_name=ReferenceInfo("name"),
                    is_above_avg=FunctionInfo(
                        "GRT",
                        [
                            ReferenceInfo("retail_price"),
                            BackReferenceExpressionInfo("avg_price", 1),
                        ],
                    ),
                ),
                "global_aggfunc_backref",
            ),
            id="global_aggfunc_backref",
        ),
        pytest.param(
            (
                TableCollectionInfo("parts")
                ** CalculateInfo(
                    [SubCollectionInfo("supply_records")],
                    name=ReferenceInfo("name"),
                    total_delta=FunctionInfo(
                        "SUM",
                        [
                            FunctionInfo(
                                "SUB",
                                [
                                    ReferenceInfo("retail_price"),
                                    ChildReferenceExpressionInfo("supply_cost", 0),
                                ],
                            )
                        ],
                    ),
                ),
                "aggregate_mixed_levels_advanced",
            ),
            id="aggregate_mixed_levels_advanced",
            marks=pytest.mark.skip("TODO"),
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("parts"),
                    "types",
                    [ChildReferenceExpressionInfo("part_type", 0)],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("parts")],
                    part_type=ReferenceInfo("part_type"),
                    num_parts=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                    avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                ),
                "agg_parts_by_type_simple",
            ),
            id="agg_parts_by_type_simple",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("orders")
                    ** CalculateInfo(
                        [],
                        year=FunctionInfo("YEAR", [ReferenceInfo("order_date")]),
                        month=FunctionInfo("MONTH", [ReferenceInfo("order_date")]),
                    ),
                    "months",
                    [
                        ChildReferenceExpressionInfo("year", 0),
                        ChildReferenceExpressionInfo("month", 0),
                    ],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("orders")],
                    year=ReferenceInfo("year"),
                    month=ReferenceInfo("month"),
                    total_orders=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                ),
                "agg_orders_by_year_month_basic",
            ),
            id="agg_orders_by_year_month_basic",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("orders")
                    ** CalculateInfo(
                        [],
                        year=FunctionInfo("YEAR", [ReferenceInfo("order_date")]),
                        month=FunctionInfo("MONTH", [ReferenceInfo("order_date")]),
                    ),
                    "months",
                    [
                        ChildReferenceExpressionInfo("year", 0),
                        ChildReferenceExpressionInfo("month", 0),
                    ],
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("orders"),
                        SubCollectionInfo("orders")
                        ** WhereInfo(
                            [
                                SubCollectionInfo("customer")
                                ** SubCollectionInfo("nation")
                                ** SubCollectionInfo("region")
                            ],
                            FunctionInfo(
                                "EQU",
                                [
                                    ChildReferenceExpressionInfo("name", 0),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        ),
                    ],
                    year=ReferenceInfo("year"),
                    month=ReferenceInfo("month"),
                    num_european_orders=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    total_orders=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(1)]
                    ),
                ),
                "agg_orders_by_year_month_vs_europe",
            ),
            id="agg_orders_by_year_month_vs_europe",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("orders")
                    ** CalculateInfo(
                        [],
                        year=FunctionInfo("YEAR", [ReferenceInfo("order_date")]),
                        month=FunctionInfo("MONTH", [ReferenceInfo("order_date")]),
                    ),
                    "months",
                    [
                        ChildReferenceExpressionInfo("year", 0),
                        ChildReferenceExpressionInfo("month", 0),
                    ],
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("orders")
                        ** WhereInfo(
                            [
                                SubCollectionInfo("customer")
                                ** SubCollectionInfo("nation")
                                ** SubCollectionInfo("region")
                            ],
                            FunctionInfo(
                                "EQU",
                                [
                                    ChildReferenceExpressionInfo("name", 0),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        ),
                    ],
                    year=ReferenceInfo("year"),
                    month=ReferenceInfo("month"),
                    num_european_orders=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                ),
                "agg_orders_by_year_month_just_europe",
            ),
            id="agg_orders_by_year_month_just_europe",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("nations")
                    ** SubCollectionInfo("customers")
                    ** SubCollectionInfo("orders")
                    ** SubCollectionInfo("lines")
                    ** SubCollectionInfo("part_and_supplier")
                    ** SubCollectionInfo("supplier")
                    ** SubCollectionInfo("nation")
                    ** CalculateInfo(
                        [],
                        year=FunctionInfo(
                            "YEAR", [BackReferenceExpressionInfo("order_date", 4)]
                        ),
                        customer_nation=BackReferenceExpressionInfo("name", 6),
                        supplier_nation=ReferenceInfo("name"),
                        value=BackReferenceExpressionInfo("extended_price", 3),
                    ),
                    "combos",
                    [
                        ChildReferenceExpressionInfo("year", 0),
                        ChildReferenceExpressionInfo("customer_nation", 0),
                        ChildReferenceExpressionInfo("supplier_nation", 0),
                    ],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("nation")],
                    year=ReferenceInfo("year"),
                    customer_nation=ReferenceInfo("customer_nation"),
                    supplier_nation=ReferenceInfo("supplier_nation"),
                    num_occurrences=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    total_value=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("value", 0)]
                    ),
                ),
                "count_cust_supplier_nation_combos",
            ),
            id="count_cust_supplier_nation_combos",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [TableCollectionInfo("parts")],
                    total_num_parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    global_avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** PartitionInfo(
                    TableCollectionInfo("parts"),
                    "types",
                    [ChildReferenceExpressionInfo("part_type", 0)],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("parts")],
                    part_type=ReferenceInfo("part_type"),
                    percentage_of_parts=FunctionInfo(
                        "DIV",
                        [
                            FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                            BackReferenceExpressionInfo("total_num_parts", 1),
                        ],
                    ),
                    avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "GEQ",
                        [
                            ReferenceInfo("avg_price"),
                            BackReferenceExpressionInfo("global_avg_price", 1),
                        ],
                    ),
                ),
                "agg_parts_by_type_backref_global",
            ),
            id="agg_parts_by_type_backref_global",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("parts"),
                    "types",
                    [ChildReferenceExpressionInfo("part_type", 0)],
                )
                ** WhereInfo(
                    [SubCollectionInfo("parts")],
                    FunctionInfo(
                        "GRT",
                        [
                            FunctionInfo(
                                "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                            ),
                            LiteralInfo(27.5, NumericType()),
                        ],
                    ),
                )
                ** SubCollectionInfo("parts")
                ** CalculateInfo(
                    [],
                    part_name=ReferenceInfo("name"),
                    part_type=ReferenceInfo("part_type"),
                    retail_price=ReferenceInfo("retail_price"),
                ),
                "access_partition_child_after_filter",
            ),
            id="access_partition_child_after_filter",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("parts"),
                    "types",
                    [ChildReferenceExpressionInfo("part_type", 0)],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("parts")],
                    avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** SubCollectionInfo("parts")
                ** CalculateInfo(
                    [],
                    part_name=ReferenceInfo("name"),
                    part_type=ReferenceInfo("part_type"),
                    retail_price_versus_avg=FunctionInfo(
                        "SUB",
                        [
                            ReferenceInfo("retail_price"),
                            BackReferenceExpressionInfo("avg_price", 1),
                        ],
                    ),
                ),
                "access_partition_child_backref_calc",
            ),
            id="access_partition_child_backref_calc",
        ),
        pytest.param(
            (
                PartitionInfo(
                    TableCollectionInfo("parts"),
                    "types",
                    [ChildReferenceExpressionInfo("part_type", 0)],
                )
                ** CalculateInfo(
                    [SubCollectionInfo("parts")],
                    avg_price=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** SubCollectionInfo("parts")
                ** CalculateInfo(
                    [],
                    part_name=ReferenceInfo("name"),
                    part_type=ReferenceInfo("part_type"),
                    retail_price=ReferenceInfo("retail_price"),
                )
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "LET",
                        [
                            ReferenceInfo("retail_price"),
                            BackReferenceExpressionInfo("avg_price", 1),
                        ],
                    ),
                ),
                "access_partition_child_filter_backref_filter",
            ),
            id="access_partition_child_filter_backref_filter",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "EQU",
                        [ReferenceInfo("name"), LiteralInfo("ASIA", StringType())],
                    ),
                )
                ** SubCollectionInfo("nations"),
                "join_asia_region_nations",
            ),
            id="join_asia_region_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
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
                "asian_nations",
            ),
            id="asian_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("lines")
                ** WhereInfo(
                    [
                        SubCollectionInfo("part_and_supplier")
                        ** SubCollectionInfo("supplier")
                        ** SubCollectionInfo("nation"),
                        SubCollectionInfo("part_and_supplier")
                        ** SubCollectionInfo("part"),
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
                )
                ** CalculateInfo(
                    [],
                    order_key=ReferenceInfo("order_key"),
                    ship_date=ReferenceInfo("ship_date"),
                    extended_price=ReferenceInfo("extended_price"),
                ),
                "lines_german_supplier_economy_part",
            ),
            id="lines_german_supplier_economy_part",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** WhereInfo(
                    [],
                    FunctionInfo(
                        "CONTAINS",
                        [ReferenceInfo("name"), BackReferenceExpressionInfo("name", 1)],
                    ),
                ),
                "nation_name_contains_region_name",
            ),
            id="nation_name_contains_region_name",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
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
                ** CalculateInfo(
                    [],
                    rname=BackReferenceExpressionInfo("name", 4),
                    price=ReferenceInfo("extended_price"),
                ),
                "lineitem_regional_shipments",
            ),
            id="lineitem_regional_shipments",
        ),
        pytest.param(
            (
                TableCollectionInfo("lines")
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
                ** CalculateInfo(
                    [
                        SubCollectionInfo("order")
                        ** SubCollectionInfo("customer")
                        ** SubCollectionInfo("nation")
                        ** SubCollectionInfo("region")
                    ],
                    rname=ChildReferenceExpressionInfo("name", 0),
                    price=ReferenceInfo("extended_price"),
                ),
                "lineitem_regional_shipments2",
            ),
            id="lineitem_regional_shipments2",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
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
                "lineitem_regional_shipments3",
            ),
            id="lineitem_regional_shipments3",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("suppliers")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "GRT",
                                [
                                    ReferenceInfo("account_balance"),
                                    LiteralInfo(0.0, NumericType()),
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
                "num_positive_accounts_per_nation",
            ),
            id="num_positive_accounts_per_nation",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo([], name=ReferenceInfo("name"))
                ** WhereInfo(
                    [
                        SubCollectionInfo("suppliers")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "GRT",
                                [
                                    ReferenceInfo("account_balance"),
                                    LiteralInfo(0.0, NumericType()),
                                ],
                            ),
                        ),
                        SubCollectionInfo("suppliers"),
                    ],
                    FunctionInfo(
                        "GRT",
                        [
                            FunctionInfo(
                                "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                            ),
                            FunctionInfo(
                                "MUL",
                                [
                                    LiteralInfo(0.5, NumericType()),
                                    FunctionInfo(
                                        "COUNT",
                                        [ChildReferenceExpressionInfo("key", 1)],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                "mostly_positive_accounts_per_nation1",
            ),
            id="mostly_positive_accounts_per_nation1",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("suppliers")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "GRT",
                                [
                                    ReferenceInfo("account_balance"),
                                    LiteralInfo(0.0, NumericType()),
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
                                    LiteralInfo(0.0, NumericType()),
                                ],
                            ),
                        ),
                        SubCollectionInfo("suppliers"),
                    ],
                    FunctionInfo(
                        "GRT",
                        [
                            FunctionInfo(
                                "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                            ),
                            FunctionInfo(
                                "MUL",
                                [
                                    LiteralInfo(0.5, NumericType()),
                                    FunctionInfo(
                                        "COUNT",
                                        [ChildReferenceExpressionInfo("key", 1)],
                                    ),
                                ],
                            ),
                        ],
                    ),
                ),
                "mostly_positive_accounts_per_nation2",
            ),
            id="mostly_positive_accounts_per_nation2",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("suppliers")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "GRT",
                                [
                                    ReferenceInfo("account_balance"),
                                    LiteralInfo(0.0, NumericType()),
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
                                    LiteralInfo(0.5, NumericType()),
                                    ReferenceInfo("total_suppliers"),
                                ],
                            ),
                        ],
                    ),
                ),
                "mostly_positive_accounts_per_nation3",
            ),
            id="mostly_positive_accounts_per_nation3",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** TopKInfo([], 2, (ReferenceInfo("name"), True, True)),
                "simple_topk",
            ),
            id="simple_topk",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** TopKInfo([], 10, (ReferenceInfo("name"), True, True))
                ** CalculateInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                ),
                "join_topk",
            ),
            id="join_topk",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, True)),
                "simple_order_by",
            ),
            id="simple_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** OrderInfo([], (ReferenceInfo("name"), False, True))
                ** CalculateInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                ),
                "join_order_by",
            ),
            id="join_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** OrderInfo([], (ReferenceInfo("name"), True, True))
                ** CalculateInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                )
                ** OrderInfo([], (ReferenceInfo("region_name"), False, True)),
                "replace_order_by",
            ),
            id="replace_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, True))
                ** TopKInfo([], 10, (ReferenceInfo("name"), True, True)),
                "topk_order_by",
            ),
            id="topk_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, True))
                ** TopKInfo([], 10, (ReferenceInfo("name"), True, True))
                ** CalculateInfo(
                    [],
                    region_name=ReferenceInfo("name"),
                    name_length=FunctionInfo("LENGTH", [ReferenceInfo("name")]),
                ),
                "topk_order_by_calc",
            ),
            id="topk_order_by_calc",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, True))
                ** OrderInfo([], (ReferenceInfo("name"), False, False))
                ** TopKInfo([], 10, (ReferenceInfo("name"), False, False)),
                "topk_replace_order_by",
            ),
            # Note: This tests is less useful because the rewrite has already
            # occurred for TopK.
            id="topk_replace_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, False))
                ** TopKInfo([], 10, (ReferenceInfo("name"), True, False))
                ** OrderInfo([], (ReferenceInfo("name"), False, False)),
                "topk_root_different_order_by",
            ),
            id="topk_root_different_order_by",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo(
                    [], (FunctionInfo("LENGTH", [ReferenceInfo("name")]), True, False)
                )
                ** TopKInfo(
                    [],
                    10,
                    (FunctionInfo("LENGTH", [ReferenceInfo("name")]), True, False),
                ),
                "order_by_expression",
            ),
            id="order_by_expression",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** OrderInfo([], (ReferenceInfo("name"), True, False))
                ** SubCollectionInfo("nations"),
                "order_by_before_join",
            ),
            # Note: This behavior may change in the future.
            id="order_by_before_join",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** OrderInfo([], (ReferenceInfo("name"), True, True))
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
                "ordered_asian_nations",
            ),
            id="ordered_asian_nations",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** OrderInfo(
                    [SubCollectionInfo("region")],
                    (ReferenceInfo("name"), True, True),
                    (ChildReferenceExpressionInfo("name", 0), True, True),
                ),
                "nations_region_order_by_name",
            ),
            id="nations_region_order_by_name",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("suppliers")
                        ** TopKInfo(
                            [], 100, (ReferenceInfo("account_balance"), True, True)
                        ),
                    ],
                    name=ReferenceInfo("name"),
                    n_top_suppliers=FunctionInfo(
                        "COUNT", [ChildReferenceExpressionInfo("key", 0)]
                    ),
                ),
                "count_at_most_100_suppliers_per_nation",
            ),
            id="count_at_most_100_suppliers_per_nation",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** OrderInfo(
                    [SubCollectionInfo("suppliers")],
                    (
                        FunctionInfo("COUNT", [ChildReferenceExpressionInfo("key", 0)]),
                        True,
                        True,
                    ),
                ),
                "nations_order_by_num_suppliers",
            ),
            id="nations_order_by_num_suppliers",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** TopKInfo(
                    [SubCollectionInfo("suppliers")],
                    5,
                    (
                        FunctionInfo("COUNT", [ChildReferenceExpressionInfo("key", 0)]),
                        True,
                        True,
                    ),
                ),
                "top_5_nations_by_num_supplierss",
            ),
            id="top_5_nations_by_num_suppliers",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** TopKInfo(
                    [SubCollectionInfo("suppliers")],
                    5,
                    (
                        FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                        True,
                        True,
                    ),
                )
                ** CalculateInfo(
                    [SubCollectionInfo("suppliers")],
                    name=ReferenceInfo("name"),
                    total_bal=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                ),
                "top_5_nations_balance_by_num_suppliers",
            ),
            id="top_5_nations_balance_by_num_suppliers",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** CalculateInfo(
                    [],
                    region_name=BackReferenceExpressionInfo("name", 1),
                    nation_name=ReferenceInfo("name"),
                )
                ** OrderInfo([], (BackReferenceExpressionInfo("name", 1), False, True)),
                "join_order_by_back_reference",
            ),
            id="join_order_by_back_reference",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** CalculateInfo(
                    [],
                    nation_name=ReferenceInfo("name"),
                )
                ** OrderInfo([], (BackReferenceExpressionInfo("name", 1), False, True)),
                "join_order_by_pruned_back_reference",
            ),
            id="join_order_by_pruned_back_reference",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [],
                    ordering_0=ReferenceInfo("name"),
                    ordering_1=ReferenceInfo("key"),
                    ordering_2=ReferenceInfo("comment"),
                )
                ** OrderInfo(
                    [],
                    (FunctionInfo("LOWER", [ReferenceInfo("name")]), True, True),
                    (FunctionInfo("ABS", [ReferenceInfo("key")]), False, True),
                    (FunctionInfo("LENGTH", [ReferenceInfo("comment")]), True, False),
                )
                ** CalculateInfo(
                    [],
                    ordering_0=ReferenceInfo("ordering_2"),
                    ordering_1=ReferenceInfo("ordering_0"),
                    ordering_2=ReferenceInfo("ordering_1"),
                    ordering_3=ReferenceInfo("ordering_2"),
                    ordering_4=ReferenceInfo("ordering_1"),
                    ordering_5=ReferenceInfo("ordering_0"),
                    ordering_6=FunctionInfo("LOWER", [ReferenceInfo("name")]),
                    ordering_7=FunctionInfo("ABS", [ReferenceInfo("key")]),
                    ordering_8=FunctionInfo("LENGTH", [ReferenceInfo("comment")]),
                ),
                "ordering_name_overload",
            ),
            id="ordering_name_overload",
        ),
        pytest.param(
            (
                TableCollectionInfo("customers")
                ** WhereInfo(
                    [SubCollectionInfo("orders")],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [],
                    name=ReferenceInfo("name"),
                ),
                "simple_semi_1",
            ),
            id="simple_semi_1",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "LET",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [],
                    name=ReferenceInfo("name"),
                ),
                "simple_semi_2",
            ),
            id="simple_semi_2",
        ),
        pytest.param(
            (
                TableCollectionInfo("customers")
                ** WhereInfo(
                    [SubCollectionInfo("orders")],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [],
                    name=ReferenceInfo("name"),
                ),
                "simple_anti_1",
            ),
            id="simple_anti_1",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "LET",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [],
                    name=ReferenceInfo("name"),
                ),
                "simple_anti_2",
            ),
            id="simple_anti_2",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** WhereInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    region_name=ChildReferenceExpressionInfo("name", 0),
                ),
                "semi_singular",
            ),
            id="semi_singular",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    nation_name=ReferenceInfo("name"),
                    region_name=ChildReferenceExpressionInfo("name", 0),
                )
                ** WhereInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                ),
                "singular_semi",
            ),
            id="singular_semi",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    num_10parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    avg_price_of_10parts=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                    sum_price_of_10parts=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                ),
                "semi_aggregate",
            ),
            id="semi_aggregate",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    num_10parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    avg_price_of_10parts=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                    sum_price_of_10parts=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                ),
                "aggregate_semi",
            ),
            id="aggregate_semi",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** WhereInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    region_name=ChildReferenceExpressionInfo("name", 0),
                ),
                "anti_singular",
            ),
            id="anti_singular",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    nation_name=ReferenceInfo("name"),
                    region_name=ChildReferenceExpressionInfo("name", 0),
                )
                ** WhereInfo(
                    [
                        SubCollectionInfo("region")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "NEQ",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ASIA", StringType()),
                                ],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                ),
                "singular_anti",
            ),
            id="singular_anti",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    num_10parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    avg_price_of_10parts=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                    sum_price_of_10parts=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                ),
                "anti_aggregate",
            ),
            id="anti_aggregate",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    num_10parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    avg_price_of_10parts=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                    sum_price_of_10parts=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                )
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                ),
                "aggregate_anti",
            ),
            id="aggregate_anti",
        ),
        pytest.param(
            (
                TableCollectionInfo("parts")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("supplier")
                        ** SubCollectionInfo("nation")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("GERMANY", StringType()),
                                ],
                            ),
                        ),
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("supplier")
                        ** SubCollectionInfo("nation")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("FRANCE", StringType()),
                                ],
                            ),
                        ),
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("supplier")
                        ** SubCollectionInfo("nation")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [
                                    ReferenceInfo("name"),
                                    LiteralInfo("ARGENTINA", StringType()),
                                ],
                            ),
                        ),
                    ],
                    FunctionInfo(
                        "BAN",
                        [
                            FunctionInfo("HAS", [ChildReferenceCollectionInfo(0)]),
                            FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(1)]),
                            FunctionInfo("HAS", [ChildReferenceCollectionInfo(2)]),
                        ],
                    ),
                )
                ** CalculateInfo([], name=ReferenceInfo("name")),
                "multiple_has_hasnot",
            ),
            id="multiple_has_hasnot",
        ),
        pytest.param(
            (
                TableCollectionInfo("customers")
                ** CalculateInfo(
                    [],
                    name=ReferenceInfo("name"),
                    cust_rank=WindowInfo(
                        "RANKING",
                        (ReferenceInfo("account_balance"), False, True),
                    ),
                ),
                "rank_customers",
            ),
            id="rank_customers",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** SubCollectionInfo("customers")
                ** CalculateInfo(
                    [],
                    nation_name=BackReferenceExpressionInfo("name", 1),
                    name=ReferenceInfo("name"),
                    cust_rank=WindowInfo(
                        "RANKING",
                        (ReferenceInfo("account_balance"), False, True),
                        levels=1,
                        allow_ties=True,
                    ),
                ),
                "rank_customers_per_nation",
            ),
            id="rank_customers_per_nation",
        ),
        pytest.param(
            (
                TableCollectionInfo("regions")
                ** SubCollectionInfo("nations")
                ** SubCollectionInfo("customers")
                ** CalculateInfo(
                    [],
                    nation_name=BackReferenceExpressionInfo("name", 1),
                    name=ReferenceInfo("name"),
                    cust_rank=WindowInfo(
                        "RANKING",
                        (ReferenceInfo("account_balance"), False, True),
                        levels=2,
                        allow_ties=True,
                        dense=True,
                    ),
                ),
                "rank_customers_per_region",
            ),
            id="rank_customers_per_region",
        ),
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [
                        SubCollectionInfo("customers")
                        ** CalculateInfo(
                            [],
                            cust_rank=WindowInfo(
                                "RANKING",
                                (ReferenceInfo("account_balance"), False, True),
                                allow_ties=True,
                            ),
                        )
                    ],
                    nation_name=ReferenceInfo("name"),
                    highest_rank=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("cust_rank", 0)]
                    ),
                ),
                "agg_max_ranking",
            ),
            id="agg_max_ranking",
        ),
    ],
)
def relational_test_data(request) -> tuple[CollectionTestInfo, str]:
    """
    Input data for `test_ast_to_relational`. Parameters are the info to build
    the input QDAG nodes, and the name of the file containing the expected
    output string after converting to a relational tree.
    """
    return request.param


def test_ast_to_relational(
    relational_test_data: tuple[CollectionTestInfo, str],
    tpch_node_builder: AstNodeBuilder,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Tests whether the QDAG nodes are correctly translated into Relational nodes
    with the expected string representation.
    """
    calc_pipeline, file_name = relational_test_data
    file_path: str = get_plan_test_filename(file_name)
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    relational = convert_ast_to_relational(collection, None, default_config)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert relational.to_tree_string() == expected_relational_string.strip(), (
            "Mismatch between full string representation of output Relational node versus expected string"
        )


@pytest.fixture(
    params=[
        pytest.param(
            (
                TableCollectionInfo("nations")
                ** CalculateInfo(
                    [SubCollectionInfo("customers")],
                    nation_name=ReferenceInfo("name"),
                    total_bal=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_bal=FunctionInfo(
                        "COUNT", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    avg_bal=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    min_bal=FunctionInfo(
                        "MIN", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    max_bal=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_cust=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                ),
                "various_aggfuncs_simple",
            ),
            id="various_aggfuncs_simple",
        ),
        pytest.param(
            (
                CalculateInfo(
                    [TableCollectionInfo("customers")],
                    total_bal=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_bal=FunctionInfo(
                        "COUNT", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    avg_bal=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    min_bal=FunctionInfo(
                        "MIN", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    max_bal=FunctionInfo(
                        "MAX", [ChildReferenceExpressionInfo("account_balance", 0)]
                    ),
                    num_cust=FunctionInfo("COUNT", [ChildReferenceCollectionInfo(0)]),
                ),
                "various_aggfuncs_global",
            ),
            id="various_aggfuncs_global",
        ),
        pytest.param(
            (
                TableCollectionInfo("suppliers")
                ** WhereInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    FunctionInfo("HASNOT", [ChildReferenceCollectionInfo(0)]),
                )
                ** CalculateInfo(
                    [
                        SubCollectionInfo("supply_records")
                        ** SubCollectionInfo("part")
                        ** WhereInfo(
                            [],
                            FunctionInfo(
                                "EQU",
                                [ReferenceInfo("size"), LiteralInfo(10, NumericType())],
                            ),
                        )
                    ],
                    name=ReferenceInfo("name"),
                    num_10parts=FunctionInfo(
                        "COUNT", [ChildReferenceCollectionInfo(0)]
                    ),
                    avg_price_of_10parts=FunctionInfo(
                        "AVG", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                    sum_price_of_10parts=FunctionInfo(
                        "SUM", [ChildReferenceExpressionInfo("retail_price", 0)]
                    ),
                ),
                "anti_aggregate_alternate",
            ),
            id="anti_aggregate_alternate",
        ),
    ],
)
def relational_alternative_config_test_data(request) -> tuple[CollectionTestInfo, str]:
    """
    Input data for `test_ast_to_relational_alternative_aggregation_configs`.
    Parameters are the info to build the input QDAG nodes, and the name of the
    file containing the expected output string after converting to a relational
    tree.
    """
    return request.param


def test_ast_to_relational_alternative_aggregation_configs(
    relational_alternative_config_test_data: tuple[CollectionTestInfo, str],
    tpch_node_builder: AstNodeBuilder,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
) -> None:
    """
    Same as `test_ast_to_relational` but with various alternative aggregation
    configs:
    - `SUM` defaulting to zero is disabled.
    - `COUNT` defaulting to zero is disabled.
    """
    calc_pipeline, file_name = relational_alternative_config_test_data
    file_path: str = get_plan_test_filename(file_name)
    default_config.sum_default_zero = False
    default_config.avg_default_zero = True
    collection: PyDoughCollectionQDAG = calc_pipeline.build(tpch_node_builder)
    relational = convert_ast_to_relational(collection, None, default_config)
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert relational.to_tree_string() == expected_relational_string.strip(), (
            "Mismatch between full string representation of output Relational node versus expected string"
        )
