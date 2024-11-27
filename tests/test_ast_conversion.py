"""
TODO: add file-level docstring.
"""

import pytest
from test_utils import (
    CalcInfo,
    ChildReferenceExpressionInfo,
    CollectionTestInfo,
    FunctionInfo,
    LiteralInfo,
    ReferenceInfo,
    SubCollectionInfo,
    TableCollectionInfo,
    WhereInfo,
)

from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.pydough_ast import AstNodeBuilder, PyDoughCollectionAST
from pydough.types import (
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
 PROJECT(columns={'magic_word': foo:StringType(), 'region_name': name})
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
  PROJECT(columns={'key': key, 'name_0': foo:StringType()})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey})
""",
            id="scan_calc_calc",
        ),
        pytest.param(
            TableCollectionInfo("Regions") ** SubCollectionInfo("nations"),
            """
ROOT(columns=[('key', key), ('name', name), ('region_key', region_key), ('comment', comment)], orderings=[])
 PROJECT(columns={'comment': comment_1, 'key': key_1, 'name': name_1, 'region_key': region_key})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'comment_1': t1.comment, 'key_1': t1.key, 'name_1': t1.name, 'region_key': t1.region_key})
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
 JOIN(conditions=[t0.key_1 == t1.nation_key], types=['inner'], columns={'acctbal': t1.acctbal, 'address': t1.address, 'comment': t1.comment, 'key': t1.key, 'mktsegment': t1.mktsegment, 'name': t1.name, 'nation_key': t1.nation_key, 'phone': t1.phone})
  JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_1': t1.key})
   SCAN(table=tpch.REGION, columns={'key': r_regionkey})
   SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
  SCAN(table=tpch.CUSTOMER, columns={'acctbal': c_acctbal, 'address': c_address, 'comment': c_comment, 'key': c_custkey, 'mktsegment': c_mktsegment, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
""",
            id="join_region_nations_customers",
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
ROOT(columns=[('key_0', key_0), ('name', name), ('phone', phone), ('mktsegment', mktsegment)], orderings=[])
 PROJECT(columns={'key_0': -3:Int64Type(), 'mktsegment': mktsegment, 'name': name_1, 'phone': phone})
  JOIN(conditions=[t0.key == t1.nation_key], types=['inner'], columns={'mktsegment': t1.mktsegment, 'name_1': t1.name, 'phone': t1.phone})
   PROJECT(columns={'key': key_1})
    JOIN(conditions=[t0.key == t1.region_key], types=['inner'], columns={'key_1': t1.key})
     SCAN(table=tpch.REGION, columns={'key': r_regionkey})
     SCAN(table=tpch.NATION, columns={'key': n_nationkey, 'region_key': n_regionkey})
   SCAN(table=tpch.CUSTOMER, columns={'mktsegment': c_mktsegment, 'name': c_name, 'nation_key': c_nationkey, 'phone': c_phone})
""",
            id="join_regions_nations_calc_override",
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
\
""",
            id="join_asia_region_nations",
            marks=pytest.mark.skip("TODO"),
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
            """\
\
""",
            id="asian_regions",
            marks=pytest.mark.skip("TODO"),
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
    ],
)
def test_ast_to_relational(
    calc_pipeline: CollectionTestInfo,
    expected_relational_string: str,
    tpch_node_builder: AstNodeBuilder,
):
    """
    Tests whether the AST nodes are correctly translated into Relational nodes
    with the expected string representation.
    """
    collection: PyDoughCollectionAST = calc_pipeline.build(tpch_node_builder)
    relational = convert_ast_to_relational(collection)
    assert (
        relational.to_tree_string() == expected_relational_string.strip()
    ), "Mismatch between full string representation of output Relational node versus expected string"
