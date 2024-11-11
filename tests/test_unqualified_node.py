"""
TODO: add file-level docstring.
"""

import datetime

import pytest
from test_utils import (
    graph_fetcher,
)

from pydough.unqualified import (
    BACK,
    CONTAINS,
    COUNT,
    ENDSWITH,
    LOWER,
    PARTITION,
    STARTSWITH,
    SUM,
    UPPER,
    UnqualifiedBack,
    UnqualifiedNode,
    UnqualifiedRoot,
)


@pytest.fixture
def global_ctx() -> dict[str, object]:
    """
    A fresh global variable context including the various global variables
    accessible within one of the unqualified AST tests, including PyDough
    operator bindings and certain modules.
    """
    extra_vars: dict[str, object] = {}
    for obj in [
        BACK,
        PARTITION,
        SUM,
        COUNT,
        LOWER,
        UPPER,
        STARTSWITH,
        ENDSWITH,
        CONTAINS,
        datetime,
    ]:
        if hasattr(obj, "__name__"):
            extra_vars[obj.__name__] = obj
    return extra_vars


@pytest.mark.parametrize(
    "pydough_str, answer_str",
    [
        pytest.param(
            "answer = ROOT",
            "TPCH",
            id="root",
        ),
        pytest.param(
            "answer = ROOT.Parts",
            "TPCH.Parts",
            id="access_collection",
        ),
        pytest.param(
            "answer = ROOT.Regions.nations",
            "TPCH.Regions.nations",
            id="access_subcollection",
        ),
        pytest.param(
            "answer = ROOT.Regions.name",
            "TPCH.Regions.name",
            id="access_property",
        ),
        pytest.param(
            "answer = ROOT.Regions(region_name=ROOT.name, region_key=ROOT.key)",
            "TPCH.Regions(region_name=TPCH.name, region_key=TPCH.key)",
            id="simple_calc",
        ),
        pytest.param(
            "answer = ROOT.Nations(nation_name=UPPER(ROOT.name), total_balance=SUM(ROOT.customers.acct_bal))",
            "TPCH.Nations(nation_name=UPPER(TPCH.name), total_balance=SUM(TPCH.customers.acct_bal))",
            id="calc_with_functions",
        ),
        pytest.param(
            "answer = ROOT.x + 1",
            "(TPCH.x + 1:Int64Type())",
            id="arithmetic_01",
        ),
        pytest.param(
            "answer = 2 + ROOT.x",
            "(2:Int64Type() + TPCH.x)",
            id="arithmetic_02",
        ),
        pytest.param(
            "answer = ((1.5 * ROOT.x) - 1)",
            "((1.5:Float64Type() * TPCH.x) - 1:Int64Type())",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = ((1.5 * ROOT.x) - 1)",
            "((1.5:Float64Type() * TPCH.x) - 1:Int64Type())",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = (STARTSWITH(ROOT.x, 'hello') | ENDSWITH(ROOT.x, 'world')) & CONTAINS(ROOT.x, ' ')",
            "((STARTSWITH(TPCH.x, 'hello') | ENDSWITH(TPCH.x, 'world')) & CONTAINS(TPCH.x, ' '))",
            id="arithmetic_04",
        ),
        pytest.param(
            "answer = (1 / ROOT.x) ** 2 - ROOT.y",
            "(((1:Int64Type() / TPCH.x) ** 2:Int64Type()) - TPCH.y)",
            id="arithmetic_05",
        ),
        pytest.param(
            "answer = -(ROOT.x % 10) / 3.1415",
            "((0:Int64Type() - (TPCH.x % 10:Int64Type())) / 3.1415:Float64Type())",
            id="arithmetic_06",
        ),
        pytest.param(
            "answer = (+ROOT.x < -ROOT.y) ^ (ROOT.y == ROOT.z)",
            "((TPCH.x < (0:Int64Type() - TPCH.y)) ^ (TPCH.y == TPCH.z))",
            id="arithmetic_07",
        ),
        pytest.param(
            "answer = 'Hello' != ROOT.word",
            "(TPCH.word != 'Hello':StringType())",
            id="arithmetic_08",
        ),
        pytest.param(
            "answer = ROOT.order_date >= datetime.date(2020, 1, 1)",
            "(TPCH.order_date >= datetime.date(2020, 1, 1):DateType())",
            id="arithmetic_09",
        ),
        pytest.param(
            "answer = True & (0 >= ROOT.x)",
            "(True:BooleanType() & (TPCH.x <= 0:Int64Type()))",
            id="arithmetic_10",
        ),
        pytest.param(
            "answer = (ROOT.x == 42) | (45 == ROOT.x) | ((ROOT.x < 16) & (ROOT.x != 0)) | ((100 < ROOT.x) ^ (0 == ROOT.y))",
            "((((TPCH.x == 42:Int64Type()) | (TPCH.x == 45:Int64Type())) | ((TPCH.x < 16:Int64Type()) & (TPCH.x != 0:Int64Type()))) | ((TPCH.x > 100:Int64Type()) ^ (TPCH.y == 0:Int64Type())))",
            id="arithmetic_11",
        ),
        pytest.param(
            "answer = False ^ 100 % 2.718281828 ** ROOT.x",
            "(False:BooleanType() ^ (100:Int64Type() % (2.718281828:Float64Type() ** TPCH.x)))",
            id="arithmetic_12",
        ),
        pytest.param(
            "answer = ROOT.Parts(part_name=LOWER(ROOT.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            "TPCH.Parts(part_name=LOWER(TPCH.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            id="multi_calc_with_back",
        ),
        pytest.param(
            """\
x = ROOT.Parts(part_name=LOWER(ROOT.name))
y = x.WHERE(STARTSWITH(ROOT.part_name, 'a'))
answer = y.ORDER_BY(ROOT.retail_price.DESC())\
""",
            "TPCH.Parts(part_name=LOWER(TPCH.name)).WHERE(STARTSWITH(TPCH.part_name, 'a')).ORDER_BY(TPCH.retail_price.DESC(na_pos='last'))",
            id="calc_with_where_order",
        ),
        pytest.param(
            "answer = ROOT.TOP_K(10, by=(1 / (ROOT.retail_price - 30.0)).ASC(na_pos='first'))",
            "TPCH.TOP_K(10, by=((1:Int64Type() / (TPCH.retail_price - 30.0:Float64Type())).ASC(na_pos='first')))",
            id="topk_single",
        ),
        pytest.param(
            "answer = ROOT.TOP_K(10, by=(ROOT.size.DESC(), ROOT.type.DESC()))",
            "TPCH.TOP_K(10, by=(TPCH.size.DESC(na_pos='last'), TPCH.type.DESC(na_pos='last')))",
            id="topk_multiple",
        ),
        pytest.param(
            """\
x = ROOT.Parts.ORDER_BY(ROOT.retail_price.ASC(na_pos='last'))
answer = x.TOP_K(100)\
""",
            "TPCH.Parts.ORDER_BY(TPCH.retail_price.ASC(na_pos='last')).TOP_K(100)",
            id="order_topk_empty",
        ),
        pytest.param(
            "answer = PARTITION(ROOT.Parts, name='parts', by=ROOT.type)(type=ROOT.type, total_price=SUM(ROOT.data.retail_price), n_orders=COUNT(ROOT.data.lines))",
            "PARTITION(TPCH.Parts, name='parts', by=(TPCH.type))(type=TPCH.type, total_price=SUM(TPCH.data.retail_price), n_orders=COUNT(TPCH.data.lines))",
            id="partition",
        ),
    ],
)
def test_unqualified_to_string(
    pydough_str: str,
    answer_str: str,
    global_ctx: dict[str, object],
    get_sample_graph: graph_fetcher,
):
    """
    Tests that strings representing the setup of PyDough unqualified objects
    (with unknown variables already pre-pended with `ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    root: UnqualifiedNode = UnqualifiedRoot(get_sample_graph("TPCH"))
    env = {"ROOT": root, "BACK": UnqualifiedBack, "datetime": datetime}
    exec(pydough_str, global_ctx, env)
    assert "answer" in env, "Expected `pydough_str` to define a variable `answer`."
    answer = env["answer"]
    assert isinstance(
        answer, UnqualifiedNode
    ), "Expected `pydough_str` to define `answer` as an UnqualifiedNode."
    assert (
        repr(answer) == answer_str
    ), "Mismatch between string representation of `answer` and expected value."
