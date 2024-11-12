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
            "answer = _ROOT.Parts",
            "TPCH.Parts",
            id="access_collection",
        ),
        pytest.param(
            "answer = _ROOT.Regions.nations",
            "TPCH.Regions.nations",
            id="access_subcollection",
        ),
        pytest.param(
            "answer = _ROOT.Regions.name",
            "TPCH.Regions.name",
            id="access_property",
        ),
        pytest.param(
            "answer = _ROOT.Regions(region_name=_ROOT.name, region_key=_ROOT.key)",
            "TPCH.Regions(region_name=TPCH.name, region_key=TPCH.key)",
            id="simple_calc",
        ),
        pytest.param(
            "answer = _ROOT.Nations(nation_name=UPPER(_ROOT.name), total_balance=SUM(_ROOT.customers.acct_bal))",
            "TPCH.Nations(nation_name=UPPER(TPCH.name), total_balance=SUM(TPCH.customers.acct_bal))",
            id="calc_with_functions",
        ),
        pytest.param(
            "answer = _ROOT.x + 1",
            "(TPCH.x + 1:Int64Type())",
            id="arithmetic_01",
        ),
        pytest.param(
            "answer = 2 + _ROOT.x",
            "(2:Int64Type() + TPCH.x)",
            id="arithmetic_02",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5:Float64Type() * TPCH.x) - 1:Int64Type())",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5:Float64Type() * TPCH.x) - 1:Int64Type())",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = (STARTSWITH(_ROOT.x, 'hello') | ENDSWITH(_ROOT.x, 'world')) & CONTAINS(_ROOT.x, ' ')",
            "((STARTSWITH(TPCH.x, 'hello':StringType()) | ENDSWITH(TPCH.x, 'world':StringType())) & CONTAINS(TPCH.x, ' ':StringType()))",
            id="arithmetic_04",
        ),
        pytest.param(
            "answer = (1 / _ROOT.x) ** 2 - _ROOT.y",
            "(((1:Int64Type() / TPCH.x) ** 2:Int64Type()) - TPCH.y)",
            id="arithmetic_05",
        ),
        pytest.param(
            "answer = -(_ROOT.x % 10) / 3.1415",
            "((0:Int64Type() - (TPCH.x % 10:Int64Type())) / 3.1415:Float64Type())",
            id="arithmetic_06",
        ),
        pytest.param(
            "answer = (+_ROOT.x < -_ROOT.y) ^ (_ROOT.y == _ROOT.z)",
            "((TPCH.x < (0:Int64Type() - TPCH.y)) ^ (TPCH.y == TPCH.z))",
            id="arithmetic_07",
        ),
        pytest.param(
            "answer = 'Hello' != _ROOT.word",
            "(TPCH.word != 'Hello':StringType())",
            id="arithmetic_08",
        ),
        pytest.param(
            "answer = _ROOT.order_date >= datetime.date(2020, 1, 1)",
            "(TPCH.order_date >= datetime.date(2020, 1, 1):DateType())",
            id="arithmetic_09",
        ),
        pytest.param(
            "answer = True & (0 >= _ROOT.x)",
            "(True:BooleanType() & (TPCH.x <= 0:Int64Type()))",
            id="arithmetic_10",
        ),
        pytest.param(
            "answer = (_ROOT.x == 42) | (45 == _ROOT.x) | ((_ROOT.x < 16) & (_ROOT.x != 0)) | ((100 < _ROOT.x) ^ (0 == _ROOT.y))",
            "((((TPCH.x == 42:Int64Type()) | (TPCH.x == 45:Int64Type())) | ((TPCH.x < 16:Int64Type()) & (TPCH.x != 0:Int64Type()))) | ((TPCH.x > 100:Int64Type()) ^ (TPCH.y == 0:Int64Type())))",
            id="arithmetic_11",
        ),
        pytest.param(
            "answer = False ^ 100 % 2.718281828 ** _ROOT.x",
            "(False:BooleanType() ^ (100:Int64Type() % (2.718281828:Float64Type() ** TPCH.x)))",
            id="arithmetic_12",
        ),
        pytest.param(
            "answer = _ROOT.Parts(part_name=LOWER(_ROOT.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            "TPCH.Parts(part_name=LOWER(TPCH.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            id="multi_calc_with_back",
        ),
        pytest.param(
            """\
x = _ROOT.Parts(part_name=LOWER(_ROOT.name))
y = x.WHERE(STARTSWITH(_ROOT.part_name, 'a'))
answer = y.ORDER_BY(_ROOT.retail_price.DESC())\
""",
            "TPCH.Parts(part_name=LOWER(TPCH.name)).WHERE(STARTSWITH(TPCH.part_name, 'a':StringType())).ORDER_BY(TPCH.retail_price.DESC(na_pos='last'))",
            id="calc_with_where_order",
        ),
        pytest.param(
            "answer = _ROOT.TOP_K(10, by=(1 / (_ROOT.retail_price - 30.0)).ASC(na_pos='first'))",
            "TPCH.TOP_K(10, by=((1:Int64Type() / (TPCH.retail_price - 30.0:Float64Type())).ASC(na_pos='first')))",
            id="topk_single",
        ),
        pytest.param(
            "answer = _ROOT.TOP_K(10, by=(_ROOT.size.DESC(), _ROOT.type.DESC()))",
            "TPCH.TOP_K(10, by=(TPCH.size.DESC(na_pos='last'), TPCH.type.DESC(na_pos='last')))",
            id="topk_multiple",
        ),
        pytest.param(
            """\
x = _ROOT.Parts.ORDER_BY(_ROOT.retail_price.ASC(na_pos='last'))
answer = x.TOP_K(100)\
""",
            "TPCH.Parts.ORDER_BY(TPCH.retail_price.ASC(na_pos='last')).TOP_K(100)",
            id="order_topk_empty",
        ),
        pytest.param(
            "answer = PARTITION(_ROOT.Parts, name='parts', by=_ROOT.type)(type=_ROOT.type, total_price=SUM(_ROOT.data.retail_price), n_orders=COUNT(_ROOT.data.lines))",
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
    (with unknown variables already pre-pended with `_ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    root: UnqualifiedNode = UnqualifiedRoot(get_sample_graph("TPCH"))
    env = {"_ROOT": root, "BACK": UnqualifiedBack, "datetime": datetime}
    exec(pydough_str, global_ctx, env)
    assert "answer" in env, "Expected `pydough_str` to define a variable `answer`."
    answer = env["answer"]
    assert isinstance(
        answer, UnqualifiedNode
    ), "Expected `pydough_str` to define `answer` as an UnqualifiedNode."
    assert (
        repr(answer) == answer_str
    ), "Mismatch between string representation of `answer` and expected value."
