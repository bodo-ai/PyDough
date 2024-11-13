"""
TODO: add file-level docstring.
"""

import ast
import datetime
from collections.abc import Callable

import pytest
from test_utils import graph_fetcher
from tpch_test_functions import (
    impl_tpch_q6,
    impl_tpch_q10,
)

from pydough import init_pydough_context
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
    UnqualifiedNode,
    UnqualifiedRoot,
    transform_code,
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


def verify_pydough_code_exec_match_unqualified(
    pydough_str: str, ctx: dict[str, object], env: dict[str, object], expected_str: str
):
    """
    Verifies that executing a snippet of Python code corresponding to PyDough
    code correctly produces an UnqualifiedNode instance with the expected
    string representation. The string must store the final answer in a variable
    named `answer`.

    Args:
        `pydough_str`: the source code to be checked.
        `ctx`: the dictionary of global variables to be used for `exec`.
        `env`: the dictionary of local variables  to be used for `exec`.
        `expected_string`: the expected string representation of `answer`.

    Raises:
        `AssertionError` if `answer` was not defined, if it is not an
        UnqualifiedNode, or its repr does not match `expected_str`.
    """
    exec(pydough_str, ctx, env)
    assert "answer" in env, "Expected `pydough_str` to define a variable `answer`."
    answer = env["answer"]
    assert isinstance(
        answer, UnqualifiedNode
    ), "Expected `pydough_str` to define `answer` as an UnqualifiedNode."
    assert (
        repr(answer) == expected_str
    ), "Mismatch between string representation of `answer` and expected value."


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
            "answer = _ROOT.Parts.TOP_K(10, by=(1 / (_ROOT.retail_price - 30.0)).ASC(na_pos='first'))",
            "TPCH.Parts.TOP_K(10, by=((1:Int64Type() / (TPCH.retail_price - 30.0:Float64Type())).ASC(na_pos='first')))",
            id="topk_single",
        ),
        pytest.param(
            "answer = _ROOT.Parts.TOP_K(10, by=(_ROOT.size.DESC(), _ROOT.part_type.DESC()))",
            "TPCH.Parts.TOP_K(10, by=(TPCH.size.DESC(na_pos='last'), TPCH.part_type.DESC(na_pos='last')))",
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
            "answer = PARTITION(_ROOT.Parts, name='parts', by=_ROOT.part_type)(type=_ROOT.part_type, total_price=SUM(_ROOT.data.retail_price), n_orders=COUNT(_ROOT.data.lines))",
            "PARTITION(TPCH.Parts, name='parts', by=(TPCH.part_type))(type=TPCH.part_type, total_price=SUM(TPCH.data.retail_price), n_orders=COUNT(TPCH.data.lines))",
            id="partition",
        ),
    ],
)
def test_unqualified_to_string(
    pydough_str: str,
    answer_str: str,
    global_ctx: dict[str, object],
    get_sample_graph: graph_fetcher,
    sample_graph_path: str,
):
    """
    Tests that strings representing the setup of PyDough unqualified objects
    (with unknown variables already pre-pended with `_ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    # Test with the strings that contain "_ROOT."
    root: UnqualifiedNode = UnqualifiedRoot(get_sample_graph("TPCH"))
    env: dict[str, object] = {"_ROOT": root}
    verify_pydough_code_exec_match_unqualified(pydough_str, global_ctx, env, answer_str)

    # Now test again, but with "_ROOT." prefixes removed & re-added via
    # a call to the `transform_code` procedure.
    altered_code: list[str] = [""]
    altered_code.append("def PYDOUGH_FUNC():")
    for line in pydough_str.splitlines():
        altered_code.append(
            f"  {line.replace('_ROOT.', '').replace('answer = ', 'return ')}"
        )
    new_code: str = ast.unparse(
        transform_code(
            "\n".join(altered_code),
            sample_graph_path,
            "TPCH",
            set(global_ctx) | {"init_pydough_context"},
        )
    )
    new_code += "\nanswer = PYDOUGH_FUNC()"
    verify_pydough_code_exec_match_unqualified(new_code, global_ctx, {}, answer_str)


@pytest.mark.parametrize(
    "func, as_string",
    [
        pytest.param(
            impl_tpch_q6,
            "TPCH.TPCH(revenue=SUM(TPCH.Lineitems.WHERE(((((TPCH.ship_date >= datetime.date(1994, 1, 1):DateType()) & (TPCH.ship_date < datetime.date(1995, 1, 1):DateType())) & (TPCH.discount < 0.07:Float64Type())) & (TPCH.quantity < 24:Int64Type())))(amt=(TPCH.extendedprice * TPCH.discount)).amt))",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q10,
            "TPCH.Customers(key=TPCH.key, name=TPCH.name, revenue=SUM(TPCH.orders.WHERE(((TPCH.order_date >= datetime.date(1993, 10, 1):DateType()) & (TPCH.order_date < datetime.date(1994, 1, 1):DateType()))).lines.WHERE((TPCH.return_flag == 'R':StringType()))(amt=(TPCH.extendedprice * (1:Int64Type() - TPCH.discount))).amt), acctbal=TPCH.acctbal, nation_name=TPCH.nation.name, address=TPCH.address, phone=TPCH.phone, comment=TPCH.comment).ORDER_BY(TPCH.revenue.DESC(na_pos='last'), TPCH.key.ASC(na_pos='last')).TOP_K(20)",
            id="tpch_q10",
        ),
    ],
)
def test_init_pydough_context(
    func: Callable[[], UnqualifiedNode], as_string: str, sample_graph_path: str
):
    """
    Tests that the `init_pydough_context` decorator correctly works on several
    PyDough functions, transforming them into the correct unqualified nodes,
    at least based on string representation.
    """
    new_func: Callable[[], UnqualifiedNode] = init_pydough_context(
        sample_graph_path, "TPCH"
    )(func)
    answer: UnqualifiedNode = new_func()
    assert (
        repr(answer) == as_string
    ), "Mismatch between string representation of unqualified nodes and expected output"
