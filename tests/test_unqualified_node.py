"""
TODO: add file-level docstring.
"""

import ast
import datetime
from collections.abc import Callable

import pytest
from test_utils import graph_fetcher
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

from pydough import init_pydough_context
from pydough.unqualified import (
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
    for obj in [datetime]:
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
            "answer = _ROOT.Nations(nation_name=_ROOT.UPPER(_ROOT.name), total_balance=_ROOT.SUM(_ROOT.customers.acct_bal))",
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
            "answer = (_ROOT.STARTSWITH(_ROOT.x, 'hello') | _ROOT.ENDSWITH(_ROOT.x, 'world')) & _ROOT.CONTAINS(_ROOT.x, ' ')",
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
            "answer = _ROOT.Parts(part_name=_ROOT.LOWER(_ROOT.name)).suppliers_of_part.region(part_name=_ROOT.BACK(2).part_name)",
            "TPCH.Parts(part_name=LOWER(TPCH.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            id="multi_calc_with_back",
        ),
        pytest.param(
            """\
x = _ROOT.Parts(part_name=_ROOT.LOWER(_ROOT.name))
y = x.WHERE(_ROOT.STARTSWITH(_ROOT.part_name, 'a'))
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
            "answer = _ROOT.PARTITION(_ROOT.Parts, name='parts', by=_ROOT.part_type)(type=_ROOT.part_type, total_price=_ROOT.SUM(_ROOT.data.retail_price), n_orders=_ROOT.COUNT(_ROOT.data.lines))",
            "TPCH.PARTITION(TPCH.Parts, name='parts', by=(TPCH.part_type))(type=TPCH.part_type, total_price=SUM(TPCH.data.retail_price), n_orders=COUNT(TPCH.data.lines))",
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
            impl_tpch_q1,
            "TPCH.PARTITION(TPCH.Lineitems.WHERE((TPCH.ship_date <= datetime.date(1998, 12, 1):DateType())), name='l', by=(TPCH.return_flag, TPCH.line_status))(return_flag=TPCH.return_flag, line_status=TPCH.line_status, sum_qty=SUM(TPCH.l.quantity), sum_base_price=SUM(TPCH.l.extended_price), sum_disc_price=SUM((TPCH.l.extended_price * (1:Int64Type() - TPCH.l.discount))), sum_charge=SUM(((TPCH.l.extended_price * (1:Int64Type() - TPCH.l.discount)) * (1:Int64Type() + TPCH.l.tax))), avg_qty=AVG(TPCH.l.quantity), avg_price=AVG(TPCH.l.extended_price), avg_disc=AVG(TPCH.l.discount), count_order=COUNT(TPCH.l)).ORDER_BY(TPCH.return_flag.ASC(na_pos='last'), TPCH.line_status.ASC(na_pos='last'))",
            id="tpch_q1",
        ),
        pytest.param(
            impl_tpch_q2,
            "TPCH.PARTITION(TPCH.Nations.WHERE((TPCH.region.name == 'EUROPE':StringType())).suppliers.parts_supplied(s_acctbal=BACK(1).account_balance, s_name=BACK(1).name, n_name=BACK(2).name, p_partkey=TPCH.key, p_mfgr=TPCH.manufacturer, s_address=BACK(1).address, s_phone=BACK(1).phone, s_comment=BACK(1).comment), name='p', by=(TPCH.key))(best_cost=MIN(TPCH.p.ps_supplycost)).p.WHERE((((TPCH.ps_supplycost == BACK(1).best_cost) & ENDSWITH(TPCH.part_type, 'BRASS':StringType())) & (TPCH.size == 15:Int64Type()))).ORDER_BY(TPCH.s_acctbal.DESC(na_pos='last'), TPCH.n_name.ASC(na_pos='last'), TPCH.s_name.ASC(na_pos='last'), TPCH.p_partkey.ASC(na_pos='last'))",
            id="tpch_q2",
        ),
        pytest.param(
            impl_tpch_q3,
            "TPCH.PARTITION(TPCH.Orders.WHERE(((TPCH.customer.mktsegment == 'BUILDING':StringType()) & (TPCH.order_date < datetime.date(1995, 3, 15):DateType()))).lines.WHERE((TPCH.ship_date > datetime.date(1995, 3, 15):DateType()))(order_date=BACK(1).order_date, ship_priority=BACK(1).ship_priority), name='l', by=(TPCH.order_key, TPCH.order_date, TPCH.ship_priority))(l_orderkey=TPCH.order_key, revenue=SUM((TPCH.l.extended_price * (1:Int64Type() - TPCH.l.discount))), o_orderdate=TPCH.order_date, o_shippriority=TPCH.ship_priority).TOP_K(10, by=(TPCH.revenue.DESC(na_pos='last'), TPCH.o_orderdate.ASC(na_pos='last'), TPCH.l_orderkey.ASC(na_pos='last')))",
            id="tpch_q3",
        ),
        pytest.param(
            impl_tpch_q4,
            "TPCH.PARTITION(TPCH.Orders.WHERE((((TPCH.order_date >= datetime.date(1993, 7, 1):DateType()) & (TPCH.order_date < datetime.date(1993, 10, 1):DateType())) & (COUNT(TPCH.lines.WHERE((TPCH.commit_date < TPCH.receipt_date))) > 0:Int64Type()))), name='o', by=(TPCH.order_priority))(order_priority=TPCH.order_priority, order_count=COUNT(TPCH.o)).ORDER_BY(TPCH.order_priority.ASC(na_pos='last'))",
            id="tpch_q4",
        ),
        pytest.param(
            impl_tpch_q5,
            "TPCH.Nations.WHERE((TPCH.region.name == 'ASIA':StringType()))(n_name=TPCH.name, revenue=SUM(TPCH.customers.orders.WHERE(((TPCH.order_date >= datetime.date(1994, 1, 1):DateType()) & (TPCH.order_date < datetime.date(1995, 1, 1):DateType()))).lines.WHERE((TPCH.supplier.nation.name == BACK(3).name)).value)).ORDER_BY(TPCH.revenue.DESC(na_pos='last'))",
            id="tpch_q5",
        ),
        pytest.param(
            impl_tpch_q6,
            "TPCH.TPCH(revenue=SUM(TPCH.Lineitems.WHERE((((((TPCH.ship_date >= datetime.date(1994, 1, 1):DateType()) & (TPCH.ship_date < datetime.date(1995, 1, 1):DateType())) & (TPCH.discount > 0.05:Float64Type())) & (TPCH.discount < 0.07:Float64Type())) & (TPCH.quantity < 24:Int64Type())))(amt=(TPCH.extendedprice * TPCH.discount)).amt))",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q7,
            "TPCH.PARTITION(TPCH.Lineitems(supp_nation=TPCH.supplier.nation.name, cust_nation=TPCH.customer.nation.name, l_year=YEAR(TPCH.ship_date), volume=(TPCH.extended_price * (1:Int64Type() - TPCH.discount))).WHERE((((TPCH.ship_date >= datetime.date(1995, 1, 1):DateType()) & (TPCH.ship_date <= datetime.date(1996, 12, 31):DateType())) & (((TPCH.supp_nation == 'France':StringType()) & (TPCH.cust_nation == 'Germany':StringType())) | ((TPCH.supp_nation == 'Germany':StringType()) & (TPCH.cust_nation == 'France':StringType()))))), name='l', by=(TPCH.supp_nation, TPCH.cust_nation, TPCH.l_year))(supp_nation=TPCH.supp_nation, cust_nation=TPCH.cust_nation, l_year=TPCH.l_year, revenue=SUM(TPCH.l.volume)).ORDER_BY(TPCH.supp_nation.ASC(na_pos='last'), TPCH.cust_nation.ASC(na_pos='last'), TPCH.l_year.ASC(na_pos='last'))",
            id="tpch_q7",
        ),
        pytest.param(
            impl_tpch_q8,
            "TPCH.PARTITION(TPCH.Orders.WHERE((((TPCH.ship_date >= datetime.date(1995, 1, 1):DateType()) & (TPCH.ship_date <= datetime.date(1996, 12, 31):DateType())) & (TPCH.customer.region.name == 'AMERICA':StringType()))).lines(volume=(TPCH.extended_price * (1:Int64Type() - TPCH.discount))).supplier.WHERE((TPCH.ps_part.p_type == 'ECONOMY ANODIZED STEEL':StringType()))(o_year=YEAR(BACK(2).order_date), volume=BACK(1).volume, brazil_volume=IFF((TPCH.nation.name == 'BRAZIL':StringType()), BACK(1).volume, 0:Int64Type())), name='v', by=(TPCH.o_year))(o_year=TPCH.o_year, mkt_share=(SUM(TPCH.v.brazil_volume) / SUM(TPCH.v.volume)))",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
            "TPCH.PARTITION(TPCH.Nations.suppliers.lines(nation=BACK(2).name, o_year=YEAR(TPCH.order.order_date), value=((TPCH.extended_price * (1:Int64Type() - TPCH.discount)) - (TPCH.ps_supplycost * TPCH.quantity))).WHERE(CONTAINS(TPCH.part.name, 'green':StringType())), name='l', by=(TPCH.nation, TPCH.o_year))(nation=TPCH.nation, o_year=TPCH.o_year, amount=SUM(TPCH.l.value)).ORDER_BY(TPCH.nation.ASC(na_pos='last'), TPCH.o_year.dESC())",
            id="tpch_q9",
        ),
        pytest.param(
            impl_tpch_q10,
            "TPCH.Customers(c_key=TPCH.key, c_name=TPCH.name, revenue=SUM(TPCH.orders.WHERE(((TPCH.order_date >= datetime.date(1993, 10, 1):DateType()) & (TPCH.order_date < datetime.date(1994, 1, 1):DateType()))).lines.WHERE((TPCH.return_flag == 'R':StringType()))(amt=(TPCH.extendedprice * (1:Int64Type() - TPCH.discount))).amt), c_acctbal=TPCH.acctbal, n_name=TPCH.nation.name, c_address=TPCH.address, c_phone=TPCH.phone, c_comment=TPCH.comment).ORDER_BY(TPCH.revenue.DESC(na_pos='last'), TPCH.c_key.ASC(na_pos='last')).TOP_K(20)",
            id="tpch_q10",
        ),
        pytest.param(
            impl_tpch_q11,
            "TPCH.PARTITION(TPCH.PartSupp.WHERE((TPCH.supplier.nation.name == 'GERMANY':StringType())), name='ps', by=(TPCH.part_key))(ps_partkey=TPCH.part_key, val=SUM((TPCH.ps.supplycost * TPCH.ps.availqty))).WHERE((TPCH.val > TPCH.TPCH(amt=(SUM((TPCH.PartSupp.WHERE((TPCH.supplier.nation.name == 'GERMANY':StringType())).supplycost * TPCH.PartSupp.WHERE((TPCH.supplier.nation.name == 'GERMANY':StringType())).availqty)) * 0.0001:Float64Type())).amt))",
            id="tpch_q11",
        ),
        pytest.param(
            impl_tpch_q12,
            "TPCH.PARTITION(TPCH.Lineitems.WHERE(((((((TPCH.ship_mode == 'MAIL':StringType()) | (TPCH.ship_mode == 'SHIP':StringType())) & (TPCH.ship_date < TPCH.commit_date)) & (TPCH.commit_date < TPCH.receipt_date)) & (TPCH.receipt_date >= datetime.date(1994, 1, 1):DateType())) & (TPCH.receipt_date < datetime.date(1995, 1, 1):DateType())))(is_high_priority=((TPCH.order.order_priority == '1-URGENT':StringType()) | (TPCH.order.order_priority == '2-HIGH':StringType()))), name='l', by=(TPCH.ship_mode))(ship_mode=TPCH.ship_mode, high_line_count=SUM(TPCH.l.is_high_priority), low_line_count=SUM(NOT(TPCH.l.is_high_priority))).ORDER_BY(TPCH.ship_mode.ASC(na_pos='last'))",
            id="tpch_q12",
        ),
        pytest.param(
            impl_tpch_q13,
            "TPCH.PARTITION(TPCH.Customers(key=TPCH.key, num_non_special_orders=COUNT(TPCH.orders.WHERE(NOT(LIKE(TPCH.comment, '%special%requests%':StringType()))))), name='custs', by=(TPCH.num_non_special_orders))(c_count=TPCH.num_non_special_orders, custdist=COUNT(TPCH.custs))",
            id="tpch_q13",
        ),
        pytest.param(
            impl_tpch_q14,
            "TPCH.TPCH(promo_revenue=((100.0:Float64Type() * SUM(TPCH.Lineitems.WHERE(((TPCH.ship_date >= datetime.date(1995, 9, 1):DateType()) & (TPCH.ship_date < datetime.date(1995, 10, 1):DateType())))(value=(TPCH.extended_price * (1:Int64Type() - TPCH.discount)), promo_value=IFF(STARTSWITH(TPCH.part.part_type, 'PROMO':StringType()), (TPCH.extended_price * (1:Int64Type() - TPCH.discount)), 0:Int64Type())).promo_value)) / SUM(TPCH.Lineitems.WHERE(((TPCH.ship_date >= datetime.date(1995, 9, 1):DateType()) & (TPCH.ship_date < datetime.date(1995, 10, 1):DateType())))(value=(TPCH.extended_price * (1:Int64Type() - TPCH.discount)), promo_value=IFF(STARTSWITH(TPCH.part.part_type, 'PROMO':StringType()), (TPCH.extended_price * (1:Int64Type() - TPCH.discount)), 0:Int64Type())).value)))",
            id="tpch_q14",
        ),
        pytest.param(
            impl_tpch_q15,
            "TPCH.TPCH(max_revenue=MAX(TPCH.Suppliers(total_revenue=SUM((TPCH.lines.WHERE(((TPCH.ship_date >= datetime.date(1996, 1, 1):DateType()) & (TPCH.ship_date < datetime.date(1996, 3, 1):DateType()))).extended_price * (1:Int64Type() - TPCH.lines.WHERE(((TPCH.ship_date >= datetime.date(1996, 1, 1):DateType()) & (TPCH.ship_date < datetime.date(1996, 3, 1):DateType()))).discount)))).total_revenue)).supplier_values(s_suppkey=TPCH.key, s_name=TPCH.name, s_address=TPCH.address, s_phone=TPCH.phone_number, total_revenue=TPCH.total_revenue).WHERE((TPCH.total_revenue == BACK(1).max_revnue)).ORDER_BY(TPCH.s_suppkey.ASC(na_pos='last'))",
            id="tpch_q15",
        ),
        pytest.param(
            impl_tpch_q16,
            "TPCH.PARTITION(TPCH.Parts.WHERE((((TPCH.brand != 'BRAND#45':StringType()) & NOT(STARTSWITH(TPCH.part_type, 'MEDIUM POLISHED%':StringType()))) & ISIN(TPCH.size, [49:Int64Type(), 14:Int64Type(), 23:Int64Type(), 45:Int64Type(), 19:Int64Type(), 3:Int64Type(), 36:Int64Type(), 9:Int64Type()]:ArrayType(UnknownType())))).supply_records(p_brand=BACK(1).brand, p_type=BACK(1).part_type, p_size=BACK(1).size, ps_suppkey=TPCH.supplier_key).WHERE(NOT(LIKE(TPCH.supplier.comment, '%Customer%Complaints%':StringType()))), name='ps', by=(TPCH.p_brand, TPCH.p_type, TPCH.p_size))(p_brand=TPCH.p_brand, p_type=TPCH.p_type, p_size=TPCH.p_size, supplier_cnt=NDISTINCT(TPCH.ps.supplier_key))",
            id="tpch_q16",
        ),
        pytest.param(
            impl_tpch_q17,
            "TPCH.TPCH(avg_yearly=(SUM(TPCH.parts.WHERE(((TPCH.p_brand == 'Brand#23':StringType()) & (TPCH.p_container == 'MED BOX':StringType())))(avg_quantity=AVG(TPCH.lines.quantity)).lines.WHERE((TPCH.quantity < (0.2:Float64Type() * BACK(1).avg_quantity))).extended_price) / 7.0:Float64Type()))",
            id="tpch_q17",
        ),
        pytest.param(
            impl_tpch_q18,
            "TPCH.Orders(c_name=TPCH.customer.name, c_custkey=TPCH.customer.key, o_orderkey=TPCH.key, o_orderdate=TPCH.order_date, o_totalprice=TPCH.total_price, total_quantity=SUM(TPCH.lines.quantity)).WHERE((TPCH.total_quantity > 300:Int64Type())).ORDER_BY(TPCH.o_totalprice.DESC(na_pos='last'), TPCH.o_orderdate.ASC(na_pos='last'))",
            id="tpch_q18",
        ),
        pytest.param(
            impl_tpch_q19,
            "TPCH.TPCH(revenue=SUM((TPCH.Lineitems.WHERE((((True:BooleanType() & (TPCH.shipinstruct == 'DELIVER IN PERSON':StringType())) & (TPCH.part.size >= 1:Int64Type())) & (((((((TPCH.part.size < 5:Int64Type()) & (TPCH.quantity >= 1:Int64Type())) & (TPCH.quantity <= 11:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#12':StringType())) | (((((TPCH.part.size < 10:Int64Type()) & (TPCH.quantity >= 10:Int64Type())) & (TPCH.quantity <= 21:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#23':StringType()))) | (((((TPCH.part.size < 15:Int64Type()) & (TPCH.quantity >= 20:Int64Type())) & (TPCH.quantity <= 31:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#34':StringType()))))).extended_price * (1:Int64Type() - TPCH.Lineitems.WHERE((((True:BooleanType() & (TPCH.shipinstruct == 'DELIVER IN PERSON':StringType())) & (TPCH.part.size >= 1:Int64Type())) & (((((((TPCH.part.size < 5:Int64Type()) & (TPCH.quantity >= 1:Int64Type())) & (TPCH.quantity <= 11:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#12':StringType())) | (((((TPCH.part.size < 10:Int64Type()) & (TPCH.quantity >= 10:Int64Type())) & (TPCH.quantity <= 21:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#23':StringType()))) | (((((TPCH.part.size < 15:Int64Type()) & (TPCH.quantity >= 20:Int64Type())) & (TPCH.quantity <= 31:Int64Type())) & True:BooleanType()) & (TPCH.brand == 'Brand#34':StringType()))))).discount))))",
            id="tpch_q19",
        ),
        pytest.param(
            impl_tpch_q20,
            "TPCH.Parts.WHERE(STARTSWITH(TPCH.name, 'forest':StringType()))(quantity_threshold=SUM(TPCH.lines.WHERE(((TPCH.ship_date >= datetime.date(1994, 1, 1):DateType()) & (TPCH.ship_date < datetime.date(1995, 1, 1):DateType()))).quantity)).suppliers.WHERE(((TPCH.ps_availqty > BACK(1).quantity_threshold) & (TPCH.nation.name == 'CANADA':StringType())))(s_name=TPCH.name, s_address=TPCH.address)",
            id="tpch_q20",
        ),
        pytest.param(
            impl_tpch_q21,
            "TPCH.Suppliers.WHERE((TPCH.nation.name == 'SAUDI ARABIA':StringType()))(s_name=TPCH.name, numwait=COUNT(TPCH.lines.WHERE((TPCH.receipt_date > TPCH.commit_date)).order.WHERE((((TPCH.order_status == 'F':StringType()) & (COUNT(TPCH.lines.WHERE((TPCH.supplier_key != BACK(2).supplier_key))) > 0:Int64Type())) & (COUNT(TPCH.lines.WHERE(((TPCH.supplier_key != BACK(2).supplier_key) & (TPCH.receipt_date > TPCH.commit_date)))) == 0:Int64Type()))))).ORDER_BY(TPCH.numwait.DESC(na_pos='last'), TPCH.s_name.ASC(na_pos='last'))",
            id="tpch_q21",
        ),
        pytest.param(
            impl_tpch_q22,
            "TPCH.PARTITION(TPCH.TPCH(avg_balance=AVG(TPCH.Customers(cntry_code=SLICE(TPCH.c_phone, None:UnknownType(), 2:Int64Type(), None:UnknownType())).WHERE((ISIN(TPCH.cntry_code, ['13':StringType(), '31':StringType(), '23':StringType(), '29':StringType(), '30':StringType(), '18':StringType(), '17':StringType()]:ArrayType(UnknownType())) & (COUNT(TPCH.orders) == 0:Int64Type()))).WHERE((TPCH.acct_bal > 0.0:Float64Type())).acct_bal)).cust_info.WHERE((TPCH.acct_bal > BACK(1).avg_balance)), name='custs', by=(TPCH.cntry_code))(cntry_code=TPCH.cntry_code, num_custs=COUNT(TPCH.custs), totacctbal=SUM(TPCH.acct_bal))",
            id="tpch_q22",
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
