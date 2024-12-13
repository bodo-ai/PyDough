"""
Unit tests the PyDough unqualified nodes and the logic that transforms raw
Python code into them.

Copyright (C) 2024 Bodo Inc. All rights reserved.
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

import pydough
from pydough import init_pydough_context
from pydough.metadata import GraphMetadata
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
    assert (
        pydough.display_raw(answer) == expected_str
    ), "Mismatch between string representation of `answer` and expected value."


@pytest.mark.parametrize(
    "pydough_str, answer_str",
    [
        pytest.param(
            "answer = _ROOT.Parts",
            "?.Parts",
            id="access_collection",
        ),
        pytest.param(
            "answer = _ROOT.Regions.nations",
            "?.Regions.nations",
            id="access_subcollection",
        ),
        pytest.param(
            "answer = _ROOT.Regions.name",
            "?.Regions.name",
            id="access_property",
        ),
        pytest.param(
            "answer = _ROOT.Regions(region_name=_ROOT.name, region_key=_ROOT.key)",
            "?.Regions(region_name=?.name, region_key=?.key)",
            id="simple_calc",
        ),
        pytest.param(
            "answer = _ROOT.Nations(nation_name=_ROOT.UPPER(_ROOT.name), total_balance=_ROOT.SUM(_ROOT.customers.acct_bal))",
            "?.Nations(nation_name=UPPER(?.name), total_balance=SUM(?.customers.acct_bal))",
            id="calc_with_functions",
        ),
        pytest.param(
            "answer = _ROOT.x + 1",
            "(?.x + 1)",
            id="arithmetic_01",
        ),
        pytest.param(
            "answer = 2 + _ROOT.x",
            "(2 + ?.x)",
            id="arithmetic_02",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5 * ?.x) - 1)",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5 * ?.x) - 1)",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = (_ROOT.STARTSWITH(_ROOT.x, 'hello') | _ROOT.ENDSWITH(_ROOT.x, 'world')) & _ROOT.CONTAINS(_ROOT.x, ' ')",
            "((STARTSWITH(?.x, 'hello') | ENDSWITH(?.x, 'world')) & CONTAINS(?.x, ' '))",
            id="arithmetic_04",
        ),
        pytest.param(
            "answer = (1 / _ROOT.x) ** 2 - _ROOT.y",
            "(((1 / ?.x) ** 2) - ?.y)",
            id="arithmetic_05",
        ),
        pytest.param(
            "answer = -(_ROOT.x % 10) / 3.1415",
            "((0 - (?.x % 10)) / 3.1415)",
            id="arithmetic_06",
        ),
        pytest.param(
            "answer = (+_ROOT.x < -_ROOT.y) ^ (_ROOT.y == _ROOT.z)",
            "((?.x < (0 - ?.y)) ^ (?.y == ?.z))",
            id="arithmetic_07",
        ),
        pytest.param(
            "answer = 'Hello' != _ROOT.word",
            "(?.word != 'Hello')",
            id="arithmetic_08",
        ),
        pytest.param(
            "answer = _ROOT.order_date >= datetime.date(2020, 1, 1)",
            "(?.order_date >= datetime.date(2020, 1, 1))",
            id="arithmetic_09",
        ),
        pytest.param(
            "answer = True & (0 >= _ROOT.x)",
            "(True & (?.x <= 0))",
            id="arithmetic_10",
        ),
        pytest.param(
            "answer = (_ROOT.x == 42) | (45 == _ROOT.x) | ((_ROOT.x < 16) & (_ROOT.x != 0)) | ((100 < _ROOT.x) ^ (0 == _ROOT.y))",
            "((((?.x == 42) | (?.x == 45)) | ((?.x < 16) & (?.x != 0))) | ((?.x > 100) ^ (?.y == 0)))",
            id="arithmetic_11",
        ),
        pytest.param(
            "answer = False ^ 100 % 2.718281828 ** _ROOT.x",
            "(False ^ (100 % (2.718281828 ** ?.x)))",
            id="arithmetic_12",
        ),
        pytest.param(
            "answer = _ROOT.Parts(part_name=_ROOT.LOWER(_ROOT.name)).suppliers_of_part.region(part_name=_ROOT.BACK(2).part_name)",
            "?.Parts(part_name=LOWER(?.name)).suppliers_of_part.region(part_name=BACK(2).part_name)",
            id="multi_calc_with_back",
        ),
        pytest.param(
            """\
x = _ROOT.Parts(part_name=_ROOT.LOWER(_ROOT.name))
y = x.WHERE(_ROOT.STARTSWITH(_ROOT.part_name, 'a'))
answer = y.ORDER_BY(_ROOT.retail_price.DESC())\
""",
            "?.Parts(part_name=LOWER(?.name)).WHERE(STARTSWITH(?.part_name, 'a')).ORDER_BY(?.retail_price.DESC(na_pos='last'))",
            id="calc_with_where_order",
        ),
        pytest.param(
            "answer = _ROOT.Parts.TOP_K(10, by=(1 / (_ROOT.retail_price - 30.0)).ASC(na_pos='first'))",
            "?.Parts.TOP_K(10, by=((1 / (?.retail_price - 30.0)).ASC(na_pos='first')))",
            id="topk_single",
        ),
        pytest.param(
            "answer = _ROOT.Parts.TOP_K(10, by=(_ROOT.size.DESC(), _ROOT.part_type.DESC()))",
            "?.Parts.TOP_K(10, by=(?.size.DESC(na_pos='last'), ?.part_type.DESC(na_pos='last')))",
            id="topk_multiple",
        ),
        pytest.param(
            """\
x = _ROOT.Parts.ORDER_BY(_ROOT.retail_price.ASC(na_pos='last'))
answer = x.TOP_K(100)\
""",
            "?.Parts.ORDER_BY(?.retail_price.ASC(na_pos='last')).TOP_K(100)",
            id="order_topk_empty",
        ),
        pytest.param(
            "answer = _ROOT.PARTITION(_ROOT.Parts, name='parts', by=_ROOT.part_type)(type=_ROOT.part_type, total_price=_ROOT.SUM(_ROOT.data.retail_price), n_orders=_ROOT.COUNT(_ROOT.data.lines))",
            "?.PARTITION(?.Parts, name='parts', by=(?.part_type))(type=?.part_type, total_price=SUM(?.data.retail_price), n_orders=COUNT(?.data.lines))",
            id="partition",
        ),
    ],
)
def test_unqualified_to_string(
    pydough_str: str,
    answer_str: str,
    global_ctx: dict[str, object],
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that strings representing the setup of PyDough unqualified objects
    (with unknown variables already pre-pended with `_ROOT.`) are correctly
    transformed into UnqualifiedNode objects with an expected string
    representation. Each `pydough_str` should be called with `exec` to define
    a variable `answer` that is an `UnqualifiedNode` instance.
    """
    # Test with the strings that contain "_ROOT."
    graph_dict: dict[str, GraphMetadata] = {"_graph": get_sample_graph("TPCH")}
    root: UnqualifiedNode = UnqualifiedRoot(graph_dict["_graph"])
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
            graph_dict,
            set(global_ctx) | {"init_pydough_context"},
        )
    )
    new_code += "\nanswer = PYDOUGH_FUNC()"
    verify_pydough_code_exec_match_unqualified(
        new_code, global_ctx | graph_dict, {}, answer_str
    )


@pytest.mark.parametrize(
    "func, as_string",
    [
        pytest.param(
            impl_tpch_q1,
            "?.PARTITION(?.Lineitems.WHERE((?.ship_date <= datetime.date(1998, 12, 1))), name='l', by=(?.return_flag, ?.status))(L_RETURNFLAG=?.return_flag, L_LINESTATUS=?.status, SUM_QTY=SUM(?.l.quantity), SUM_BASE_PRICE=SUM(?.l.extended_price), SUM_DISC_PRICE=SUM((?.l.extended_price * (1 - ?.l.discount))), SUM_CHARGE=SUM(((?.l.extended_price * (1 - ?.l.discount)) * (1 + ?.l.tax))), AVG_QTY=AVG(?.l.quantity), AVG_PRICE=AVG(?.l.extended_price), AVG_DISC=AVG(?.l.discount), COUNT_ORDER=COUNT(?.l)).ORDER_BY(?.L_RETURNFLAG.ASC(na_pos='last'), ?.L_LINESTATUS.ASC(na_pos='last'))",
            id="tpch_q1",
        ),
        pytest.param(
            impl_tpch_q2,
            "?.PARTITION(?.Nations.WHERE((?.region.name == 'EUROPE')).suppliers.supply_records.part(s_acctbal=BACK(2).account_balance, s_name=BACK(2).name, n_name=BACK(3).name, s_address=BACK(2).address, s_phone=BACK(2).phone, s_comment=BACK(2).comment, supplycost=BACK(1).supplycost).WHERE((ENDSWITH(?.part_type, 'BRASS') & (?.size == 15))), name='p', by=(?.key))(best_cost=MIN(?.p.supplycost)).p.WHERE((((?.supplycost == BACK(1).best_cost) & ENDSWITH(?.part_type, 'BRASS')) & (?.size == 15)))(S_ACCTBAL=?.s_acctbal, S_NAME=?.s_name, N_NAME=?.n_name, P_PARTKEY=?.key, P_MFGR=?.manufacturer, S_ADDRESS=?.s_address, S_PHONE=?.s_phone, S_COMMENT=?.s_comment).TOP_K(10, by=(?.S_ACCTBAL.DESC(na_pos='last'), ?.N_NAME.ASC(na_pos='last'), ?.S_NAME.ASC(na_pos='last'), ?.P_PARTKEY.ASC(na_pos='last')))",
            id="tpch_q2",
        ),
        pytest.param(
            impl_tpch_q3,
            "?.PARTITION(?.Orders.WHERE(((?.customer.mktsegment == 'BUILDING') & (?.order_date < datetime.date(1995, 3, 15)))).lines.WHERE((?.ship_date > datetime.date(1995, 3, 15)))(order_date=BACK(1).order_date, ship_priority=BACK(1).ship_priority), name='l', by=(?.order_key, ?.order_date, ?.ship_priority))(L_ORDERKEY=?.order_key, REVENUE=SUM((?.l.extended_price * (1 - ?.l.discount))), O_ORDERDATE=?.order_date, O_SHIPPRIORITY=?.ship_priority).TOP_K(10, by=(?.REVENUE.DESC(na_pos='last'), ?.O_ORDERDATE.ASC(na_pos='last'), ?.L_ORDERKEY.ASC(na_pos='last')))",
            id="tpch_q3",
        ),
        pytest.param(
            impl_tpch_q4,
            "?.PARTITION(?.Orders.WHERE((((?.order_date >= datetime.date(1993, 7, 1)) & (?.order_date < datetime.date(1993, 10, 1))) & HAS(?.lines.WHERE((?.commit_date < ?.receipt_date))))), name='o', by=(?.order_priority))(O_ORDERPRIORITY=?.order_priority, ORDER_COUNT=COUNT(?.o)).ORDER_BY(?.O_ORDERPRIORITY.ASC(na_pos='last'))",
            id="tpch_q4",
        ),
        pytest.param(
            impl_tpch_q5,
            "?.Nations.WHERE((?.region.name == 'ASIA'))(N_NAME=?.name, REVENUE=SUM(?.customers.orders.WHERE(((?.order_date >= datetime.date(1994, 1, 1)) & (?.order_date < datetime.date(1995, 1, 1)))).lines.WHERE((?.supplier.nation.name == BACK(3).name))(value=(?.extended_price * (1 - ?.discount))).value)).ORDER_BY(?.REVENUE.DESC(na_pos='last'))",
            id="tpch_q5",
        ),
        pytest.param(
            impl_tpch_q6,
            "?.TPCH(REVENUE=SUM(?.Lineitems.WHERE((((((?.ship_date >= datetime.date(1994, 1, 1)) & (?.ship_date < datetime.date(1995, 1, 1))) & (?.discount >= 0.05)) & (?.discount <= 0.07)) & (?.quantity < 24)))(amt=(?.extended_price * ?.discount)).amt))",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q7,
            "?.PARTITION(?.Lineitems(supp_nation=?.supplier.nation.name, cust_nation=?.order.customer.nation.name, l_year=YEAR(?.ship_date), volume=(?.extended_price * (1 - ?.discount))).WHERE((((?.ship_date >= datetime.date(1995, 1, 1)) & (?.ship_date <= datetime.date(1996, 12, 31))) & (((?.supp_nation == 'FRANCE') & (?.cust_nation == 'GERMANY')) | ((?.supp_nation == 'GERMANY') & (?.cust_nation == 'FRANCE'))))), name='l', by=(?.supp_nation, ?.cust_nation, ?.l_year))(SUPP_NATION=?.supp_nation, CUST_NATION=?.cust_nation, L_YEAR=?.l_year, REVENUE=SUM(?.l.volume)).ORDER_BY(?.SUPP_NATION.ASC(na_pos='last'), ?.CUST_NATION.ASC(na_pos='last'), ?.L_YEAR.ASC(na_pos='last'))",
            id="tpch_q7",
        ),
        pytest.param(
            impl_tpch_q8,
            "?.PARTITION(?.Nations.suppliers.supply_records.WHERE((?.part.part_type == 'ECONOMY ANODIZED STEEL')).lines(volume=(?.extended_price * (1 - ?.discount))).order(o_year=YEAR(?.order_date), volume=BACK(1).volume, brazil_volume=IFF((BACK(4).name == 'BRAZIL'), BACK(1).volume, 0)).WHERE((((?.order_date >= datetime.date(1995, 1, 1)) & (?.order_date <= datetime.date(1996, 12, 31))) & (?.customer.nation.region.name == 'AMERICA'))), name='v', by=(?.o_year))(O_YEAR=?.o_year, MKT_SHARE=(SUM(?.v.brazil_volume) / SUM(?.v.volume)))",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
            "?.PARTITION(?.Nations.suppliers.supply_records.WHERE(CONTAINS(?.part.name, 'green')).lines(nation=BACK(3).name, o_year=YEAR(?.order.order_date), value=((?.extended_price * (1 - ?.discount)) - (BACK(1).supplycost * ?.quantity))), name='l', by=(?.nation, ?.o_year))(NATION=?.nation, O_YEAR=?.o_year, AMOUNT=SUM(?.l.value)).TOP_K(10, by=(?.NATION.ASC(na_pos='last'), ?.O_YEAR.DESC(na_pos='last')))",
            id="tpch_q9",
        ),
        pytest.param(
            impl_tpch_q10,
            "?.Customers(C_CUSTKEY=?.key, C_NAME=?.name, REVENUE=SUM(?.orders.WHERE(((?.order_date >= datetime.date(1993, 10, 1)) & (?.order_date < datetime.date(1994, 1, 1)))).lines.WHERE((?.return_flag == 'R'))(amt=(?.extended_price * (1 - ?.discount))).amt), C_ACCTBAL=?.acctbal, N_NAME=?.nation.name, C_ADDRESS=?.address, C_PHONE=?.phone, C_COMMENT=?.comment).TOP_K(20, by=(?.REVENUE.DESC(na_pos='last'), ?.C_CUSTKEY.ASC(na_pos='last')))",
            id="tpch_q10",
        ),
        pytest.param(
            impl_tpch_q11,
            "?.TPCH(min_market_share=(SUM(?.PartSupp.WHERE((?.supplier.nation.name == 'GERMANY'))(metric=(?.supplycost * ?.availqty)).metric) * 0.0001)).PARTITION(?.PartSupp.WHERE((?.supplier.nation.name == 'GERMANY'))(metric=(?.supplycost * ?.availqty)), name='ps', by=(?.part_key))(PS_PARTKEY=?.part_key, VALUE=SUM(?.ps.metric)).WHERE((?.VALUE > BACK(1).min_market_share)).TOP_K(10, by=(?.VALUE.DESC(na_pos='last')))",
            id="tpch_q11",
        ),
        pytest.param(
            impl_tpch_q12,
            "?.PARTITION(?.Lineitems.WHERE(((((((?.ship_mode == 'MAIL') | (?.ship_mode == 'SHIP')) & (?.ship_date < ?.commit_date)) & (?.commit_date < ?.receipt_date)) & (?.receipt_date >= datetime.date(1994, 1, 1))) & (?.receipt_date < datetime.date(1995, 1, 1))))(is_high_priority=((?.order.order_priority == '1-URGENT') | (?.order.order_priority == '2-HIGH'))), name='l', by=(?.ship_mode))(L_SHIPMODE=?.ship_mode, HIGH_LINE_COUNT=SUM(?.l.is_high_priority), LOW_LINE_COUNT=SUM(NOT(?.l.is_high_priority))).ORDER_BY(?.L_SHIPMODE.ASC(na_pos='last'))",
            id="tpch_q12",
        ),
        pytest.param(
            impl_tpch_q13,
            "?.PARTITION(?.Customers(key=?.key, num_non_special_orders=COUNT(?.orders.WHERE(NOT(LIKE(?.comment, '%special%requests%'))))), name='custs', by=(?.num_non_special_orders))(C_COUNT=?.num_non_special_orders, CUSTDIST=COUNT(?.custs)).TOP_K(10, by=(?.CUSTDIST.DESC(na_pos='last'), ?.C_COUNT.DESC(na_pos='last')))",
            id="tpch_q13",
        ),
        pytest.param(
            impl_tpch_q14,
            "?.TPCH(PROMO_REVENUE=((100.0 * SUM(?.Lineitems.WHERE(((?.ship_date >= datetime.date(1995, 9, 1)) & (?.ship_date < datetime.date(1995, 10, 1))))(value=(?.extended_price * (1 - ?.discount)), promo_value=IFF(STARTSWITH(?.part.part_type, 'PROMO'), (?.extended_price * (1 - ?.discount)), 0)).promo_value)) / SUM(?.Lineitems.WHERE(((?.ship_date >= datetime.date(1995, 9, 1)) & (?.ship_date < datetime.date(1995, 10, 1))))(value=(?.extended_price * (1 - ?.discount)), promo_value=IFF(STARTSWITH(?.part.part_type, 'PROMO'), (?.extended_price * (1 - ?.discount)), 0)).value)))",
            id="tpch_q14",
        ),
        pytest.param(
            impl_tpch_q15,
            "?.TPCH(max_revenue=MAX(?.Suppliers(total_revenue=SUM((?.lines.WHERE(((?.ship_date >= datetime.date(1996, 1, 1)) & (?.ship_date < datetime.date(1996, 4, 1)))).extended_price * (1 - ?.lines.WHERE(((?.ship_date >= datetime.date(1996, 1, 1)) & (?.ship_date < datetime.date(1996, 4, 1)))).discount)))).total_revenue)).Suppliers(S_SUPPKEY=?.key, S_NAME=?.name, S_ADDRESS=?.address, S_PHONE=?.phone, TOTAL_REVENUE=SUM((?.lines.WHERE(((?.ship_date >= datetime.date(1996, 1, 1)) & (?.ship_date < datetime.date(1996, 4, 1)))).extended_price * (1 - ?.lines.WHERE(((?.ship_date >= datetime.date(1996, 1, 1)) & (?.ship_date < datetime.date(1996, 4, 1)))).discount)))).WHERE((?.TOTAL_REVENUE == BACK(1).max_revenue)).ORDER_BY(?.S_SUPPKEY.ASC(na_pos='last'))",
            id="tpch_q15",
        ),
        pytest.param(
            impl_tpch_q16,
            "?.PARTITION(?.Parts.WHERE((((?.brand != 'BRAND#45') & NOT(STARTSWITH(?.part_type, 'MEDIUM POLISHED%'))) & ISIN(?.size, [49, 14, 23, 45, 19, 3, 36, 9]))).supply_records(p_brand=BACK(1).brand, p_type=BACK(1).part_type, p_size=BACK(1).size, ps_suppkey=?.supplier_key).WHERE(NOT(LIKE(?.supplier.comment, '%Customer%Complaints%'))), name='ps', by=(?.p_brand, ?.p_type, ?.p_size))(P_BRAND=?.p_brand, P_TYPE=?.p_type, P_SIZE=?.p_size, SUPPLIER_COUNT=NDISTINCT(?.ps.supplier_key)).TOP_K(10, by=(?.SUPPLIER_COUNT.DESC(na_pos='last'), ?.P_BRAND.ASC(na_pos='last'), ?.P_TYPE.ASC(na_pos='last'), ?.P_SIZE.ASC(na_pos='last')))",
            id="tpch_q16",
        ),
        pytest.param(
            impl_tpch_q17,
            "?.TPCH(AVG_YEARLY=(SUM(?.Parts.WHERE(((?.brand == 'Brand#23') & (?.container == 'MED BOX')))(avg_quantity=AVG(?.lines.quantity)).lines.WHERE((?.quantity < (0.2 * BACK(1).avg_quantity))).extended_price) / 7.0))",
            id="tpch_q17",
        ),
        pytest.param(
            impl_tpch_q18,
            "?.Orders(C_NAME=?.customer.name, C_CUSTKEY=?.customer.key, O_ORDERKEY=?.key, O_ORDERDATE=?.order_date, O_TOTALPRICE=?.total_price, TOTAL_QUANTITY=SUM(?.lines.quantity)).WHERE((?.TOTAL_QUANTITY > 300)).TOP_K(10, by=(?.O_TOTALPRICE.DESC(na_pos='last'), ?.O_ORDERDATE.ASC(na_pos='last')))",
            id="tpch_q18",
        ),
        pytest.param(
            impl_tpch_q19,
            "?.TPCH(REVENUE=SUM((?.Lineitems.WHERE((((ISIN(?.ship_mode, ['AIR', 'AIR REG']) & (?.ship_instruct == 'DELIVER IN PERSON')) & (?.part.size >= 1)) & (((((((?.part.size <= 5) & (?.quantity >= 1)) & (?.quantity <= 11)) & ISIN(?.part.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (?.part.brand == 'Brand#12')) | (((((?.part.size <= 10) & (?.quantity >= 10)) & (?.quantity <= 20)) & ISIN(?.part.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'])) & (?.part.brand == 'Brand#23'))) | (((((?.part.size <= 15) & (?.quantity >= 20)) & (?.quantity <= 30)) & ISIN(?.part.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (?.part.brand == 'Brand#34'))))).extended_price * (1 - ?.Lineitems.WHERE((((ISIN(?.ship_mode, ['AIR', 'AIR REG']) & (?.ship_instruct == 'DELIVER IN PERSON')) & (?.part.size >= 1)) & (((((((?.part.size <= 5) & (?.quantity >= 1)) & (?.quantity <= 11)) & ISIN(?.part.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (?.part.brand == 'Brand#12')) | (((((?.part.size <= 10) & (?.quantity >= 10)) & (?.quantity <= 20)) & ISIN(?.part.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'])) & (?.part.brand == 'Brand#23'))) | (((((?.part.size <= 15) & (?.quantity >= 20)) & (?.quantity <= 30)) & ISIN(?.part.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (?.part.brand == 'Brand#34'))))).discount))))",
            id="tpch_q19",
        ),
        pytest.param(
            impl_tpch_q20,
            "?.Suppliers(S_NAME=?.name, S_ADDRESS=?.address).WHERE((((?.nation.name == 'CANADA') & COUNT(?.supply_records.part.WHERE((STARTSWITH(?.name, 'forest') & (BACK(1).availqty > (SUM(?.lines.WHERE(((?.ship_date >= datetime.date(1994, 1, 1)) & (?.ship_date < datetime.date(1995, 1, 1)))).quantity) * 0.5)))))) > 0)).TOP_K(10, by=(?.S_NAME.ASC(na_pos='last')))",
            id="tpch_q20",
        ),
        pytest.param(
            impl_tpch_q21,
            "?.Suppliers.WHERE((?.nation.name == 'SAUDI ARABIA'))(S_NAME=?.name, NUMWAIT=COUNT(?.lines.WHERE((?.receipt_date > ?.commit_date)).order.WHERE((((?.order_status == 'F') & HAS(?.lines.WHERE((?.supplier_key != BACK(2).supplier_key)))) & HASNOT(?.lines.WHERE(((?.supplier_key != BACK(2).supplier_key) & (?.receipt_date > ?.commit_date)))))))).TOP_K(10, by=(?.NUMWAIT.DESC(na_pos='last'), ?.S_NAME.ASC(na_pos='last')))",
            id="tpch_q21",
        ),
        pytest.param(
            impl_tpch_q22,
            "?.TPCH(avg_balance=AVG(?.Customers(cntry_code=SLICE(?.phone, None, 2, None)).WHERE((ISIN(?.cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT(?.orders))).WHERE((?.acctbal > 0.0)).acctbal)).PARTITION(?.Customers(cntry_code=SLICE(?.phone, None, 2, None)).WHERE((ISIN(?.cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & HASNOT(?.orders))).WHERE((?.acctbal > BACK(1).avg_balance)), name='custs', by=(?.cntry_code))(CNTRY_CODE=?.cntry_code, NUM_CUSTS=COUNT(?.custs), TOTACCTBAL=SUM(?.custs.acctbal))",
            id="tpch_q22",
        ),
    ],
)
def test_init_pydough_context(
    func: Callable[[], UnqualifiedNode],
    as_string: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Tests that the `init_pydough_context` decorator correctly works on several
    PyDough functions, transforming them into the correct unqualified nodes,
    at least based on string representation.
    """
    sample_graph: GraphMetadata = get_sample_graph("TPCH")
    new_func: Callable[[], UnqualifiedNode] = init_pydough_context(sample_graph)(func)
    answer: UnqualifiedNode = new_func()
    assert (
        repr(answer) == as_string
    ), "Mismatch between string representation of unqualified nodes and expected output"
    assert (
        pydough.display_raw(answer) == as_string
    ), "Mismatch between string representation of unqualified nodes and expected output"
