"""
Unit tests the PyDough unqualified nodes and the logic that transforms raw
Python code into them.
"""

import ast
import datetime
import re
from collections.abc import Callable

import pytest

import pydough
from pydough import init_pydough_context
from pydough.metadata import GraphMetadata
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    transform_code,
)
from tests.test_pydough_functions.simple_pydough_functions import (
    abs_round_magic_method,
    annotated_assignment,
    args_kwargs,
    class_handling,
    dict_comp_terms,
    exception_handling,
    function_defined_terms,
    function_defined_terms_with_duplicate_names,
    generator_comp_terms,
    lambda_defined_terms,
    list_comp_terms,
    loop_generated_terms,
    nested_unpacking,
    set_comp_terms,
    unpacking,
    unpacking_in_iterable,
    with_import_statement,
)
from tests.test_pydough_functions.tpch_test_functions import (
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
from tests.testing_utilities import graph_fetcher

from .test_pydough_functions.bad_pydough_functions import (
    bad_bool_1,
    bad_bool_2,
    bad_bool_3,
    bad_ceil,
    bad_complex,
    bad_contains,
    bad_float,
    bad_floor,
    bad_index,
    bad_int,
    bad_iter,
    bad_len,
    bad_nonzero,
    bad_reversed,
    bad_setitem,
    bad_trunc,
    bad_unsupported_kwarg1,
    bad_unsupported_kwarg2,
    bad_unsupported_kwarg3,
    bad_window_1,
    bad_window_2,
    bad_window_3,
    bad_window_4,
    bad_window_5,
    bad_window_6,
    bad_window_7,
    bad_window_8,
    bad_window_9,
    bad_window_10,
)


@pytest.fixture
def global_ctx() -> dict[str, object]:
    """
    A fresh global variable context including the various global variables
    accessible within one of the unqualified node tests, including PyDough
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
    assert isinstance(answer, UnqualifiedNode), (
        "Expected `pydough_str` to define `answer` as an UnqualifiedNode."
    )
    assert repr(answer) == expected_str, (
        "Mismatch between string representation of `answer` and expected value."
    )
    assert pydough.display_raw(answer) == expected_str, (
        "Mismatch between string representation of `answer` and expected value."
    )


@pytest.mark.parametrize(
    "pydough_str, answer_str",
    [
        pytest.param(
            "answer = _ROOT.parts",
            "parts",
            id="access_collection",
        ),
        pytest.param(
            "answer = _ROOT.regions.nations",
            "regions.nations",
            id="access_subcollection",
        ),
        pytest.param(
            "answer = _ROOT.regions.name",
            "regions.name",
            id="access_property",
        ),
        pytest.param(
            "answer = _ROOT.regions.CALCULATE(region_name=_ROOT.name, region_key=_ROOT.key)",
            "regions.CALCULATE(region_name=name, region_key=key)",
            id="simple_calc",
        ),
        pytest.param(
            "answer = _ROOT.nations.CALCULATE(nation_name=_ROOT.UPPER(_ROOT.name), total_balance=_ROOT.SUM(_ROOT.customers.acct_bal))",
            "nations.CALCULATE(nation_name=UPPER(name), total_balance=SUM(customers.acct_bal))",
            id="calc_with_functions",
        ),
        pytest.param(
            "answer = _ROOT.x + 1",
            "(x + 1)",
            id="arithmetic_01",
        ),
        pytest.param(
            "answer = 2 + _ROOT.x",
            "(2 + x)",
            id="arithmetic_02",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5 * x) - 1)",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = ((1.5 * _ROOT.x) - 1)",
            "((1.5 * x) - 1)",
            id="arithmetic_03",
        ),
        pytest.param(
            "answer = (_ROOT.STARTSWITH(_ROOT.x, 'hello') | _ROOT.ENDSWITH(_ROOT.x, 'world')) & _ROOT.CONTAINS(_ROOT.x, ' ')",
            "((STARTSWITH(x, 'hello') | ENDSWITH(x, 'world')) & CONTAINS(x, ' '))",
            id="arithmetic_04",
        ),
        pytest.param(
            "answer = (1 / _ROOT.x) ** 2 - _ROOT.y",
            "(((1 / x) ** 2) - y)",
            id="arithmetic_05",
        ),
        pytest.param(
            "answer = -(_ROOT.x % 10) / 3.1415",
            "((0 - (x % 10)) / 3.1415)",
            id="arithmetic_06",
        ),
        pytest.param(
            "answer = (+_ROOT.x < -_ROOT.y) ^ (_ROOT.y == _ROOT.z)",
            "((x < (0 - y)) ^ (y == z))",
            id="arithmetic_07",
        ),
        pytest.param(
            "answer = 'Hello' != _ROOT.word",
            "(word != 'Hello')",
            id="arithmetic_08",
        ),
        pytest.param(
            "answer = _ROOT.order_date >= datetime.date(2020, 1, 1)",
            "(order_date >= datetime.date(2020, 1, 1))",
            id="arithmetic_09",
        ),
        pytest.param(
            "answer = True & (0 >= _ROOT.x)",
            "(True & (x <= 0))",
            id="arithmetic_10",
        ),
        pytest.param(
            "answer = (_ROOT.x == 42) | (45 == _ROOT.x) | ((_ROOT.x < 16) & (_ROOT.x != 0)) | ((100 < _ROOT.x) ^ (0 == _ROOT.y))",
            "((((x == 42) | (x == 45)) | ((x < 16) & (x != 0))) | ((x > 100) ^ (y == 0)))",
            id="arithmetic_11",
        ),
        pytest.param(
            "answer = False ^ 100 % 2.718281828 ** _ROOT.x",
            "(False ^ (100 % (2.718281828 ** x)))",
            id="arithmetic_12",
        ),
        pytest.param(
            "answer = _ROOT.parts.CALCULATE(part_name=_ROOT.LOWER(_ROOT.name)).suppliers_of_part.region.CALCULATE(part_name=_ROOT.part_name)",
            "parts.CALCULATE(part_name=LOWER(name)).suppliers_of_part.region.CALCULATE(part_name=part_name)",
            id="multi_calc_with_back",
        ),
        pytest.param(
            """\
x = _ROOT.parts.CALCULATE(part_name=_ROOT.LOWER(_ROOT.name))
y = x.WHERE(_ROOT.STARTSWITH(_ROOT.part_name, 'a'))
answer = y.ORDER_BY(_ROOT.retail_price.DESC())\
""",
            "parts.CALCULATE(part_name=LOWER(name)).WHERE(STARTSWITH(part_name, 'a')).ORDER_BY(retail_price.DESC(na_pos='last'))",
            id="calc_with_where_order",
        ),
        pytest.param(
            "answer = _ROOT.parts.TOP_K(10, by=(1 / (_ROOT.retail_price - 30.0)).ASC(na_pos='last'))",
            "parts.TOP_K(10, by=((1 / (retail_price - 30.0)).ASC(na_pos='last')))",
            id="topk_single",
        ),
        pytest.param(
            "answer = _ROOT.parts.TOP_K(10, by=(_ROOT.size.DESC(), _ROOT.part_type.DESC()))",
            "parts.TOP_K(10, by=(size.DESC(na_pos='last'), part_type.DESC(na_pos='last')))",
            id="topk_multiple",
        ),
        pytest.param(
            """\
x = _ROOT.parts.ORDER_BY(_ROOT.retail_price.ASC(na_pos='first'))
answer = x.TOP_K(100)\
""",
            "parts.ORDER_BY(retail_price.ASC(na_pos='first')).TOP_K(100)",
            id="order_topk_empty",
        ),
        pytest.param(
            "answer = _ROOT.parts.CALCULATE(_ROOT.name, rank=_ROOT.RANKING(by=_ROOT.retail_price.DESC()))",
            "parts.CALCULATE(name=name, rank=RANKING(by=(retail_price.DESC(na_pos='last')))",
            id="ranking_1",
        ),
        pytest.param(
            "answer = _ROOT.parts.CALCULATE(_ROOT.name, rank=_ROOT.RANKING(by=_ROOT.retail_price.DESC(), per='A'))",
            "parts.CALCULATE(name=name, rank=RANKING(by=(retail_price.DESC(na_pos='last'), per='A'))",
            id="ranking_2",
        ),
        pytest.param(
            "answer = _ROOT.parts.CALCULATE(_ROOT.name, rank=_ROOT.RANKING(by=_ROOT.retail_price.DESC(), allow_ties=True))",
            "parts.CALCULATE(name=name, rank=RANKING(by=(retail_price.DESC(na_pos='last'), allow_ties=True))",
            id="ranking_3",
        ),
        pytest.param(
            "answer = _ROOT.parts.CALCULATE(_ROOT.name, rank=_ROOT.RANKING(by=_ROOT.retail_price.DESC(), per='B', allow_ties=True, dense=True))",
            "parts.CALCULATE(name=name, rank=RANKING(by=(retail_price.DESC(na_pos='last'), per='B', allow_ties=True, dense=True))",
            id="ranking_4",
        ),
        pytest.param(
            "answer = _ROOT.nations.CALCULATE(name=_ROOT.name, num_customers=_ROOT.COUNT(_ROOT.customers)).BEST(by=_ROOT.num_customers.DESC())",
            "nations.CALCULATE(name=name, num_customers=COUNT(customers)).BEST(by=(num_customers.DESC(na_pos='last')))",
            id="best_global",
        ),
        pytest.param(
            "answer = _ROOT.nations.CALCULATE(nation_name=_ROOT.name).suppliers.BEST(per='nations', by=_ROOT.account_balance.DESC()).CALCULATE(nation_name=_ROOT.nation_name, supplier_name=_ROOT.name, supplier_balance=_ROOT.account_balance)",
            "nations.CALCULATE(nation_name=name).suppliers.BEST(by=(account_balance.DESC(na_pos='last')), per=False).CALCULATE(nation_name=nation_name, supplier_name=name, supplier_balance=account_balance)",
            id="best_access",
        ),
        pytest.param(
            """\
richest_customer = _ROOT.customers.BEST(per='nations', by=_ROOT.acct_bal.DESC())
answer = _ROOT.nations.CALCULATE(name=_ROOT.name, richest_customer_name=richest_customer.name)
""",
            "nations.CALCULATE(name=name, richest_customer_name=customers.BEST(by=(acct_bal.DESC(na_pos='last')), per=False).name)",
            id="best_child",
        ),
        pytest.param(
            "answer = _ROOT.customers.orders.line.BEST(per='customers', by=_ROOT.ship_date.DESC(), allow_ties=True)",
            "customers.orders.line.BEST(by=(ship_date.DESC(na_pos='last')), per=True, allow_ties=True)",
            id="best_ties",
        ),
        pytest.param(
            "answer = _ROOT.customers.orders.lines.BEST(per='customers', by=_ROOT.ship_date.DESC(), n_best=5)",
            "customers.orders.lines.BEST(by=(ship_date.DESC(na_pos='last')), per=False, n_best=5)",
            id="best_multiple",
        ),
        pytest.param(
            "answer = _ROOT.regions.CROSS(_ROOT.regions)",
            "regions.CROSS(regions)",
            id="cross_simple",
        ),
        pytest.param(
            "answer = _ROOT.customers.CROSS(_ROOT.customers.orders.WHERE(_ROOT.order_priority == '1-URGENT')).TOP_K(5, by=(_ROOT.key.DESC()))",
            "customers.CROSS(customers.orders.WHERE((order_priority == '1-URGENT'))).TOP_K(5, by=(key.DESC(na_pos='last')))",
            id="cross_filter",
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
            "lines.WHERE((ship_date <= datetime.date(1998, 12, 1))).PARTITION(name='groups', by=(return_flag, status)).CALCULATE(L_RETURNFLAG=return_flag, L_LINESTATUS=status, SUM_QTY=SUM(lines.quantity), SUM_BASE_PRICE=SUM(lines.extended_price), SUM_DISC_PRICE=SUM((lines.extended_price * (1 - lines.discount))), SUM_CHARGE=SUM(((lines.extended_price * (1 - lines.discount)) * (1 + lines.tax))), AVG_QTY=AVG(lines.quantity), AVG_PRICE=AVG(lines.extended_price), AVG_DISC=AVG(lines.discount), COUNT_ORDER=COUNT(lines)).ORDER_BY(L_RETURNFLAG.ASC(na_pos='first'), L_LINESTATUS.ASC(na_pos='first'))",
            id="tpch_q1",
        ),
        pytest.param(
            impl_tpch_q2,
            "parts.WHERE((ENDSWITH(part_type, 'BRASS') & (size == 15))).CALCULATE(P_PARTKEY=key, P_MFGR=manufacturer).supply_records.WHERE((supplier.nation.region.name == 'EUROPE')).BEST(by=(supply_cost.ASC(na_pos='first')), per=True, allow_ties=True).CALCULATE(S_ACCTBAL=supplier.account_balance, S_NAME=supplier.name, N_NAME=supplier.nation.name, P_PARTKEY=P_PARTKEY, P_MFGR=P_MFGR, S_ADDRESS=supplier.address, S_PHONE=supplier.phone, S_COMMENT=supplier.comment).TOP_K(10, by=(S_ACCTBAL.DESC(na_pos='last'), N_NAME.ASC(na_pos='first'), S_NAME.ASC(na_pos='first'), P_PARTKEY.ASC(na_pos='first')))",
            id="tpch_q2",
        ),
        pytest.param(
            impl_tpch_q3,
            "orders.CALCULATE(order_date=order_date, ship_priority=ship_priority).WHERE(((customer.market_segment == 'BUILDING') & (order_date < datetime.date(1995, 3, 15)))).lines.WHERE((ship_date > datetime.date(1995, 3, 15))).PARTITION(name='groups', by=(order_key, order_date, ship_priority)).CALCULATE(L_ORDERKEY=order_key, REVENUE=SUM((lines.extended_price * (1 - lines.discount))), O_ORDERDATE=order_date, O_SHIPPRIORITY=ship_priority).TOP_K(10, by=(REVENUE.DESC(na_pos='last'), O_ORDERDATE.ASC(na_pos='first'), L_ORDERKEY.ASC(na_pos='first')))",
            id="tpch_q3",
        ),
        pytest.param(
            impl_tpch_q4,
            "orders.WHERE((((YEAR(order_date) == 1993) & (QUARTER(order_date) == 3)) & HAS(lines.WHERE((commit_date < receipt_date))))).PARTITION(name='priorities', by=(order_priority)).CALCULATE(O_ORDERPRIORITY=order_priority, ORDER_COUNT=COUNT(orders)).ORDER_BY(O_ORDERPRIORITY.ASC(na_pos='first'))",
            id="tpch_q4",
        ),
        pytest.param(
            impl_tpch_q5,
            "nations.CALCULATE(nation_name=name).WHERE((region.name == 'ASIA')).WHERE(HAS(customers.orders.WHERE(((order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1)))).lines.WHERE((supplier.nation.name == nation_name)).CALCULATE(value=(extended_price * (1 - discount))))).CALCULATE(N_NAME=name, REVENUE=SUM(customers.orders.WHERE(((order_date >= datetime.date(1994, 1, 1)) & (order_date < datetime.date(1995, 1, 1)))).lines.WHERE((supplier.nation.name == nation_name)).CALCULATE(value=(extended_price * (1 - discount))).value)).ORDER_BY(REVENUE.DESC(na_pos='last'))",
            id="tpch_q5",
        ),
        pytest.param(
            impl_tpch_q6,
            "TPCH.CALCULATE(REVENUE=SUM(lines.WHERE((((((ship_date >= datetime.date(1994, 1, 1)) & (ship_date < datetime.date(1995, 1, 1))) & (discount >= 0.05)) & (discount <= 0.07)) & (quantity < 24))).CALCULATE(amt=(extended_price * discount)).amt))",
            id="tpch_q6",
        ),
        pytest.param(
            impl_tpch_q7,
            "lines.CALCULATE(supp_nation=supplier.nation.name, cust_nation=order.customer.nation.name, l_year=YEAR(ship_date), volume=(extended_price * (1 - discount))).WHERE((ISIN(YEAR(ship_date), [1995, 1996]) & (((supp_nation == 'FRANCE') & (cust_nation == 'GERMANY')) | ((supp_nation == 'GERMANY') & (cust_nation == 'FRANCE'))))).PARTITION(name='groups', by=(supp_nation, cust_nation, l_year)).CALCULATE(SUPP_NATION=supp_nation, CUST_NATION=cust_nation, L_YEAR=l_year, REVENUE=SUM(lines.volume)).ORDER_BY(SUPP_NATION.ASC(na_pos='first'), CUST_NATION.ASC(na_pos='first'), L_YEAR.ASC(na_pos='first'))",
            id="tpch_q7",
        ),
        pytest.param(
            impl_tpch_q8,
            "lines.WHERE((((part.part_type == 'ECONOMY ANODIZED STEEL') & ISIN(YEAR(order.order_date), [1995, 1996])) & (order.customer.nation.region.name == 'AMERICA'))).CALCULATE(O_YEAR=YEAR(order.order_date), volume=(extended_price * (1 - discount)), brazil_volume=IFF((supplier.nation.name == 'BRAZIL'), (extended_price * (1 - discount)), 0)).PARTITION(name='years', by=(O_YEAR)).CALCULATE(O_YEAR=O_YEAR, MKT_SHARE=(SUM(lines.brazil_volume) / SUM(lines.volume)))",
            id="tpch_q8",
        ),
        pytest.param(
            impl_tpch_q9,
            "lines.WHERE(CONTAINS(part.name, 'green')).CALCULATE(nation_name=supplier.nation.name, o_year=YEAR(order.order_date), value=((extended_price * (1 - discount)) - (part_and_supplier.supply_cost * quantity))).PARTITION(name='groups', by=(nation_name, o_year)).CALCULATE(NATION=nation_name, O_YEAR=o_year, AMOUNT=SUM(lines.value)).TOP_K(10, by=(NATION.ASC(na_pos='first'), O_YEAR.DESC(na_pos='last')))",
            id="tpch_q9",
        ),
        pytest.param(
            impl_tpch_q10,
            "customers.CALCULATE(C_CUSTKEY=key, C_NAME=name, REVENUE=SUM((orders.WHERE(((YEAR(order_date) == 1993) & (QUARTER(order_date) == 4))).lines.WHERE((return_flag == 'R')).extended_price * (1 - orders.WHERE(((YEAR(order_date) == 1993) & (QUARTER(order_date) == 4))).lines.WHERE((return_flag == 'R')).discount))), C_ACCTBAL=account_balance, N_NAME=nation.name, C_ADDRESS=address, C_PHONE=phone, C_COMMENT=comment).TOP_K(20, by=(REVENUE.DESC(na_pos='last'), C_CUSTKEY.ASC(na_pos='first')))",
            id="tpch_q10",
        ),
        pytest.param(
            impl_tpch_q11,
            "TPCH.CALCULATE(min_market_share=(SUM(supply_records.WHERE((supplier.nation.name == 'GERMANY')).CALCULATE(metric=(supply_cost * available_quantity)).metric) * 0.0001)).supply_records.WHERE((supplier.nation.name == 'GERMANY')).PARTITION(name='parts', by=(part_key)).CALCULATE(PS_PARTKEY=part_key, VALUE=SUM((supply_records.supply_cost * supply_records.available_quantity))).WHERE((VALUE > min_market_share)).TOP_K(10, by=(VALUE.DESC(na_pos='last')))",
            id="tpch_q11",
        ),
        pytest.param(
            impl_tpch_q12,
            "lines.WHERE((((((ship_mode == 'MAIL') | (ship_mode == 'SHIP')) & (ship_date < commit_date)) & (commit_date < receipt_date)) & (YEAR(receipt_date) == 1994))).CALCULATE(is_high_priority=ISIN(order.order_priority, ['1-URGENT', '2-HIGH'])).PARTITION(name='modes', by=(ship_mode)).CALCULATE(L_SHIPMODE=ship_mode, HIGH_LINE_COUNT=SUM(lines.is_high_priority), LOW_LINE_COUNT=SUM(NOT(lines.is_high_priority))).ORDER_BY(L_SHIPMODE.ASC(na_pos='first'))",
            id="tpch_q12",
        ),
        pytest.param(
            impl_tpch_q13,
            "customers.CALCULATE(num_non_special_orders=COUNT(orders.WHERE(NOT(LIKE(comment, '%special%requests%'))))).PARTITION(name='num_order_groups', by=(num_non_special_orders)).CALCULATE(C_COUNT=num_non_special_orders, CUSTDIST=COUNT(customers)).TOP_K(10, by=(CUSTDIST.DESC(na_pos='last'), C_COUNT.DESC(na_pos='last')))",
            id="tpch_q13",
        ),
        pytest.param(
            impl_tpch_q14,
            "TPCH.CALCULATE(PROMO_REVENUE=((100.0 * SUM(lines.WHERE(((YEAR(ship_date) == 1995) & (MONTH(ship_date) == 9))).CALCULATE(value=(extended_price * (1 - discount)), promo_value=IFF(STARTSWITH(part.part_type, 'PROMO'), (extended_price * (1 - discount)), 0)).promo_value)) / SUM(lines.WHERE(((YEAR(ship_date) == 1995) & (MONTH(ship_date) == 9))).CALCULATE(value=(extended_price * (1 - discount)), promo_value=IFF(STARTSWITH(part.part_type, 'PROMO'), (extended_price * (1 - discount)), 0)).value)))",
            id="tpch_q14",
        ),
        pytest.param(
            impl_tpch_q15,
            "TPCH.CALCULATE(max_revenue=MAX(suppliers.WHERE(HAS(lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))))).CALCULATE(total_revenue=SUM((lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).extended_price * (1 - lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).discount)))).total_revenue)).suppliers.WHERE((HAS(lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1))))) & (SUM((lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).extended_price * (1 - lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).discount))) == max_revenue))).CALCULATE(S_SUPPKEY=key, S_NAME=name, S_ADDRESS=address, S_PHONE=phone, TOTAL_REVENUE=SUM((lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).extended_price * (1 - lines.WHERE(((ship_date >= datetime.date(1996, 1, 1)) & (ship_date < datetime.date(1996, 4, 1)))).discount)))).ORDER_BY(S_SUPPKEY.ASC(na_pos='first'))",
            id="tpch_q15",
        ),
        pytest.param(
            impl_tpch_q16,
            "supply_records.WHERE((NOT(LIKE(supplier.comment, '%Customer%Complaints%')) & HAS(part.WHERE((((brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%'))) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9])))))).CALCULATE(P_BRAND=part.WHERE((((brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%'))) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9]))).brand, P_TYPE=part.WHERE((((brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%'))) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9]))).part_type, P_SIZE=part.WHERE((((brand != 'BRAND#45') & NOT(STARTSWITH(part_type, 'MEDIUM POLISHED%'))) & ISIN(size, [49, 14, 23, 45, 19, 3, 36, 9]))).size).PARTITION(name='groups', by=(P_BRAND, P_TYPE, P_SIZE)).CALCULATE(P_BRAND=P_BRAND, P_TYPE=P_TYPE, P_SIZE=P_SIZE, SUPPLIER_COUNT=NDISTINCT(supply_records.supplier_key)).TOP_K(10, by=(SUPPLIER_COUNT.DESC(na_pos='last'), P_BRAND.ASC(na_pos='first'), P_TYPE.ASC(na_pos='first'), P_SIZE.ASC(na_pos='first')))",
            id="tpch_q16",
        ),
        pytest.param(
            impl_tpch_q17,
            "TPCH.CALCULATE(AVG_YEARLY=(SUM(parts.WHERE(((brand == 'Brand#23') & (container == 'MED BOX'))).lines.WHERE((quantity < (0.2 * RELAVG(quantity, by=(, per='parts')))).extended_price) / 7.0))",
            id="tpch_q17",
        ),
        pytest.param(
            impl_tpch_q18,
            "orders.CALCULATE(C_NAME=customer.name, C_CUSTKEY=customer.key, O_ORDERKEY=key, O_ORDERDATE=order_date, O_TOTALPRICE=total_price, TOTAL_QUANTITY=SUM(lines.quantity)).WHERE((TOTAL_QUANTITY > 300)).TOP_K(10, by=(O_TOTALPRICE.DESC(na_pos='last'), O_ORDERDATE.ASC(na_pos='first')))",
            id="tpch_q18",
        ),
        pytest.param(
            impl_tpch_q19,
            "TPCH.CALCULATE(REVENUE=SUM((lines.WHERE(((ISIN(ship_mode, ['AIR', 'AIR REG']) & (ship_instruct == 'DELIVER IN PERSON')) & (((((MONOTONIC(1, part.size, 5) & MONOTONIC(1, quantity, 11)) & ISIN(part.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (part.brand == 'Brand#12')) | (((MONOTONIC(1, part.size, 10) & MONOTONIC(10, quantity, 20)) & ISIN(part.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'])) & (part.brand == 'Brand#23'))) | (((MONOTONIC(1, part.size, 15) & MONOTONIC(20, quantity, 30)) & ISIN(part.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (part.brand == 'Brand#34'))))).extended_price * (1 - lines.WHERE(((ISIN(ship_mode, ['AIR', 'AIR REG']) & (ship_instruct == 'DELIVER IN PERSON')) & (((((MONOTONIC(1, part.size, 5) & MONOTONIC(1, quantity, 11)) & ISIN(part.container, ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'])) & (part.brand == 'Brand#12')) | (((MONOTONIC(1, part.size, 10) & MONOTONIC(10, quantity, 20)) & ISIN(part.container, ['MED BAG', 'MED BOX', 'MED PACK', 'MED PKG'])) & (part.brand == 'Brand#23'))) | (((MONOTONIC(1, part.size, 15) & MONOTONIC(20, quantity, 30)) & ISIN(part.container, ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'])) & (part.brand == 'Brand#34'))))).discount))))",
            id="tpch_q19",
        ),
        pytest.param(
            impl_tpch_q20,
            "suppliers.CALCULATE(S_NAME=name, S_ADDRESS=address).WHERE((((nation.name == 'CANADA') & (COUNT(supply_records.WHERE((HAS(part.WHERE((STARTSWITH(name, 'forest') & HAS(lines.WHERE((YEAR(ship_date) == 1994))))).CALCULATE(part_qty=SUM(lines.WHERE((YEAR(ship_date) == 1994)).quantity))) & (available_quantity > (0.5 * SUM(part.WHERE((STARTSWITH(name, 'forest') & HAS(lines.WHERE((YEAR(ship_date) == 1994))))).CALCULATE(part_qty=SUM(lines.WHERE((YEAR(ship_date) == 1994)).quantity)).part_qty)))))) > 0)) & HAS(supply_records.WHERE((HAS(part.WHERE((STARTSWITH(name, 'forest') & HAS(lines.WHERE((YEAR(ship_date) == 1994))))).CALCULATE(part_qty=SUM(lines.WHERE((YEAR(ship_date) == 1994)).quantity))) & (available_quantity > (0.5 * SUM(part.WHERE((STARTSWITH(name, 'forest') & HAS(lines.WHERE((YEAR(ship_date) == 1994))))).CALCULATE(part_qty=SUM(lines.WHERE((YEAR(ship_date) == 1994)).quantity)).part_qty)))))))).TOP_K(10, by=(S_NAME.ASC(na_pos='first')))",
            id="tpch_q20",
        ),
        pytest.param(
            impl_tpch_q21,
            "suppliers.WHERE((nation.name == 'SAUDI ARABIA')).CALCULATE(S_NAME=name, NUMWAIT=COUNT(lines.CALCULATE(original_key=supplier_key).WHERE((receipt_date > commit_date)).order.WHERE((((order_status == 'F') & HAS(lines.WHERE((supplier_key != original_key)))) & HASNOT(lines.WHERE(((supplier_key != original_key) & (receipt_date > commit_date)))))))).TOP_K(10, by=(NUMWAIT.DESC(na_pos='last'), S_NAME.ASC(na_pos='first')))",
            id="tpch_q21",
        ),
        pytest.param(
            impl_tpch_q22,
            "TPCH.CALCULATE(global_avg_balance=AVG(customers.CALCULATE(cntry_code=SLICE(phone, None, 2, None)).WHERE(ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17'])).WHERE((account_balance > 0.0)).account_balance)).customers.CALCULATE(cntry_code=SLICE(phone, None, 2, None)).WHERE(((ISIN(cntry_code, ['13', '31', '23', '29', '30', '18', '17']) & (account_balance > global_avg_balance)) & (COUNT(orders) == 0))).PARTITION(name='countries', by=(cntry_code)).CALCULATE(CNTRY_CODE=cntry_code, NUM_CUSTS=COUNT(customers), TOTACCTBAL=SUM(customers.account_balance)).ORDER_BY(CNTRY_CODE.ASC(na_pos='first'))",
            id="tpch_q22",
        ),
        pytest.param(
            loop_generated_terms,
            "nations.CALCULATE(name=name, interval_0=COUNT(customers.WHERE(MONOTONIC(0, account_balance, 1000))), interval_1=COUNT(customers.WHERE(MONOTONIC(1000, account_balance, 2000))), interval_2=COUNT(customers.WHERE(MONOTONIC(2000, account_balance, 3000))))",
            id="loop_generated_terms",
        ),
        pytest.param(
            function_defined_terms,
            "nations.CALCULATE(name=name, interval_7=COUNT(customers.WHERE(MONOTONIC(7000, account_balance, 8000))), interval_4=COUNT(customers.WHERE(MONOTONIC(4000, account_balance, 5000))), interval_13=COUNT(customers.WHERE(MONOTONIC(13000, account_balance, 14000))))",
            id="function_defined_terms",
        ),
        pytest.param(
            function_defined_terms_with_duplicate_names,
            "nations.CALCULATE(name=name, redefined_name=name, interval_7=COUNT(customers.WHERE(MONOTONIC(7000, account_balance, 8000))), interval_4=COUNT(customers.WHERE(MONOTONIC(4000, account_balance, 5000))), interval_13=COUNT(customers.WHERE(MONOTONIC(13000, account_balance, 14000))))",
            id="function_defined_terms_with_duplicate_names",
        ),
        pytest.param(
            lambda_defined_terms,
            "nations.CALCULATE(name=name, interval_7=COUNT(customers.WHERE(MONOTONIC(7000, account_balance, 8000))), interval_4=COUNT(customers.WHERE(MONOTONIC(4000, account_balance, 5000))), interval_13=COUNT(customers.WHERE(MONOTONIC(13000, account_balance, 14000))))",
            id="lambda_defined_terms",
        ),
        pytest.param(
            dict_comp_terms,
            "nations.CALCULATE(name=name, interval_0=COUNT(customers.WHERE(MONOTONIC(0, account_balance, 1000))), interval_1=COUNT(customers.WHERE(MONOTONIC(1000, account_balance, 2000))), interval_2=COUNT(customers.WHERE(MONOTONIC(2000, account_balance, 3000))))",
            id="dict_comp_terms",
        ),
        pytest.param(
            list_comp_terms,
            "nations.CALCULATE(name=name, _expr0=COUNT(customers.WHERE(MONOTONIC(0, account_balance, 1000))), _expr1=COUNT(customers.WHERE(MONOTONIC(1000, account_balance, 2000))), _expr2=COUNT(customers.WHERE(MONOTONIC(2000, account_balance, 3000))))",
            id="list_comp_terms",
        ),
        pytest.param(
            set_comp_terms,
            "nations.CALCULATE(_expr0=COUNT(customers.WHERE(MONOTONIC(0, account_balance, 1000))), _expr1=COUNT(customers.WHERE(MONOTONIC(1000, account_balance, 2000))), _expr2=COUNT(customers.WHERE(MONOTONIC(2000, account_balance, 3000))), name=name)",
            id="set_comp_terms",
        ),
        pytest.param(
            generator_comp_terms,
            "nations.CALCULATE(name=name, interval_0=COUNT(customers.WHERE(MONOTONIC(0, account_balance, 1000))), interval_1=COUNT(customers.WHERE(MONOTONIC(1000, account_balance, 2000))), interval_2=COUNT(customers.WHERE(MONOTONIC(2000, account_balance, 3000))))",
            id="generator_comp_terms",
        ),
        pytest.param(
            args_kwargs,
            "TPCH.CALCULATE(n_tomato=COUNT(parts.WHERE(CONTAINS(part_name, 'tomato'))), n_almond=COUNT(parts.WHERE(CONTAINS(part_name, 'almond'))), small=COUNT(parts.WHERE(True)), large=COUNT(parts.WHERE(True)))",
            id="args_kwargs",
        ),
        pytest.param(
            unpacking,
            "orders.WHERE(MONOTONIC(1992, YEAR(order_date), 1994))",
            id="unpacking",
        ),
        pytest.param(
            nested_unpacking,
            "customers.WHERE(ISIN(nation.name, ['GERMANY', 'FRANCE', 'ARGENTINA']))",
            id="nested_unpacking",
        ),
        pytest.param(
            unpacking_in_iterable,
            "nations.CALCULATE(c0=COUNT(orders.WHERE((YEAR(order_date) == 1992))), c1=COUNT(orders.WHERE((YEAR(order_date) == 1993))), c2=COUNT(orders.WHERE((YEAR(order_date) == 1994))), c3=COUNT(orders.WHERE((YEAR(order_date) == 1995))), c4=COUNT(orders.WHERE((YEAR(order_date) == 1996))))",
            id="unpacking_in_iterable",
        ),
        pytest.param(
            with_import_statement,
            "customers.WHERE(ISIN(nation.name, ['Canada', 'Mexico']))",
            id="with_import_statement",
        ),
        pytest.param(
            exception_handling,
            "customers.WHERE(ISIN(nation.name, ['Canada', 'Mexico']))",
            id="exception_handling",
        ),
        pytest.param(
            class_handling,
            "customers.WHERE(ISIN(nation.name, ['Canada', 'Mexico']))",
            id="class_handling",
        ),
        pytest.param(
            annotated_assignment,
            "nations.WHERE((region.name == 'SOUTH WEST AMERICA'))",
            id="annotated_assignment",
        ),
        pytest.param(
            abs_round_magic_method,
            "daily_prices.CALCULATE(abs_low=ABS(low), round_low=ROUND(low, 2), round_zero=ROUND(low, 0))",
            id="abs_round_magic_method",
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
    assert repr(answer) == as_string, (
        "Mismatch between string representation of unqualified nodes and expected output"
    )
    assert pydough.display_raw(answer) == as_string, (
        "Mismatch between string representation of unqualified nodes and expected output"
    )


@pytest.mark.parametrize(
    "func, error_msg",
    [
        pytest.param(
            bad_bool_1,
            "PyDough code cannot be treated as a boolean. If you intend to do a logical operation, use `|`, `&` or `~` instead of `or`, `and` and `not`.",
            id="bad_bool_1",
        ),
        pytest.param(
            bad_bool_2,
            "PyDough code cannot be treated as a boolean. If you intend to do a logical operation, use `|`, `&` or `~` instead of `or`, `and` and `not`.",
            id="bad_bool_2",
        ),
        pytest.param(
            bad_bool_3,
            "PyDough code cannot be treated as a boolean. If you intend to do a logical operation, use `|`, `&` or `~` instead of `or`, `and` and `not`.",
            id="bad_bool_3",
        ),
        pytest.param(
            bad_window_1,
            "The `by` argument to `RANKING` must be provided",
            id="bad_window_1",
        ),
        pytest.param(
            bad_window_2,
            "The `by` argument to `PERCENTILE` must be a single expression or a non-empty list/tuple of expressions. "
            "Please refer to the config documentation for more information.",
            id="bad_window_2",
        ),
        pytest.param(
            bad_window_3,
            "`n_buckets` argument must be a positive integer",
            id="bad_window_3",
        ),
        pytest.param(
            bad_window_4,
            "`n_buckets` argument must be a positive integer",
            id="bad_window_4",
        ),
        pytest.param(
            bad_window_5,
            "When the `by` argument to `RELSUM` is provided, either `cumulative=True` or the `frame` argument must be provided",
            id="bad_window_5",
        ),
        pytest.param(
            bad_window_6,
            "The `by` argument to `RELAVG` must be provided when the `cumulative` argument is True",
            id="bad_window_6",
        ),
        pytest.param(
            bad_window_7,
            "The `by` argument to `RELCOUNT` must be provided when the `frame` argument is provided",
            id="bad_window_7",
        ),
        pytest.param(
            bad_window_8,
            "The `cumulative` argument to `RELSIZE` cannot be used with the `frame` argument",
            id="bad_window_8",
        ),
        pytest.param(
            bad_window_9,
            re.escape(
                "Malformed `frame` argument to `RELSUM`: (-10.5, 0) (must be a tuple of two integers or None values)"
            ),
            id="bad_window_9",
        ),
        pytest.param(
            bad_window_10,
            re.escape(
                "Malformed `frame` argument to `RELSUM`: (0, -1) (lower bound must be less than or equal to upper bound)"
            ),
            id="bad_window_10",
        ),
        pytest.param(
            bad_floor,
            "PyDough does not support the math.floor function at this time.",
            id="bad_floor",
        ),
        pytest.param(
            bad_ceil,
            "PyDough does not support the math.ceil function at this time.",
            id="bad_ceil",
        ),
        pytest.param(
            bad_trunc,
            "PyDough does not support the math.trunc function at this time.",
            id="bad_trunc",
        ),
        pytest.param(
            bad_reversed,
            "PyDough does not support the reversed function at this time.",
            id="bad_reversed",
        ),
        pytest.param(
            bad_int,
            "PyDough objects cannot be cast to int.",
            id="bad_int",
        ),
        pytest.param(
            bad_float,
            "PyDough objects cannot be cast to float.",
            id="bad_float",
        ),
        pytest.param(
            bad_complex,
            "PyDough objects cannot be cast to complex.",
            id="bad_complex",
        ),
        pytest.param(
            bad_index,
            "PyDough objects cannot be used as indices in Python slices.",
            id="bad_index",
        ),
        pytest.param(
            bad_nonzero,
            "PyDough code cannot be treated as a boolean. If you intend to do a logical operation, use `|`, `&` and `~` instead of `or`, `and` and `not`.",
            id="bad_nonzero_1",
        ),
        pytest.param(
            bad_len,
            "PyDough objects cannot be used with the len function.",
            id="bad_len",
        ),
        pytest.param(
            bad_contains,
            "PyDough objects cannot be used with the 'in' operator.",
            id="bad_contains",
        ),
        pytest.param(
            bad_setitem,
            "PyDough objects cannot support item assignment.",
            id="bad_setitem",
        ),
        pytest.param(
            bad_iter,
            "Cannot index into PyDough object customers with 0",
            id="bad_iter",
        ),
        pytest.param(
            bad_unsupported_kwarg1,
            re.escape("No matching implementation found for VAR(type='wrong_type')."),
            id="bad_unsupported_kwarg1",
        ),
        pytest.param(
            bad_unsupported_kwarg2,
            re.escape("No matching implementation found for VAR(wrong_type='sample')."),
            id="bad_unsupported_kwarg2",
        ),
        pytest.param(
            bad_unsupported_kwarg3,
            re.escape(
                "PyDough function call SUM does not support "
                "keyword arguments at this time."
            ),
            id="bad_unsupported_kwarg3",
        ),
    ],
)
def test_unqualified_errors(
    func: Callable[[], UnqualifiedNode],
    error_msg: str,
    get_sample_graph: graph_fetcher,
) -> None:
    """
    Same as `test_init_pydough_context` except for cases that should raise an
    exception during the conversion to unqualified nodes.
    """
    sample_graph: GraphMetadata = get_sample_graph("TPCH")
    new_func: Callable[[], UnqualifiedNode] = init_pydough_context(sample_graph)(func)
    with pytest.raises(Exception, match=error_msg):
        new_func()
