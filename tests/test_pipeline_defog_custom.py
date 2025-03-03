"""
Integration tests for the PyDough workflow on custom queries using the defog.ai
schemas.
"""

from collections.abc import Callable

import pandas as pd
import pytest
from bad_pydough_functions import (
    bad_lpad_1,
    bad_lpad_2,
    bad_lpad_3,
    bad_lpad_4,
    bad_lpad_5,
    bad_lpad_6,
    bad_lpad_7,
    bad_lpad_8,
    bad_rpad_1,
    bad_rpad_2,
    bad_rpad_3,
    bad_rpad_4,
    bad_rpad_5,
    bad_rpad_6,
    bad_rpad_7,
    bad_rpad_8,
)
from simple_pydough_functions import (
    exponentiation,
    hour_minute_day,
    minutes_seconds_datediff,
    multi_partition_access_1,
    multi_partition_access_2,
    multi_partition_access_3,
    multi_partition_access_4,
    multi_partition_access_5,
    multi_partition_access_6,
    padding_functions,
    step_slicing,
    years_months_days_hours_datediff,
)
from test_utils import (
    graph_fetcher,
)

from pydough import init_pydough_context, to_df, to_sql
from pydough.configs import PyDoughConfigs
from pydough.conversion.relational_converter import convert_ast_to_relational
from pydough.database_connectors import DatabaseContext
from pydough.evaluation.evaluate_unqualified import _load_column_selection
from pydough.metadata import GraphMetadata
from pydough.qdag import PyDoughCollectionQDAG, PyDoughQDAG
from pydough.relational import RelationalRoot
from pydough.unqualified import (
    UnqualifiedNode,
    UnqualifiedRoot,
    qualify_node,
)


@pytest.fixture(
    params=[
        pytest.param(
            (
                multi_partition_access_1,
                None,
                "Broker",
                "multi_partition_access_1",
                lambda: pd.DataFrame(
                    {"symbol": ["AAPL", "AMZN", "BRK.B", "FB", "GOOG"]}
                ),
            ),
            id="multi_partition_access_1",
        ),
        pytest.param(
            (
                multi_partition_access_2,
                None,
                "Broker",
                "multi_partition_access_2",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [f"TX{i:03}" for i in (22, 24, 25, 27, 56)],
                        "name": [
                            "Jane Smith",
                            "Samantha Lee",
                            "Michael Chen",
                            "David Kim",
                            "Jane Smith",
                        ],
                        "symbol": ["MSFT", "TSLA", "GOOGL", "BRK.B", "FB"],
                        "transaction_type": ["sell", "sell", "buy", "buy", "sell"],
                        "cus_tick_typ_avg_shares": [56.66667, 55.0, 4.0, 55.5, 47.5],
                        "cust_tick_avg_shares": [
                            50.0,
                            41.66667,
                            3.33333,
                            37.33333,
                            47.5,
                        ],
                        "cust_avg_shares": [50.625, 46.25, 40.0, 37.33333, 50.625],
                    }
                ),
            ),
            id="multi_partition_access_2",
        ),
        pytest.param(
            (
                multi_partition_access_3,
                None,
                "Broker",
                "multi_partition_access_3",
                lambda: pd.DataFrame(
                    {
                        "symbol": [
                            "AAPL",
                            "AMZN",
                            "FB",
                            "GOOGL",
                            "JPM",
                            "MSFT",
                            "NFLX",
                            "PG",
                            "TSLA",
                            "V",
                        ],
                        "close": [
                            153.5,
                            3235,
                            207,
                            2535,
                            133.75,
                            284,
                            320.5,
                            143.25,
                            187.75,
                            223.5,
                        ],
                    }
                ),
            ),
            id="multi_partition_access_3",
        ),
        pytest.param(
            (
                multi_partition_access_4,
                None,
                "Broker",
                "multi_partition_access_4",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (3, 4, 5, 6, 7, 8, 9, 40, 41, 42, 43, 47, 48, 49)
                        ],
                    }
                ),
            ),
            id="multi_partition_access_4",
        ),
        pytest.param(
            (
                multi_partition_access_5,
                None,
                "Broker",
                "multi_partition_access_5",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (
                                40,
                                41,
                                42,
                                43,
                                2,
                                4,
                                6,
                                22,
                                24,
                                26,
                                32,
                                34,
                                36,
                                46,
                                48,
                                50,
                                52,
                                54,
                                56,
                            )
                        ],
                        "n_ticker_type_trans": [1] * 4 + [5] * 15,
                        "n_ticker_trans": [1] * 4 + [6] * 15,
                        "n_type_trans": [29, 27] * 2 + [27] * 15,
                    }
                ),
            ),
            id="multi_partition_access_5",
        ),
        pytest.param(
            (
                multi_partition_access_6,
                None,
                "Broker",
                "multi_partition_access_6",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            f"TX{i:03}"
                            for i in (
                                11,
                                12,
                                13,
                                14,
                                15,
                                16,
                                17,
                                18,
                                19,
                                20,
                                30,
                                46,
                                47,
                                48,
                                49,
                                50,
                            )
                        ],
                    }
                ),
            ),
            id="multi_partition_access_6",
        ),
        pytest.param(
            (
                hour_minute_day,
                None,
                "Broker",
                "hour_minute_day",
                lambda: pd.DataFrame(
                    {
                        "transaction_id": [
                            "TX001",
                            "TX005",
                            "TX011",
                            "TX015",
                            "TX021",
                            "TX025",
                            "TX031",
                            "TX033",
                            "TX035",
                            "TX044",
                            "TX045",
                            "TX049",
                            "TX051",
                            "TX055",
                        ],
                        "_expr0": [9, 12, 9, 12, 9, 12, 0, 0, 0, 10, 10, 16, 0, 0],
                        "_expr1": [30, 30, 30, 30, 30, 30, 0, 0, 0, 0, 30, 0, 0, 0],
                        "_expr2": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                    }
                ),
            ),
            id="hour_minute_day",
        ),
        pytest.param(
            (
                exponentiation,
                None,
                "Broker",
                "exponentiation",
                lambda: pd.DataFrame(
                    {
                        "low_square": [
                            6642.2500,
                            6740.4100,
                            6839.2900,
                            6938.8900,
                            7039.2100,
                            7140.2500,
                            7242.0100,
                            16576.5625,
                            16900.0000,
                            17292.2500,
                        ],
                        "low_sqrt": [
                            9.027735,
                            9.060905,
                            9.093954,
                            9.126883,
                            9.159694,
                            9.192388,
                            9.224966,
                            11.346806,
                            11.401754,
                            11.467345,
                        ],
                        "low_cbrt": [
                            4.335633,
                            4.346247,
                            4.356809,
                            4.367320,
                            4.377781,
                            4.388191,
                            4.398553,
                            5.049508,
                            5.065797,
                            5.085206,
                        ],
                    }
                ),
            ),
            id="exponentiation",
        ),
        pytest.param(
            (
                years_months_days_hours_datediff,
                None,
                "Broker",
                "years_months_days_hours_datediff",
                lambda: pd.DataFrame(
                    data={
                        "x": [
                            "2023-04-01 09:30:00",
                            "2023-04-01 10:15:00",
                            "2023-04-01 11:00:00",
                            "2023-04-01 11:45:00",
                            "2023-04-01 12:30:00",
                            "2023-04-01 13:15:00",
                            "2023-04-01 14:00:00",
                            "2023-04-01 14:45:00",
                            "2023-04-01 15:30:00",
                            "2023-04-01 16:15:00",
                            "2023-04-02 09:30:00",
                            "2023-04-02 10:15:00",
                            "2023-04-02 11:00:00",
                            "2023-04-02 11:45:00",
                            "2023-04-02 12:30:00",
                            "2023-04-02 13:15:00",
                            "2023-04-02 14:00:00",
                            "2023-04-02 14:45:00",
                            "2023-04-02 15:30:00",
                            "2023-04-02 16:15:00",
                            "2023-04-03 09:30:00",
                            "2023-04-03 10:15:00",
                            "2023-04-03 11:00:00",
                            "2023-04-03 11:45:00",
                            "2023-04-03 12:30:00",
                            "2023-04-03 13:15:00",
                            "2023-04-03 14:00:00",
                            "2023-04-03 14:45:00",
                            "2023-04-03 15:30:00",
                            "2023-04-03 16:15:00",
                        ],
                        "y1": ["2025-05-02 11:00:00"] * 30,
                        "years_diff": [2] * 30,
                        "c_years_diff": [2] * 30,
                        "c_y_diff": [2] * 30,
                        "y_diff": [2] * 30,
                        "months_diff": [25] * 30,
                        "c_months_diff": [25] * 30,
                        "mm_diff": [25] * 30,
                        "days_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "c_days_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "c_d_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "d_diff": [762] * 10 + [761] * 10 + [760] * 10,
                        "hours_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                        "c_hours_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                        "c_h_diff": [
                            18290,
                            18289,
                            18288,
                            18288,
                            18287,
                            18286,
                            18285,
                            18285,
                            18284,
                            18283,
                            18266,
                            18265,
                            18264,
                            18264,
                            18263,
                            18262,
                            18261,
                            18261,
                            18260,
                            18259,
                            18242,
                            18241,
                            18240,
                            18240,
                            18239,
                            18238,
                            18237,
                            18237,
                            18236,
                            18235,
                        ],
                    }
                ),
            ),
            id="years_months_days_hours_datediff",
        ),
        pytest.param(
            (
                minutes_seconds_datediff,
                None,
                "Broker",
                "minutes_seconds_datediff",
                lambda: pd.DataFrame(
                    {
                        "x": [
                            "2023-04-03 16:15:00",
                            "2023-04-03 15:30:00",
                            "2023-04-03 14:45:00",
                            "2023-04-03 14:00:00",
                            "2023-04-03 13:15:00",
                            "2023-04-03 12:30:00",
                            "2023-04-03 11:45:00",
                            "2023-04-03 11:00:00",
                            "2023-04-03 10:15:00",
                            "2023-04-03 09:30:00",
                            "2023-04-02 16:15:00",
                            "2023-04-02 15:30:00",
                            "2023-04-02 14:45:00",
                            "2023-04-02 14:00:00",
                            "2023-04-02 13:15:00",
                            "2023-04-02 12:30:00",
                            "2023-04-02 11:45:00",
                            "2023-04-02 11:00:00",
                            "2023-04-02 10:15:00",
                            "2023-04-02 09:30:00",
                            "2023-04-01 16:15:00",
                            "2023-04-01 15:30:00",
                            "2023-04-01 14:45:00",
                            "2023-04-01 14:00:00",
                            "2023-04-01 13:15:00",
                            "2023-04-01 12:30:00",
                            "2023-04-01 11:45:00",
                            "2023-04-01 11:00:00",
                            "2023-04-01 10:15:00",
                            "2023-04-01 09:30:00",
                        ],
                        "y": ["2023-04-03 13:16:30"] * 30,
                        "minutes_diff": [
                            -179,
                            -134,
                            -89,
                            -44,
                            1,
                            46,
                            91,
                            136,
                            181,
                            226,
                            1261,
                            1306,
                            1351,
                            1396,
                            1441,
                            1486,
                            1531,
                            1576,
                            1621,
                            1666,
                            2701,
                            2746,
                            2791,
                            2836,
                            2881,
                            2926,
                            2971,
                            3016,
                            3061,
                            3106,
                        ],
                        "seconds_diff": [
                            -10710,
                            -8010,
                            -5310,
                            -2610,
                            90,
                            2790,
                            5490,
                            8190,
                            10890,
                            13590,
                            75690,
                            78390,
                            81090,
                            83790,
                            86490,
                            89190,
                            91890,
                            94590,
                            97290,
                            99990,
                            162090,
                            164790,
                            167490,
                            170190,
                            172890,
                            175590,
                            178290,
                            180990,
                            183690,
                            186390,
                        ],
                    }
                ),
            ),
            id="minutes_seconds_datediff",
        ),
        pytest.param(
            (
                padding_functions,
                None,
                "Broker",
                "padding_functions",
                lambda: pd.DataFrame(
                    {
                        "original_name": [
                            "Alex Rodriguez",
                            "Ava Wilson",
                            "Bob Johnson",
                            "David Kim",
                            "Emily Davis",
                        ]
                    }
                ).assign(
                    ref_rpad=lambda x: "Cust0001**********************",
                    ref_lpad=lambda x: "**********************Cust0001",
                    right_padded=lambda x: x.original_name.apply(
                        lambda s: (s + "*" * 30)[:30]
                    ),
                    # This lambda only works when each string is less than 30 characters
                    left_padded=lambda x: x.original_name.apply(
                        lambda s: ("#" * 30 + s)[-30:]
                    ),
                    truncated_right=[
                        "Alex Rod",
                        "Ava Wils",
                        "Bob John",
                        "David Ki",
                        "Emily Da",
                    ],
                    truncated_left=[
                        "Alex Rod",
                        "Ava Wils",
                        "Bob John",
                        "David Ki",
                        "Emily Da",
                    ],
                    zero_pad_right=[""] * 5,
                    zero_pad_left=[""] * 5,
                    right_padded_space=lambda x: x.original_name.apply(
                        lambda s: (s + " " * 30)[:30]
                    ),
                    left_padded_space=lambda x: x.original_name.apply(
                        lambda s: (" " * 30 + s)[-30:]
                    ),
                ),
            ),
            id="padding_functions",
        ),
        pytest.param(
            (
                step_slicing,
                None,
                "Broker",
                "step_slicing",
                lambda: pd.DataFrame(
                    {
                        "name": [
                            "john doe",
                            "Jane Smith",
                            "Bob Johnson",
                            "Samantha Lee",
                            "Michael Chen",
                            "Emily Davis",
                            "David Kim",
                            "Sarah Nguyen",
                            "William Garcia",
                            "Jessica Hernandez",
                            "Alex Rodriguez",
                            "Olivia Johnson",
                            "Ethan Davis",
                            "Ava Wilson",
                            "Emma Brown",
                            "sophia martinez",
                            "Jacob Taylor",
                            "Michael Anderson",
                            "Isabella Thompson",
                            "Maurice Lee",
                        ]
                    }
                ).assign(
                    neg_none_step=lambda x: x["name"].str[-2::1],
                    pos_none_step=lambda x: x["name"].str[3::1],
                    none_pos_step=lambda x: x["name"].str[:3:1],
                    none_neg_step=lambda x: x["name"].str[:-2:1],
                    pos_pos_step=lambda x: x["name"].str[2:4:1],
                    pos_neg_step=lambda x: x["name"].str[2:-2:1],
                    neg_pos_step=lambda x: x["name"].str[-12:2:1],
                    neg_neg_step=lambda x: x["name"].str[-4:-2:1],
                    inbetween_chars=lambda x: x["name"].str[1:-1:1],
                    empty1=lambda x: x["name"].str[2:2:1],
                    empty2=lambda x: x["name"].str[-2:-2:1],
                    empty3=lambda x: x["name"].str[-2:-4:1],
                    empty4=lambda x: x["name"].str[4:2:1],
                    oob1=lambda x: x["name"].str[100:200:1],
                    oob2=lambda x: x["name"].str[-200:-100:1],
                    oob3=lambda x: x["name"].str[100::1],
                    oob4=lambda x: x["name"].str[-200::1],
                    oob5=lambda x: x["name"].str[:100:1],
                    oob6=lambda x: x["name"].str[:-200:1],
                    oob7=lambda x: x["name"].str[100:-200:1],
                    oob8=lambda x: x["name"].str[-200:100:1],
                    oob9=lambda x: x["name"].str[100:-1:1],
                    oob10=lambda x: x["name"].str[-100:-1:1],
                    oob11=lambda x: x["name"].str[-3:100:1],
                    oob12=lambda x: x["name"].str[-3:-100:1],
                    zero1=lambda x: x["name"].str[0:0:1],
                    zero2=lambda x: x["name"].str[0:1:1],
                    zero3=lambda x: x["name"].str[-1:0:1],
                    zero4=lambda x: x["name"].str[1:0:1],
                    zero5=lambda x: x["name"].str[0:-1:1],
                    zero6=lambda x: x["name"].str[0:-20:1],
                    zero7=lambda x: x["name"].str[0:100:1],
                    zero8=lambda x: x["name"].str[20:0:1],
                    zero9=lambda x: x["name"].str[-20:0:1],
                    wo_step1=lambda x: x["name"].str[-2:],
                    wo_step2=lambda x: x["name"].str[3:],
                    wo_step3=lambda x: x["name"].str[:3],
                    wo_step4=lambda x: x["name"].str[:-2],
                    wo_step5=lambda x: x["name"].str[2:4],
                    wo_step6=lambda x: x["name"].str[2:-2],
                    wo_step7=lambda x: x["name"].str[-4:2],
                    wo_step8=lambda x: x["name"].str[-4:-2],
                    wo_step9=lambda x: x["name"].str[2:2],
                ),
            ),
            id="step_slicing",
        ),
    ],
)
def custom_defog_test_data(
    request,
) -> tuple[
    Callable[[], UnqualifiedNode],
    dict[str, str] | list[str] | None,
    str,
    str,
    pd.DataFrame,
]:
    """
    Test data for `test_pipeline_e2e_defog_custom`. Returns a tuple of the
    following arguments:
    1. `unqualified_impl`: a PyDough implementation function.
    2. `columns`: the columns to select from the relational plan (optional).
    3. `graph_name`: the name of the graph from the defog database to use.
    4. `file_name`: the name of the file containing the expected relational
    plan.
    5. `answer_impl`: a function that takes in nothing and returns the answer
    to a defog query as a Pandas DataFrame.
    """
    return request.param


def test_pipeline_until_relational_defog(
    custom_defog_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        str,
        pd.DataFrame,
    ],
    defog_graphs: graph_fetcher,
    default_config: PyDoughConfigs,
    get_plan_test_filename: Callable[[str], str],
    update_tests: bool,
):
    """
    Tests that a PyDough unqualified node can be correctly translated to its
    qualified DAG version, with the correct string representation. Run on
    custom questions using the defog.ai schemas.
    """
    unqualified_impl, columns, graph_name, file_name, _ = custom_defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    init_pydough_context(graph)(unqualified_impl)()
    file_path: str = get_plan_test_filename(file_name)
    UnqualifiedRoot(graph)
    unqualified: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    qualified: PyDoughQDAG = qualify_node(unqualified, graph)
    assert isinstance(
        qualified, PyDoughCollectionQDAG
    ), "Expected qualified answer to be a collection, not an expression"
    relational: RelationalRoot = convert_ast_to_relational(
        qualified, _load_column_selection({"columns": columns}), default_config
    )
    if update_tests:
        with open(file_path, "w") as f:
            f.write(relational.to_tree_string() + "\n")
    else:
        with open(file_path) as f:
            expected_relational_string: str = f.read()
        assert (
            relational.to_tree_string() == expected_relational_string.strip()
        ), "Mismatch between tree string representation of relational node and expected Relational tree string"


@pytest.mark.execute
def test_pipeline_e2e_defog_custom(
    custom_defog_test_data: tuple[
        Callable[[], UnqualifiedNode],
        dict[str, str] | list[str] | None,
        str,
        str,
        pd.DataFrame,
    ],
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on custom questions using the defog.ai
    schemas.
    """
    unqualified_impl, columns, graph_name, _, answer_impl = custom_defog_test_data
    graph: GraphMetadata = defog_graphs(graph_name)
    root: UnqualifiedNode = init_pydough_context(graph)(unqualified_impl)()
    result: pd.DataFrame = to_df(
        root, columns=columns, metadata=graph, database=sqlite_defog_connection
    )
    pd.testing.assert_frame_equal(result, answer_impl())


@pytest.mark.parametrize(
    "impl, graph_name, error_msg",
    [
        pytest.param(
            bad_lpad_1,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_1",
        ),
        pytest.param(
            bad_lpad_2,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_2",
        ),
        pytest.param(
            bad_lpad_3,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_3",
        ),
        pytest.param(
            bad_lpad_4,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_4",
        ),
        pytest.param(
            bad_lpad_5,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_5",
        ),
        pytest.param(
            bad_lpad_6,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_6",
        ),
        pytest.param(
            bad_lpad_7,
            "Broker",
            "LPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_lpad_7",
        ),
        pytest.param(
            bad_lpad_8,
            "Broker",
            "LPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_lpad_8",
        ),
        pytest.param(
            bad_rpad_1,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_1",
        ),
        pytest.param(
            bad_rpad_2,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_2",
        ),
        pytest.param(
            bad_rpad_3,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_3",
        ),
        pytest.param(
            bad_rpad_4,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_4",
        ),
        pytest.param(
            bad_rpad_5,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_5",
        ),
        pytest.param(
            bad_rpad_6,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_6",
        ),
        pytest.param(
            bad_rpad_7,
            "Broker",
            "RPAD function requires the length argument to be a non-negative integer literal.",
            id="bad_rpad_7",
        ),
        pytest.param(
            bad_rpad_8,
            "Broker",
            "RPAD function requires the padding argument to be a string literal of length 1.",
            id="bad_rpad_8",
        ),
    ],
)
def test_defog_e2e_errors(
    impl: Callable[[], UnqualifiedNode],
    graph_name: str,
    error_msg: str,
    defog_graphs: graph_fetcher,
    sqlite_defog_connection: DatabaseContext,
):
    """
    Tests running bad PyDough code through the entire pipeline to verify that
    a certain error is raised for defog database.
    """
    graph: GraphMetadata = defog_graphs(graph_name)
    with pytest.raises(Exception, match=error_msg):
        root: UnqualifiedNode = init_pydough_context(graph)(impl)()
        to_sql(root, metadata=graph, database=sqlite_defog_connection)
