"""
Integration tests for the PyDough workflow on the TPC-H queries using Oracle.
"""

from collections.abc import Callable

import pandas as pd
import pytest

from pydough.configs import PyDoughConfigs
from pydough.database_connectors import DatabaseContext
from tests.testing_utilities import (
    PyDoughPandasTest,
    PyDoughSQLComparisonTest,
    graph_fetcher,
)

from .test_pipeline_custom_datasets import custom_datasets_test_data  # noqa
from .test_pipeline_defog import defog_pipeline_test_data  # noqa
from .test_pipeline_defog_custom import defog_custom_pipeline_test_data  # noqa
from .test_pipeline_tpch_custom import tpch_custom_pipeline_test_data  # noqa


@pytest.fixture
def defog_custom_oracle_test_data(
    defog_custom_pipeline_test_data: PyDoughPandasTest,  # noqa: F811
) -> PyDoughPandasTest:
    """
    Modify reference solution data for some Defog queries.
    Return an instance of PyDoughPandasTest containing the modified data.
    """
    if defog_custom_pipeline_test_data.test_name == "week_offset":
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            "Broker",
            lambda: pd.DataFrame(
                {
                    "date_time": [
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
                        "2023-01-15 10:00:00",
                        "2023-01-16 10:30:00",
                        "2023-02-20 11:30:00",
                        "2023-03-25 14:45:00",
                        "2023-01-30 13:15:00",
                        "2023-02-28 16:00:00",
                        "2023-03-30 09:45:00",
                    ],
                    "week_adj1": [
                        "2023-04-09 09:30:00",
                        "2023-04-09 10:15:00",
                        "2023-04-09 11:00:00",
                        "2023-04-09 11:45:00",
                        "2023-04-09 12:30:00",
                        "2023-04-09 13:15:00",
                        "2023-04-09 14:00:00",
                        "2023-04-09 14:45:00",
                        "2023-04-09 15:30:00",
                        "2023-04-09 16:15:00",
                        "2023-04-10 09:30:00",
                        "2023-04-10 10:15:00",
                        "2023-04-10 11:00:00",
                        "2023-04-10 11:45:00",
                        "2023-04-10 12:30:00",
                        "2023-04-10 13:15:00",
                        "2023-04-10 14:00:00",
                        "2023-04-10 14:45:00",
                        "2023-04-10 15:30:00",
                        "2023-04-10 16:15:00",
                        "2023-01-22 10:00:00",
                        "2023-01-23 10:30:00",
                        "2023-02-27 11:30:00",
                        "2023-04-01 14:45:00",
                        "2023-02-06 13:15:00",
                        "2023-03-07 16:00:00",
                        "2023-04-06 09:45:00",
                    ],
                    "week_adj2": [
                        "2023-03-26 09:30:00",
                        "2023-03-26 10:15:00",
                        "2023-03-26 11:00:00",
                        "2023-03-26 11:45:00",
                        "2023-03-26 12:30:00",
                        "2023-03-26 13:15:00",
                        "2023-03-26 14:00:00",
                        "2023-03-26 14:45:00",
                        "2023-03-26 15:30:00",
                        "2023-03-26 16:15:00",
                        "2023-03-27 09:30:00",
                        "2023-03-27 10:15:00",
                        "2023-03-27 11:00:00",
                        "2023-03-27 11:45:00",
                        "2023-03-27 12:30:00",
                        "2023-03-27 13:15:00",
                        "2023-03-27 14:00:00",
                        "2023-03-27 14:45:00",
                        "2023-03-27 15:30:00",
                        "2023-03-27 16:15:00",
                        "2023-01-08 10:00:00",
                        "2023-01-09 10:30:00",
                        "2023-02-13 11:30:00",
                        "2023-03-18 14:45:00",
                        "2023-01-23 13:15:00",
                        "2023-02-21 16:00:00",
                        "2023-03-23 09:45:00",
                    ],
                    "week_adj3": [
                        "2023-04-16 10:30:00",
                        "2023-04-16 11:15:00",
                        "2023-04-16 12:00:00",
                        "2023-04-16 12:45:00",
                        "2023-04-16 13:30:00",
                        "2023-04-16 14:15:00",
                        "2023-04-16 15:00:00",
                        "2023-04-16 15:45:00",
                        "2023-04-16 16:30:00",
                        "2023-04-16 17:15:00",
                        "2023-04-17 10:30:00",
                        "2023-04-17 11:15:00",
                        "2023-04-17 12:00:00",
                        "2023-04-17 12:45:00",
                        "2023-04-17 13:30:00",
                        "2023-04-17 14:15:00",
                        "2023-04-17 15:00:00",
                        "2023-04-17 15:45:00",
                        "2023-04-17 16:30:00",
                        "2023-04-17 17:15:00",
                        "2023-01-29 11:00:00",
                        "2023-01-30 11:30:00",
                        "2023-03-06 12:30:00",
                        "2023-04-08 15:45:00",
                        "2023-02-13 14:15:00",
                        "2023-03-14 17:00:00",
                        "2023-04-13 10:45:00",
                    ],
                    "week_adj4": [
                        "2023-04-16 09:29:59",
                        "2023-04-16 10:14:59",
                        "2023-04-16 10:59:59",
                        "2023-04-16 11:44:59",
                        "2023-04-16 12:29:59",
                        "2023-04-16 13:14:59",
                        "2023-04-16 13:59:59",
                        "2023-04-16 14:44:59",
                        "2023-04-16 15:29:59",
                        "2023-04-16 16:14:59",
                        "2023-04-17 09:29:59",
                        "2023-04-17 10:14:59",
                        "2023-04-17 10:59:59",
                        "2023-04-17 11:44:59",
                        "2023-04-17 12:29:59",
                        "2023-04-17 13:14:59",
                        "2023-04-17 13:59:59",
                        "2023-04-17 14:44:59",
                        "2023-04-17 15:29:59",
                        "2023-04-17 16:14:59",
                        "2023-01-29 09:59:59",
                        "2023-01-30 10:29:59",
                        "2023-03-06 11:29:59",
                        "2023-04-08 14:44:59",
                        "2023-02-13 13:14:59",
                        "2023-03-14 15:59:59",
                        "2023-04-13 09:44:59",
                    ],
                    "week_adj5": [
                        "2023-04-17 09:30:00",
                        "2023-04-17 10:15:00",
                        "2023-04-17 11:00:00",
                        "2023-04-17 11:45:00",
                        "2023-04-17 12:30:00",
                        "2023-04-17 13:15:00",
                        "2023-04-17 14:00:00",
                        "2023-04-17 14:45:00",
                        "2023-04-17 15:30:00",
                        "2023-04-17 16:15:00",
                        "2023-04-18 09:30:00",
                        "2023-04-18 10:15:00",
                        "2023-04-18 11:00:00",
                        "2023-04-18 11:45:00",
                        "2023-04-18 12:30:00",
                        "2023-04-18 13:15:00",
                        "2023-04-18 14:00:00",
                        "2023-04-18 14:45:00",
                        "2023-04-18 15:30:00",
                        "2023-04-18 16:15:00",
                        "2023-01-30 10:00:00",
                        "2023-01-31 10:30:00",
                        "2023-03-07 11:30:00",
                        "2023-04-09 14:45:00",
                        "2023-02-14 13:15:00",
                        "2023-03-15 16:00:00",
                        "2023-04-14 09:45:00",
                    ],
                    "week_adj6": [
                        "2023-04-16 09:29:00",
                        "2023-04-16 10:14:00",
                        "2023-04-16 10:59:00",
                        "2023-04-16 11:44:00",
                        "2023-04-16 12:29:00",
                        "2023-04-16 13:14:00",
                        "2023-04-16 13:59:00",
                        "2023-04-16 14:44:00",
                        "2023-04-16 15:29:00",
                        "2023-04-16 16:14:00",
                        "2023-04-17 09:29:00",
                        "2023-04-17 10:14:00",
                        "2023-04-17 10:59:00",
                        "2023-04-17 11:44:00",
                        "2023-04-17 12:29:00",
                        "2023-04-17 13:14:00",
                        "2023-04-17 13:59:00",
                        "2023-04-17 14:44:00",
                        "2023-04-17 15:29:00",
                        "2023-04-17 16:14:00",
                        "2023-01-29 09:59:00",
                        "2023-01-30 10:29:00",
                        "2023-03-06 11:29:00",
                        "2023-04-08 14:44:00",
                        "2023-02-13 13:14:00",
                        "2023-03-14 15:59:00",
                        "2023-04-13 09:44:00",
                    ],
                    "week_adj7": [
                        "2023-05-16 09:30:00",
                        "2023-05-16 10:15:00",
                        "2023-05-16 11:00:00",
                        "2023-05-16 11:45:00",
                        "2023-05-16 12:30:00",
                        "2023-05-16 13:15:00",
                        "2023-05-16 14:00:00",
                        "2023-05-16 14:45:00",
                        "2023-05-16 15:30:00",
                        "2023-05-16 16:15:00",
                        "2023-05-17 09:30:00",
                        "2023-05-17 10:15:00",
                        "2023-05-17 11:00:00",
                        "2023-05-17 11:45:00",
                        "2023-05-17 12:30:00",
                        "2023-05-17 13:15:00",
                        "2023-05-17 14:00:00",
                        "2023-05-17 14:45:00",
                        "2023-05-17 15:30:00",
                        "2023-05-17 16:15:00",
                        "2023-03-01 10:00:00",
                        "2023-03-02 10:30:00",
                        "2023-04-03 11:30:00",
                        "2023-05-09 14:45:00",
                        "2023-03-14 13:15:00",
                        "2023-04-14 16:00:00",  # This changed
                        "2023-05-14 09:45:00",
                    ],
                    "week_adj8": [
                        "2024-04-16 09:30:00",
                        "2024-04-16 10:15:00",
                        "2024-04-16 11:00:00",
                        "2024-04-16 11:45:00",
                        "2024-04-16 12:30:00",
                        "2024-04-16 13:15:00",
                        "2024-04-16 14:00:00",
                        "2024-04-16 14:45:00",
                        "2024-04-16 15:30:00",
                        "2024-04-16 16:15:00",
                        "2024-04-17 09:30:00",
                        "2024-04-17 10:15:00",
                        "2024-04-17 11:00:00",
                        "2024-04-17 11:45:00",
                        "2024-04-17 12:30:00",
                        "2024-04-17 13:15:00",
                        "2024-04-17 14:00:00",
                        "2024-04-17 14:45:00",
                        "2024-04-17 15:30:00",
                        "2024-04-17 16:15:00",
                        "2024-01-29 10:00:00",
                        "2024-01-30 10:30:00",
                        "2024-03-05 11:30:00",
                        "2024-04-08 14:45:00",
                        "2024-02-13 13:15:00",
                        "2024-03-14 16:00:00",  # This changed
                        "2024-04-13 09:45:00",
                    ],
                }
            ),
            "week_offset",
            skip_sql=True,
        )

    if defog_custom_pipeline_test_data.test_name == "get_part_multiple":
        # In Oracle None and empty string are treated the same, so we need to
        # modify the reference solution data to match this behavior.
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            "Broker",
            lambda: pd.DataFrame(
                {
                    "k": [1, 2, 3, 4],
                    "p1": ["john", "Smith", None, None],
                    "p2": ["doe", "Jane", None, None],
                    "p3": ["john", "smith@email", "com", None],
                    "p4": ["com", "smith@email", "bob", None],
                    "p5": ["555", "987", "8135", None],
                    "p6": ["4567", "987", "555", None],
                    "p7": ["9", "02", None, None],
                    "p8": ["01", "1", None, None],
                    "p9": ["john doe", None, None, None],
                    "p10": ["john doe", None, None, None],
                    "p11": ["john doe", None, None, None],
                    "p12": ["john doe", None, None, None],
                    "p13": ["john doe", None, None, None],
                    "p14": [None, None, None, None],
                    "p15": ["john", "Jane", "Bob", "Samantha"],
                    "p16": [None, None, None, None],
                    "p17": [None, None, None, None],
                    "p18": ["9", None, None, None],
                }
            ),
            "get_part_multiple",
            skip_sql=True,
        )

    if defog_custom_pipeline_test_data.test_name == "padding_functions":
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            defog_custom_pipeline_test_data.graph_name,
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
                ref_rpad="Cust0001**********************",
                ref_lpad="**********************Cust0001",
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
                zero_pad_right=[None] * 5,
                zero_pad_left=[None] * 5,
                right_padded_space=lambda x: x.original_name.apply(
                    lambda s: (s + " " * 30)[:30]
                ),
                left_padded_space=lambda x: x.original_name.apply(
                    lambda s: (" " * 30 + s)[-30:]
                ),
            ),
            defog_custom_pipeline_test_data.test_name,
            skip_sql=defog_custom_pipeline_test_data.skip_sql,
        )

    if defog_custom_pipeline_test_data.test_name == "step_slicing":
        refsol = defog_custom_pipeline_test_data.pd_function().copy()
        value_cols = refsol.columns.difference(["name"])
        refsol[value_cols] = refsol[value_cols].where(refsol[value_cols] != "", None)
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            defog_custom_pipeline_test_data.graph_name,
            lambda: refsol,
            defog_custom_pipeline_test_data.test_name,
            skip_sql=defog_custom_pipeline_test_data.skip_sql,
        )

    if defog_custom_pipeline_test_data.test_name == "strip":
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            defog_custom_pipeline_test_data.graph_name,
            lambda: pd.DataFrame(
                {
                    "stripped_name": [None],
                    "stripped_name1": ["Alex Rodriguez"],
                    "stripped_name_with_chars": ["x Rodrigu"],
                    "stripped_alt_name1": ["Alex Rodriguez"],
                    "stripped_alt_name2": ["Alex Rodriguez"],
                    "stripped_alt_name3": ["Alex Rodriguez"],
                    "stripped_alt_name4": ["Alex Rodriguez"],
                    "stripped_alt_name5": ["Alex Rodriguez"],
                }
            ),
            defog_custom_pipeline_test_data.test_name,
            skip_sql=defog_custom_pipeline_test_data.skip_sql,
        )

    if defog_custom_pipeline_test_data.test_name == "replace":
        return PyDoughPandasTest(
            defog_custom_pipeline_test_data.pydough_function,
            defog_custom_pipeline_test_data.graph_name,
            lambda: pd.DataFrame(
                {
                    "replaced_name": ["Alexander Rodriguez"],
                    "removed_name": [" Rodriguez"],
                    "case_name": ["Alex Rodriguez"],
                    "replace_empty_text": [None],
                    "replace_with_empty_pattern": ["abc"],
                    "remove_substring": ["bc"],
                    "empty_all": [None],
                    "substring_not_found": ["hello"],
                    "overlapping_matches": ["ba"],
                    "multiple_occurrences": ["b b b"],
                    "case_sensitive": ["Apple"],
                    "unicode_handling": ["cafe"],
                    "special_character_replace": ["abc"],
                    "longer_replacement": ["xyz"],
                    "shorter_replacement": ["xx"],
                    "same_value_args": ["foofoo"],
                    "nested_like_replace": ["abcabcabcabc"],
                }
            ),
            defog_custom_pipeline_test_data.test_name,
            skip_sql=defog_custom_pipeline_test_data.skip_sql,
        )

    return defog_custom_pipeline_test_data


@pytest.fixture
def custom_functions_oracle_test_data(
    custom_functions_test_data: PyDoughPandasTest,  # noqa: F811
) -> PyDoughPandasTest:
    """
    Modify reference solution data for some custom queries.
    Return an instance of PyDoughPandasTest containing the modified data.
    """
    if custom_functions_test_data.test_name == "get_part_test":
        # In Oracle None and empty string are treated the same, so we need to
        # modify the reference solution data to match this behavior.
        return PyDoughPandasTest(
            custom_functions_test_data.pydough_function,
            custom_functions_test_data.graph_name,
            lambda: pd.DataFrame(
                {
                    "k": [1, 2, 3, 4],
                    "p1": ["Customer", "000000002", None, None],
                    "p2": ["Customer#", None, None, None],
                    "p3": ["IVhzIApeRb ot", "NCwDVaWNe6tEgvwfmRchLXak", None, None],
                    "p4": ["E", "XSTf4", None, None],
                    "p5": ["25", "768", "748", "5944"],
                    "p6": ["2988", "687", "719", "14"],
                    "p7": ["to", "accounts.", "eat", "regular"],
                    "p8": ["e", "boldly:", "even", "ideas"],
                    "p9": ["IVhzIApeRb ot,c,E", None, None, None],
                    "p10": ["BUILDING", "M", "AUT", None],
                    "p11": ["Customer#", "2", None, None],
                    "p12": ["*^%3$#", "##2$#&", "^%1$$", None],
                    "p13": ["Customer#000000001", None, None, None],
                    "p14": [None, None, None, None],
                    "p15": ["Customer", "Customer", "Customer", "Customer"],
                    "p16": [None, None, None, None],
                    "p17": [None, "68", "48", None],
                }
            ),
            custom_functions_test_data.test_name,
        )
    return custom_functions_test_data


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_custom_functions(
    custom_functions_oracle_test_data: PyDoughPandasTest,
    get_sample_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the custom functions test data using TPCH with Oracle
    """
    custom_functions_oracle_test_data.run_e2e_test(
        get_sample_graph, oracle_conn_db_context("tpch"), coerce_types=True
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_defog_custom(
    defog_custom_oracle_test_data: PyDoughPandasTest,  # noqa: F811
    get_oracle_defog_graphs: graph_fetcher,
    defog_config: PyDoughConfigs,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the defog analytical queries with Oracle database.
    """
    defog_custom_oracle_test_data.run_e2e_test(
        get_oracle_defog_graphs,
        oracle_conn_db_context(defog_custom_oracle_test_data.graph_name.lower()),
        config=defog_config,
        coerce_types=True,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_defog(
    defog_pipeline_test_data: PyDoughSQLComparisonTest,  # noqa: F811
    get_oracle_defog_graphs: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
    defog_config: PyDoughConfigs,
    sqlite_defog_connection: DatabaseContext,
) -> None:
    """
    Test executing the defog analytical questions on the sqlite database,
    comparing against the result of running the reference SQL query text on the
    same database connector. Run on the defog.ai queries.
    NOTE: passing SQLite connection as reference database so that refsol
    is executed using SQLite.
    This is needed because refsol uses SQLite SQL syntax to obtain
    the correct results.
    """
    defog_pipeline_test_data.run_e2e_test(
        get_oracle_defog_graphs,
        oracle_conn_db_context(defog_pipeline_test_data.graph_name.lower()),
        defog_config,
        reference_database=sqlite_defog_connection,
        coerce_types=True,
        rtol=1e4,
    )


@pytest.mark.oracle
@pytest.mark.execute
def test_pipeline_e2e_oracle_custom_datasets(
    custom_datasets_test_data: PyDoughPandasTest,  # noqa: F811
    get_oracle_custom_datasets_graph: graph_fetcher,
    oracle_conn_db_context: Callable[[str], DatabaseContext],
):
    """
    Test executing the the custom queries with the custom datasets against the
    refsol DataFrame.
    """
    custom_datasets_test_data.run_e2e_test(
        get_oracle_custom_datasets_graph,
        oracle_conn_db_context(custom_datasets_test_data.graph_name.lower()),
        coerce_types=True,
    )
