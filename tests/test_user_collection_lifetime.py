"""
Tests for the lifecycle of PyDough user-generated collections
across independent query executions.
"""

import gc
import weakref
from uuid import uuid4

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughSession
from tests.test_pydough_functions.user_collections import (
    dataframe_collection_filtered_count,
)


@pytest.mark.execute
@pytest.mark.parametrize(
    "same_name",
    [
        pytest.param(True, id="same_name"),
        pytest.param(False, id="different_names"),
    ],
)
def test_dataframe_collection_query_lifetime(
    sqlite_tpch_session: PyDoughSession,
    same_name: bool,
) -> None:
    """
    Tests that independent queries return correct results and release their
    temporary input DataFrames after execution (#649).
    """
    query = init_pydough_context(sqlite_tpch_session.metadata)(
        dataframe_collection_filtered_count
    )
    prefix = f"cache_query_lifetime_{uuid4().hex}"

    def execute_query(i: int):
        df = pd.DataFrame({"k": range(i, i + 10)})
        name = prefix if same_name else f"{prefix}_{i}"
        result = to_df(
            query(name, df),
            metadata=sqlite_tpch_session.metadata,
            database=sqlite_tpch_session.database,
            config=sqlite_tpch_session.config,
        )
        return weakref.ref(df), result

    refs = []
    for i, expected_count in enumerate([3, 2, 1, 0, 0]):
        df_ref, result = execute_query(i)
        pd.testing.assert_frame_equal(
            result, pd.DataFrame({"n": [expected_count]}), check_dtype=False
        )
        refs.append(df_ref)

    gc.collect()
    retained_dataframes = sum(ref() is not None for ref in refs)
    assert retained_dataframes == 0, (
        f"{retained_dataframes} temporary DataFrames were retained after query execution"
    )
