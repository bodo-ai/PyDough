"""
Tests for the lifecycle of PyDough user-generated collections
at the QDAG level and across independent query executions.
"""

import gc
import weakref
from uuid import uuid4

import pandas as pd
import pytest

from pydough import init_pydough_context, to_df
from pydough.configs import PyDoughSession
from pydough.qdag.collections.user_collection_qdag import (
    PyDoughUserGeneratedCollectionQDag,
)
from pydough.qdag.expressions.reference import Reference
from pydough.unqualified import qualify_node
from tests.test_pydough_functions.simple_pydough_functions import (
    dataframe_collection_filtered_count,
    dataframe_collection_term_access,
)


def qualify_dataframe_collection(
    session: PyDoughSession,
    collection_name: str,
    df: pd.DataFrame,
) -> PyDoughUserGeneratedCollectionQDag:
    """
    Qualifies a user-generated DataFrame collection using the function-style
    PyDough interface.
    """
    assert session.metadata is not None

    unqualified = init_pydough_context(session.metadata)(
        dataframe_collection_term_access
    )(collection_name, df)
    qualified = qualify_node(unqualified, session)
    assert isinstance(qualified, PyDoughUserGeneratedCollectionQDag)
    return qualified


@pytest.mark.parametrize(
    "num_collections, same_name",
    [
        pytest.param(1, False, id="single_collection"),
        pytest.param(5, False, id="different_names"),
        pytest.param(5, True, id="same_name"),
    ],
)
def test_qdag_dataframe_collection_lifetime(
    tpch_session: PyDoughSession,
    num_collections: int,
    same_name: bool,
) -> None:
    """
    Tests that accessing terms does not retain temporary QDAG collections,
    their underlying user-generated collections, or their DataFrames (#649).
    """
    # Keep names unique across test invocations, including pytest-repeat runs.
    prefix = f"cache_lifetime_{uuid4().hex}"

    def create_collection(i: int):
        df = pd.DataFrame({"k": range(i, i + 10)})
        name = prefix if same_name else f"{prefix}_{i}"
        qdag = qualify_dataframe_collection(tpch_session, name, df)
        qdag.get_term("k")
        return (
            weakref.ref(qdag),
            weakref.ref(qdag.collection),
            weakref.ref(df),
        )

    refs = [create_collection(i) for i in range(num_collections)]
    gc.collect()

    retained_qdags = sum(qdag() is not None for qdag, _, _ in refs)
    retained_collections = sum(collection() is not None for _, collection, _ in refs)
    retained_dataframes = sum(df() is not None for _, _, df in refs)
    assert (retained_qdags, retained_collections, retained_dataframes) == (0, 0, 0)


def test_qdag_dataframe_collection_repeated_term_access(
    tpch_session: PyDoughSession,
) -> None:
    """
    Tests that repeated term access returns valid references to the live
    user-generated collection, without requiring reference object reuse.
    """
    df = pd.DataFrame({"k": [1, 2, 3]})
    qdag = qualify_dataframe_collection(
        tpch_session, f"cache_repeated_term_access_{uuid4().hex}", df
    )

    for _ in range(5):
        term = qdag.get_term("k")
        assert isinstance(term, Reference)
        assert term.term_name == "k"
        assert term.collection is qdag


@pytest.mark.parametrize(
    "same_data",
    [
        pytest.param(True, id="same_contents"),
        pytest.param(False, id="different_contents"),
    ],
)
def test_qdag_dataframe_collection_term_context(
    tpch_session: PyDoughSession,
    same_data: bool,
) -> None:
    """
    Tests that term references belong to the actual collection instance,
    even when independently qualified collections share a name (#649).
    """
    first_df = pd.DataFrame({"k": [1, 2, 3]})
    second_df = pd.DataFrame({"k": [1, 2, 3] if same_data else [10, 20, 30]})
    name = f"cache_term_context_{uuid4().hex}"

    first = qualify_dataframe_collection(tpch_session, name, first_df)
    second = qualify_dataframe_collection(tpch_session, name, second_df)

    first_term = first.get_term("k")
    second_term = second.get_term("k")
    assert isinstance(first_term, Reference)
    assert isinstance(second_term, Reference)
    assert first_term.collection is first
    assert second_term.collection is second


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
    assert sqlite_tpch_session.metadata is not None
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
