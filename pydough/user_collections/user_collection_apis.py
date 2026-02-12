"""
Implementation of User Collection APIs in PyDough.
"""

__all__ = ["dataframe_collection", "range_collection"]

import pandas as pd

from pydough.unqualified.unqualified_node import UnqualifiedGeneratedCollection
from pydough.user_collections.dataframe_collection import DataframeGeneratedCollection
from pydough.user_collections.range_collection import RangeGeneratedCollection


def range_collection(
    name: str, column: str, *args: int
) -> UnqualifiedGeneratedCollection:
    """
    Implementation of the `pydough.range_collection` function, which provides
    a way to create a collection of integer ranges over a specified column in PyDough.

    Args:
        `name` : The name of the collection.
        `column` : The column to create ranges for.
        `*args` : Variable length arguments that specify the range parameters.
        Supported formats:
            - `range_collection(end)`: generates a range from 0 to `end-1`
                                        with a step of 1.
            - `range_collection(start, end)`: generates a range from `start`
                                        to `end-1` with a step of 1.
            - `range_collection(start, end, step)`: generates a range from
                                    `start` to `end-1` with the specified step.
    Returns:
        A collection of integer ranges.
    """
    if not isinstance(name, str):
        raise TypeError(f"Expected 'name' to be a string, got {type(name).__name__}")
    if not isinstance(column, str):
        raise TypeError(
            f"Expected 'column' to be a string, got {type(column).__name__}"
        )
    r = range(*args)
    range_collection = RangeGeneratedCollection(
        name=name,
        column_name=column,
        range=r,
    )

    return UnqualifiedGeneratedCollection(range_collection)


def dataframe_collection(
    name: str, dataframe: pd.DataFrame, unique_column_names: list[str | list[str]]
) -> UnqualifiedGeneratedCollection:
    """
    Implementation of the `pydough.dataframe_collection` function, which provides
    a way to create a collection of Pandas Dataframe in PyDough.

    Args:
        `name` : The name of the collection.
        `dataframe` : The dataframe of the collection
        `unique_column_names`: List of unique properties or unique combinations
        in the collection.

    Returns:
        A collection with the given dataframe.
    """
    if not isinstance(name, str):
        raise TypeError(f"Expected 'name' to be a string, got {type(name).__name__}")
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError(
            f"Expected 'dataframe' to be a Pandas DataFrame, got {type(dataframe).__name__}"
        )
    if not isinstance(unique_column_names, list):
        raise TypeError(
            f"Expected 'unique_column_names' to be a list of string, got {type(unique_column_names).__name__}"
        )

    unique_flatten_columns: list[str] = [
        col
        for item in unique_column_names
        for col in ([item] if isinstance(item, str) else item)
    ]

    if not all(col in dataframe.columns for col in unique_flatten_columns):
        raise ValueError(
            "Not existing column from 'unique_column_names' in the dataframe."
        )

    dataframe_collection = DataframeGeneratedCollection(
        name=name, dataframe=dataframe, unique_column_names=unique_column_names
    )

    return UnqualifiedGeneratedCollection(dataframe_collection)
