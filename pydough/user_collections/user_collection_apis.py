"""
Implementation of User Collection APIs in PyDough.
"""

__all__ = ["range_collection"]

from pydough.unqualified.unqualified_node import UnqualifiedGeneratedCollection
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
