"""
Implementation of User Collection APIs in PyDough.
"""

__all__ = ["range_collection"]

from pydough.unqualified.unqualified_node import UnqualifiedGeneratedCollection


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
    if len(args) == 1:
        end = args[0]
        start = 0
        step = 1
    elif len(args) == 2:
        start, end = args
        step = 1
    elif len(args) == 3:
        start, end, step = args
    else:
        raise ValueError(f"Expected 1 to 3 arguments, got {len(args)}")
    if not isinstance(start, int):
        raise TypeError(
            f"Expected 'start' to be an integer, got {type(start).__name__}"
        )
    if not isinstance(end, int):
        raise TypeError(f"Expected 'end' to be an integer, got {type(end).__name__}")
    if not isinstance(step, int):
        raise TypeError(f"Expected 'step' to be an integer, got {type(step).__name__}")
    if start >= end:
        raise ValueError(f"Expected 'start' ({start}) to be less than 'end' ({end})")
    if step == 0:
        raise ValueError("Expected 'step' to be a non-zero integer")
    if start < 0:
        raise ValueError(f"Expected 'start' to be a non-negative integer, got {start}")
    if end < 0:
        raise ValueError(f"Expected 'end' to be a non-negative integer, got {end}")
    # TODO: support negative step values
    if step <= 0:
        raise ValueError(f"Expected 'step' to be a positive integer, got {step}")

    range_args = [start, end, step]
    return UnqualifiedGeneratedCollection(
        name,
        [
            column,
        ],
        range_args,
    )
