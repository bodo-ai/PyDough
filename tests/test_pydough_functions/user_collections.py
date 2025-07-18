"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pydough


def simple_range_1():
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    )


def simple_range_2():
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    ).ORDER_BY(value.DESC())


def simple_range_3():
    # Creates a collection `sizes` with a single property `part_size` whose values are the
    # integers from 1 (inclusive) to 100 (exclusive), skipping by 5s, then for each size value,
    # counts how many turquoise parts have that size.
    sizes = pydough.range_collection("sizes", "part_size", 1, 100, 5)
    turquoise_parts = parts.WHERE(CONTAINS(name, "turquoise"))
    return sizes.CALCULATE(part_size).CALCULATE(
        part_size, n_parts=COUNT(CROSS(turquoise_parts).WHERE(size == part_size))
    )
