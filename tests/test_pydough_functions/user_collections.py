"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pydough


def simple_range_1():
    # Generates a table with column named `value` containing integers from 0 to 9.
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    )


def simple_range_2():
    # Generates a table with column named `value` containing integers from 0 to 9,
    # ordered in descending order.
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    ).ORDER_BY(value.DESC())


def simple_range_3():
    # Generates a table with column named `foo` containing integers from 15 to
    # 20 exclusive, ordered in ascending order.
    return pydough.range_collection("T1", "foo", 15, 20).ORDER_BY(foo.ASC())


def simple_range_4():
    #  Generate a table with 1 column named `N` counting backwards
    # from 10 to 1 (inclusive)
    return pydough.range_collection("T2", "N", 10, 0, -1).ORDER_BY(N.ASC())


def simple_range_5():
    # Generate a table with 1 column named `x` which is an empty range
    return pydough.range_collection("T3", "x", -1)


def user_range_collection_1():
    # Creates a collection `sizes` with a single property `part_size` whose values are the
    # integers from 1 (inclusive) to 100 (exclusive), skipping by 5s, then for each size value,
    # counts how many turquoise parts have that size.
    sizes = pydough.range_collection("sizes", "part_size", 1, 100, 5)
    turquoise_parts = parts.WHERE(CONTAINS(name, "turquoise"))
    return sizes.CALCULATE(part_size).CALCULATE(
        part_size, n_parts=COUNT(CROSS(turquoise_parts).WHERE(size == part_size))
    )


def user_range_collection_2():
    # Generate two tables with one column: `a` has a column `x` of digits 0-9,
    # `b` has a column `y` of every even number from 0 to 1000 (inclusive), and for
    # every row of `a` count how many rows of `b` have `x` has a prefix of `y`, and
    # how many have `x` as a suffix of `y`
    table_a = pydough.range_collection("a", "x", 10)
    table_b = pydough.range_collection("b", "y", 0, 1001, 2)
    result = (
        table_a.CALCULATE(x)
        .CALCULATE(
            x,
            n_prefix=COUNT(CROSS(table_b).WHERE(STARTSWITH(STRING(y), STRING(x)))),
            n_suffix=COUNT(CROSS(table_b).WHERE(ENDSWITH(STRING(y), STRING(x)))),
        )
        .ORDER_BY(x.ASC())
    )
    return result
