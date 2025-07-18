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
