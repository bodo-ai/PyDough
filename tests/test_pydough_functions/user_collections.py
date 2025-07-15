"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pandas as pd
import datetime

import pydough


def simple_range():
    return pydough.range_collection(
        "simple_range",
        "value",
        10,  # end value
    ).ORDER_BY(value.ASC())
