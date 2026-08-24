"""
Various functions containing user generated collections as
PyDough code snippets for testing purposes.
"""
# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this

import pydough


def template_call():
    return orders_revenue_by(1996, customer.market_segment)


def template_api():
    return pydough.call_template(
        "orders_revenue_by",
        labels={"arg_year": "Year 1996", "arg_dimension": "Customer Region"},
    )
