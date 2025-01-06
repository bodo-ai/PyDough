"""
TODO
"""

__all__ = [
    "impl_defog_broker_adv1",
    "impl_defog_broker_basic3",
    "impl_defog_broker_basic4",
]

# ruff: noqa
# mypy: ignore-errors
# ruff & mypy should not try to typecheck or verify any of this


def impl_defog_broker_adv1():
    """
    PyDough implementation of the following question for the Broker graph:

    Who are the top 5 customers by total transaction amount? Return their name
    and total amount.
    """
    return Customers(name, total_amount=SUM(transactions_made.amount)).TOP_K(
        5, by=total_amount.DESC()
    )


def impl_defog_broker_basic3():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the 2 most frequently bought stock ticker symbols in the past 10
    days? Return the ticker symbol and number of buy transactions.
    """
    return Tickers(
        symbol,
        num_transactions=COUNT(transactions_of),
        total_amount=SUM(transactions_of.amount),
    ).TOP_K(10, by=total_amount.DESC())


def impl_defog_broker_basic4():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 5 combinations of customer state and ticker type by
    number of transactions? Return the customer state, ticker type and number
    of transactions.
    """
    data = Customers.transactions_made.ticker(state=BACK(2).state)
    return PARTITION(data, name="entries", by=(state, ticker_type))(
        state,
        ticker_type,
        num_transactions=COUNT(entries),
    ).TOP_K(5, by=num_transactions.DESC())
