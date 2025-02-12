__all__ = [
    "impl_defog_broker_adv1",
    "impl_defog_broker_adv11",
    "impl_defog_broker_adv12",
    "impl_defog_broker_adv15",
    "impl_defog_broker_adv3",
    "impl_defog_broker_adv6",
    "impl_defog_broker_basic10",
    "impl_defog_broker_basic3",
    "impl_defog_broker_basic4",
    "impl_defog_broker_basic5",
    "impl_defog_broker_basic7",
    "impl_defog_broker_basic8",
    "impl_defog_broker_basic9",
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
    return Customers.CALCULATE(name, total_amount=SUM(transactions_made.amount)).TOP_K(
        5, by=total_amount.DESC()
    )


def impl_defog_broker_adv3():
    """
    PyDough implementation of the following question for the Broker graph:

    For customers with at least 5 total transactions, what is their transaction success rate? Return the customer name and success rate, ordered from lowest to highest success rate.
    """
    n_transactions = COUNT(transactions_made)
    n_success = SUM(transactions_made.status == "success")
    return (
        Customers.WHERE(n_transactions >= 5)
        .CALCULATE(name, success_rate=100.0 * n_success / n_transactions)
        .ORDER_BY(success_rate.ASC(na_pos="first"))
    )


def impl_defog_broker_adv6():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer name, number of transactions, total transaction amount,
    and CR for all customers. CR = customer rank by total transaction amount,
    with rank 1 being the customer with the highest total transaction amount.
    """
    total_amount = SUM(transactions_made.amount)
    return Customers.WHERE(HAS(transactions_made)).CALCULATE(
        name,
        num_tx=COUNT(transactions_made),
        total_amount=total_amount,
        cust_rank=RANKING(by=total_amount.DESC(na_pos="last"), allow_ties=True),
    )


def impl_defog_broker_adv11():
    """
    PyDough implementation of the following question for the Broker graph:

    How many distinct customers with a .com email address bought stocks of
    FAANG companies (Amazon, Apple, Google, Meta or Netflix)?
    """
    faang = ("AMZN", "AAPL", "GOOGL", "META", "NFLX")
    return Broker(
        n_customers=COUNT(
            Customers.WHERE(
                ENDSWITH(email, ".com")
                & HAS(transactions_made.WHERE(ISIN(ticker.symbol, faang)))
            )
        )
    )


def impl_defog_broker_adv12():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the number of customers whose name starts with J or ends with
    'ez', and who live in a state ending with the letter 'a'?
    """
    selected_customers = Customers.WHERE(
        (STARTSWITH(LOWER(name), "j") | ENDSWITH(LOWER(name), "ez"))
        & ENDSWITH(LOWER(state), "a")
    )
    return Broker(n_customers=COUNT(selected_customers))


def impl_defog_broker_adv15():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the AR for each country for customers who joined in 2022? Return the country and AR. AR (Activity Ratio) = (Number of Active Customers with Transactions / Total Number of Customers with Transactions) * 100.
    """
    selected_customers = Customers.WHERE(
        (join_date >= "2022-01-01") & (join_date <= "2022-12-31")
    )
    countries = PARTITION(selected_customers, name="custs", by=country)
    n_active = SUM(custs.status == "active")
    n_custs = COUNT(custs)
    return countries(
        country,
        ar=100 * DEFAULT_TO(n_active / n_custs, 0.0),
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
    number of transactions? Return the customer state, ticker type and number of transactions.
    """
    data = Customers.transactions_made.ticker(state=BACK(2).state)
    return (
        PARTITION(data, name="combo", by=(state, ticker_type))
        .CALCULATE(
            state,
            ticker_type,
            num_transactions=COUNT(combo),
        )
        .TOP_K(5, by=num_transactions.DESC())
    )


def impl_defog_broker_basic5():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the distinct list of customer IDs who have made a 'buy' transaction.
    """
    return Customers.WHERE(
        HAS(transactions_made.WHERE(transaction_type == "buy"))
    ).CALCULATE(_id)


def impl_defog_broker_basic7():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 3 transaction statuses by number of transactions? Return
    the status and number of transactions.
    """
    return (
        PARTITION(Transactions, name="status_group", by=status)
        .CALCULATE(status, num_transactions=COUNT(status_group))
        .TOP_K(3, by=num_transactions.DESC())
    )


def impl_defog_broker_basic8():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 5 countries by number of customers? Return the country
    name and number of customers.
    """
    return (
        PARTITION(Customers, name="custs", by=country)
        .CALCULATE(country, num_customers=COUNT(custs))
        .TOP_K(5, by=num_customers.DESC())
    )


def impl_defog_broker_basic9():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer ID and name of customers who have not made any
    transactions.
    """
    return Customers.WHERE(HASNOT(transactions_made)).CALCULATE(_id, name)


def impl_defog_broker_basic10():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the ticker ID and symbol of tickers that do not have any daily
    price records.
    """
    return Tickers.WHERE(HASNOT(historical_prices)).CALCULATE(_id, symbol)
