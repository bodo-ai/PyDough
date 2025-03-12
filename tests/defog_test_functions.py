__all__ = [
    "impl_defog_broker_adv1",
    "impl_defog_broker_adv10",
    "impl_defog_broker_adv11",
    "impl_defog_broker_adv12",
    "impl_defog_broker_adv13",
    "impl_defog_broker_adv14",
    "impl_defog_broker_adv15",
    "impl_defog_broker_adv16",
    "impl_defog_broker_adv2",
    "impl_defog_broker_adv3",
    "impl_defog_broker_adv4",
    "impl_defog_broker_adv5",
    "impl_defog_broker_adv6",
    "impl_defog_broker_adv7",
    "impl_defog_broker_adv8",
    "impl_defog_broker_adv9",
    "impl_defog_broker_basic1",
    "impl_defog_broker_basic10",
    "impl_defog_broker_basic2",
    "impl_defog_broker_basic3",
    "impl_defog_broker_basic4",
    "impl_defog_broker_basic5",
    "impl_defog_broker_basic6",
    "impl_defog_broker_basic7",
    "impl_defog_broker_basic8",
    "impl_defog_broker_basic9",
]

import datetime

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


def impl_defog_broker_adv2():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the 2 most frequently bought stock ticker symbols in the past 10
    days? Return the ticker symbol and number of buy transactions.
    """
    selected_txns = transactions_of.WHERE(
        (transaction_type == "buy")
        & (date_time >= DATETIME("now", "-10 days", "start of day"))
    )
    ticker_info = Tickers.CALCULATE(symbol, tx_count=COUNT(selected_txns))
    return ticker_info.TOP_K(2, by=tx_count.DESC())


def impl_defog_broker_adv3():
    """
    PyDough implementation of the following question for the Broker graph:

    For customers with at least 5 total transactions, what is their transaction
    success rate? Return the customer name and success rate, ordered from
    lowest to highest success rate.
    """
    n_transactions = COUNT(transactions_made)
    n_success = SUM(transactions_made.status == "success")
    return (
        Customers.WHERE(n_transactions >= 5)
        .CALCULATE(name, success_rate=100.0 * n_success / n_transactions)
        .ORDER_BY(success_rate.ASC(na_pos="first"))
    )


def impl_defog_broker_adv4():
    """
    PyDough implementation of the following question for the Broker graph:

    Which 3 distinct stocks had the highest price change between the low and
    high from April 1 2023 to April 4 2023? I want the different in the low and
    high throughout this timerange, not just the intraday price changes. Return
    the ticker symbol and price change.
    """
    selected_prices = historical_prices.WHERE(
        (date >= datetime.date(2023, 4, 1)) & (date <= datetime.date(2023, 4, 4))
    ).CALCULATE(ticker_symbol=ticker.symbol)
    ticker_info = Tickers.CALCULATE(
        symbol,
        low=MIN(selected_prices.low),
        high=MAX(selected_prices.high),
    )
    return ticker_info.CALCULATE(symbol, price_change=high - low).TOP_K(
        3, price_change.DESC()
    )


def impl_defog_broker_adv5():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the ticker symbol, month, average closing price, highest price,
    lowest price, and MoMC for each ticker by month? MoMC = month-over-month
    change in average closing price, which is calculated as:
    (avg_close_given_month - avg_close_previous_month) /
    avg_close_previous_month for each ticker symbol each month.
    """
    price_info = DailyPrices.CALCULATE(
        month=JOIN_STRINGS("-", YEAR(date), LPAD(MONTH(date), 2, "0")),
        symbol=ticker.symbol,
    )
    ticker_months = PARTITION(price_info, name="updates", by=(symbol, month))
    months = PARTITION(ticker_months, name="months", by=symbol).months
    month_stats = months.CALCULATE(
        avg_close=AVG(updates.close),
        max_high=MAX(updates.high),
        min_low=MIN(updates.low),
    )
    prev_month_avg_close = PREV(avg_close, by=month.ASC(), levels=1)
    return month_stats.CALCULATE(
        symbol,
        month,
        avg_close,
        max_high,
        min_low,
        momc=(avg_close - prev_month_avg_close) / prev_month_avg_close,
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


def impl_defog_broker_adv7():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the PMCS and PMAT for customers who signed up in the last 6 months
    excluding the current month? PMCS = per month customer signups. PMAT = per
    month average transaction amount. Truncate date to month for aggregation.
    """
    selected_customers = Customers.WHERE(
        (join_date >= DATETIME("now", "-6 months", "start of month"))
        & (join_date < DATETIME("now", "start of month"))
    ).CALCULATE(
        join_year=YEAR(join_date),
        join_month=MONTH(join_date),
        month=JOIN_STRINGS("-", YEAR(join_date), LPAD(MONTH(join_date), 2, "0")),
    )
    months = PARTITION(selected_customers, name="custs", by=month)
    selected_txns = custs.transactions_made.WHERE(
        (YEAR(date_time) == join_year) & (MONTH(date_time) == join_month)
    )
    return months.CALCULATE(
        month,
        customer_signups=COUNT(custs),
        avg_tx_amount=AVG(selected_txns.amount),
    )


def impl_defog_broker_adv8():
    """
    PyDough implementation of the following question for the Broker graph:

    How many transactions were made by customers from the USA last week
    (exclusive of the current week)? Return the number of transactions and
    total transaction amount.
    """
    is_american = HAS(customer.WHERE(LOWER(country) == "usa"))
    selected_txns = Transactions.WHERE(
        is_american
        & (date_time < DATETIME("now", "start_of_week"))
        & (date_time >= DATETIME("now", "start_of_week", "-1 week"))
    )
    return Broker.CALCULATE(
        n_transactions=COUNT(selected_txns),
        total_amount=SUM(selected_txns.amount),
    )


def impl_defog_broker_adv9():
    """
    PyDough implementation of the following question for the Broker graph:

    How many transactions for stocks occurred in each of the last 8 weeks
    excluding the current week? How many of these transactions happened on
    weekends? Weekend days are Saturday and Sunday. Truncate date to week for
    aggregation.
    """
    selected_transactions = Transactions.WHERE(
        (date_time < DATETIME("now", "start_of_week"))
        & (date_time >= DATETIME("now", "start_of_week", "-8 weeks"))
    ).CALCULATE(
        week=DATETIME(date_time, "start of week"),
        is_weekend=ISIN(DAYOFWEEK(date_time), (0, 6)),
    )
    weeks = PARTITION(selected_transactions, name="txns", by=week)
    return weeks.CALCULATE(
        week,
        num_transactions=COUNT(txns),
        weekend_transactions=SUM(txns.is_weekend),
    )


def impl_defog_broker_adv10():
    """
    PyDough implementation of the following question for the Broker graph:

    Which customer made the highest number of transactions in the same month as
    they signed up? Return the customer's id, name and number of transactions.
    """
    cust_info = Customers.CALCULATE(
        join_year=YEAR(join_date), join_month=MONTH(join_date)
    )
    selected_txns = transactions_made.WHERE(
        (YEAR(date_time) == join_year) & (MONTH(date_time) == join_month)
    )
    return cust_info.CALCULATE(_id, name, num_transactions=COUNT(selected_txns)).TOP_K(
        1, by=num_transactions.DESC()
    )


def impl_defog_broker_adv11():
    """
    PyDough implementation of the following question for the Broker graph:

    How many distinct customers with a .com email address bought stocks of
    FAANG companies (Amazon, Apple, Google, Meta or Netflix)?
    """
    faang = ("AMZN", "AAPL", "GOOGL", "META", "NFLX")
    selected_customers = Customers.WHERE(
        ENDSWITH(email, ".com")
        & HAS(transactions_made.WHERE(ISIN(ticker.symbol, faang)))
    )
    return Broker.CALCULATE(n_customers=COUNT(selected_customers))


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
    return Broker.CALCULATE(n_customers=COUNT(selected_customers))


def impl_defog_broker_adv13():
    """
    PyDough implementation of the following question for the Broker graph:

    How many TAC are there from each country, for customers who joined on or
    after January 1, 2023? Return the country and the count. TAC = Total Active
    Customers who joined on or after January 1, 2023.
    """
    selected_customers = Customers.WHERE(join_date >= datetime.date(2023, 1, 1))
    countries = PARTITION(selected_customers, name="custs", by=country)
    return countries.CALCULATE(cust_country=country, TAC=COUNT(custs))


def impl_defog_broker_adv14():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the ACP for each ticker type in the past 7 days, inclusive of
    today? Return the ticker type and the average closing price.
    ACP = Average Closing Price of tickers in the last 7 days, inclusive of
    today.
    """
    selected_updates = DailyPrices.WHERE(
        DATEDIFF("days", date, DATETIME("now")) <= 7
    ).CALCULATE(ticker_type=ticker.ticker_type)

    ticker_types = PARTITION(selected_updates, name="updates", by=ticker_type)

    return ticker_types.CALCULATE(ticker_type, ACP=AVG(updates.close))


def impl_defog_broker_adv15():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the AR for each country for customers who joined in 2022? Return
    the country and AR. AR (Activity Ratio) = (Number of Active Customers with
    Transactions / Total Number of Customers with Transactions) * 100.
    """
    selected_customers = Customers.WHERE(
        (join_date >= "2022-01-01") & (join_date <= "2022-12-31")
    )
    countries = PARTITION(selected_customers, name="custs", by=country)
    n_active = SUM(custs.status == "active")
    n_custs = COUNT(custs)
    return countries.CALCULATE(
        country,
        ar=100 * DEFAULT_TO(n_active / n_custs, 0.0),
    )


def impl_defog_broker_adv16():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the SPM for each ticker symbol from sell transactions in the past
    month, inclusive of 1 month ago? Return the ticker symbol and SPM.
    SPM (Selling Profit Margin) = (Total Amount from Sells - (Tax + Commission))
    / Total Amount from Sells * 100
    """
    selected_txns = transactions_of.WHERE(
        (transaction_type == "sell") & (date_time >= DATETIME("now", "-1 month"))
    )
    spm = (
        100.0
        * (
            SUM(selected_txns.amount)
            - SUM(selected_txns.tax + selected_txns.commission)
        )
        / SUM(selected_txns.amount)
    )
    return Tickers.CALCULATE(symbol, SPM=spm).WHERE(PRESENT(SPM)).ORDER_BY(symbol.ASC())


def impl_defog_broker_basic1():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 5 countries by total transaction amount in the past 30
    days, inclusive of 30 days ago? Return the country name, number of
    transactions and total transaction amount.
    """
    counries = PARTITION(Customers, name="custs", by=country)
    selected_txns = custs.transactions_made.WHERE(
        date_time >= DATETIME("now", "-30 days", "start of day")
    )
    return counries.CALCULATE(
        country,
        num_transactions=COUNT(selected_txns),
        total_amount=SUM(selected_txns.amount),
    )


def impl_defog_broker_basic2():
    """
    PyDough implementation of the following question for the Broker graph:

    How many distinct customers made each type of transaction between Jan 1,
    2023 and Mar 31, 2023 (inclusive of start and end dates)? Return the
    transaction type, number of distinct customers and average number of
    shares, for the top 3 transaction types by number of customers.
    """
    selected_txns = Transactions.WHERE(
        (date_time >= datetime.date(2023, 1, 1))
        & (date_time <= datetime.date(2023, 3, 31))
    )
    txn_types = PARTITION(selected_txns, name="txns", by=transaction_type)
    return txn_types.CALCULATE(
        transaction_type,
        num_customers=NDISTINCT(txns.customer_id),
        avg_shares=AVG(txns.shares),
    ).TOP_K(3, by=num_customers.DESC())


def impl_defog_broker_basic3():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 10 ticker symbols by total transaction amount? Return the
    ticker symbol, number of transactions and total transaction amount.
    """
    return Tickers.CALCULATE(
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
    data = Customers.CALCULATE(state=state).transactions_made.ticker
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


def impl_defog_broker_basic6():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the distinct list of ticker IDs that have daily price records on or
    after Apr 1, 2023.
    """
    selected_price_records = historical_prices.WHERE(date >= datetime.date(2023, 4, 1))
    return Tickers.CALCULATE(_id).WHERE(HAS(selected_price_records))


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
