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
    "impl_defog_broker_gen1",
    "impl_defog_broker_gen2",
    "impl_defog_broker_gen3",
    "impl_defog_broker_gen4",
    "impl_defog_broker_gen5",
    "impl_defog_ewallet_adv1",
    "impl_defog_ewallet_adv10",
    "impl_defog_ewallet_adv11",
    "impl_defog_ewallet_adv12",
    "impl_defog_ewallet_adv13",
    "impl_defog_ewallet_adv14",
    "impl_defog_ewallet_adv15",
    "impl_defog_ewallet_adv16",
    "impl_defog_ewallet_adv2",
    "impl_defog_ewallet_adv3",
    "impl_defog_ewallet_adv4",
    "impl_defog_ewallet_adv5",
    "impl_defog_ewallet_adv6",
    "impl_defog_ewallet_adv7",
    "impl_defog_ewallet_adv8",
    "impl_defog_ewallet_adv9",
    "impl_defog_ewallet_basic1",
    "impl_defog_ewallet_basic10",
    "impl_defog_ewallet_basic2",
    "impl_defog_ewallet_basic3",
    "impl_defog_ewallet_basic4",
    "impl_defog_ewallet_basic5",
    "impl_defog_ewallet_basic6",
    "impl_defog_ewallet_basic7",
    "impl_defog_ewallet_basic8",
    "impl_defog_ewallet_basic9",
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
    selected_updates = DailyPrices.WHERE(DATEDIFF("days", date, "now") <= 7).CALCULATE(
        ticker_type=ticker.ticker_type
    )

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


def impl_defog_broker_gen1():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the lowest daily closest price for symbol `VTI` in the past 7
    days.
    """
    selected_prices = DailyPrices.WHERE(
        (ticker.symbol == "VTI") & (DATEDIFF("days", date, "now") <= 7)
    )

    return Broker.CALCULATE(lowest_price=MIN(selected_prices.close))


def impl_defog_broker_gen2():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the number of transactions by users who joined in the past 70
    days.
    """
    selected_tx = Transactions.WHERE(customer.join_date >= DATETIME("now", "-70 days"))

    return Broker.CALCULATE(transaction_count=COUNT(selected_tx.customer_id))


def impl_defog_broker_gen3():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer id and the difference between their time from
    joining to their first transaction. Ignore customers who haven't made
    any transactions.
    """
    selected_customers = Customers.WHERE(HAS(transactions_made))

    return selected_customers.CALCULATE(
        cust_id=_id,
        DaysFromJoinToFirstTransaction=(
            DATEDIFF("seconds", join_date, MIN(transactions_made.date_time))
        )
        / 86400.0,
    )


def impl_defog_broker_gen4():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer who made the most sell transactions on 2023-04-01.
    Return the id, name and number of transactions.
    """
    selected_transactions = transactions_made.WHERE(
        (DATETIME(date_time, "start of day") == datetime.date(2023, 4, 1))
        & (transaction_type == "sell")
    )
    return Customers.CALCULATE(_id, name, num_tx=COUNT(selected_transactions)).TOP_K(
        1, by=num_tx.DESC()
    )


def impl_defog_broker_gen5():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the monthly average transaction price for successful
    transactions in the 1st quarter of 2023?
    """
    selected_transactions = Transactions.WHERE(
        MONOTONIC(datetime.date(2023, 1, 1), date_time, datetime.date(2023, 3, 31))
        & (status == "success")
    ).CALCULATE(month=DATETIME(date_time, "start of month"))

    return (
        PARTITION(selected_transactions, name="m", by=(month))
        .CALCULATE(month=month, avg_price=AVG(m.price))
        .ORDER_BY(month.ASC())
    )


def impl_defog_ewallet_adv1():
    """
    PyDough implementation of the following question for the eWallet graph:

    Calculate the CPUR for each merchant, considering only successful
    transactions. Return the merchant name and CPUR. CPUR (coupon usage
    rate) = number of distinct coupons used / number of distinct transactions
    """
    # Filter the transactions to get only successful ones
    successful_transactions = transactions_received.WHERE(status == "success")

    # Calculate the CPUR for the merchant
    return Merchants.WHERE(HAS(successful_transactions)).CALCULATE(
        name=name,
        CPUR=NDISTINCT(successful_transactions.coupon_id)
        * 1.0
        / NDISTINCT(successful_transactions.txid),
    )


def impl_defog_ewallet_adv2():
    """
    PyDough implementation of the following question for the eWallet graph:

    For users in the US and Canada, how many total notifications were sent in
    each of the last 3 weeks excluding the current week? How many of those
    were sent on weekends? Weekends are Saturdays and Sundays. Truncate
    created_at to week for aggregation.
    """
    past_notifs = (
        Users.WHERE(ISIN(country, ("US", "CA")))
        .notifications.WHERE(
            (created_at < DATETIME("now", "start_of_week"))
            & (created_at >= DATETIME("now", "start_of_week", "-3 weeks"))
        )
        .CALCULATE(
            week=DATETIME(created_at, "start of week"),
            is_weekend=ISIN(DAYOFWEEK(created_at), (5, 6)),
        )
    )
    weeks = PARTITION(past_notifs, name="notifs", by=week)
    return weeks.CALCULATE(
        week,
        num_notifs=COUNT(past_notifs),
        weekend_notifs=SUM(past_notifs.is_weekend),
    )


def impl_defog_ewallet_adv3():
    """
    PyDough implementation of the following question for the eWallet graph:

    How many active retail merchants have issued coupons? Return the merchant
    name and the total number of coupons issued. Merchant category should be
    matched case-insensitively.
    """
    # Retrieve merchant summary for active merchants in the "retail" category who have coupons
    return Merchants.WHERE(
        (status == "active") & (CONTAINS(LOWER(category), "%retail%")) & HAS(coupons)
    ).CALCULATE(merchant_name=name, total_coupons=COUNT(coupons))


def impl_defog_ewallet_adv4():
    """
    PyDough implementation of the following question for the eWallet graph:

    How many wallet transactions were made by users from the US in the last 7
    days inclusive of today? Return the number of transactions and total
    transaction amount. Last 7 days = DATE('now', -'7 days') to DATE('now').
    Always join wallet_transactions_daily with users before using the
    wallet_transactions_daily table.
    """
    # Filter transactions based on the creation date and sending user's country
    us_transactions = Transactions.WHERE(
        (DATEDIFF("days", created_at, "now") <= 7) & (sending_user.country == "US")
    )
    # Calculate the number of transactions and the total amount for the filtered transactions
    return Ewallet.CALCULATE(
        num_transactions=COUNT(us_transactions),
        total_amount=KEEP_IF(SUM(us_transactions.amount), COUNT(us_transactions) > 0),
    )


def impl_defog_ewallet_adv5():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the average AMB for user wallets updated in the past week,
    inclusive of 7 days ago? Return the average balance. AMB = average balance
    per user (for the given time duration).
    """
    selected_user_balances = UserBalances.WHERE(
        DATEDIFF("days", updated_at, "now") <= 7
    )

    return Ewallet.CALCULATE(AMB=AVG(selected_user_balances.balance))


def impl_defog_ewallet_adv6():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the LUB for each user. LUB = Latest User Balance, which is the most
    recent balance for each user
    """
    latest_balance_record = balances.WHERE(
        RANKING(by=updated_at.DESC(), levels=1) == 1
    ).SINGULAR()

    return Users.WHERE(HAS(balances)).CALCULATE(
        user_id=uid, latest_balance=latest_balance_record.balance
    )


def impl_defog_ewallet_adv7():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the marketing opt-in preference for each user? Return the user ID
    and boolean opt-in value. To get any user's settings, only select the
    latest snapshot of user_setting_snapshot for each user.
    """
    latest_snapshot = setting_snapshots.WHERE(
        RANKING(by=created_at.DESC(), levels=1) == 1
    ).SINGULAR()

    return Users.WHERE(HAS(latest_snapshot)).CALCULATE(
        uid=uid, marketing_opt_in=latest_snapshot.marketing_opt_in
    )


def impl_defog_ewallet_adv8():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the MRR for each merchant? Return the merchant name, category,
    revenue amount, and revenue rank. MRR = Merchant Revenue Rank, which ranks
    merchants based on amounts from successfully received transactions only.
    """
    successful_transactions = transactions_received.WHERE(
        (receiver_type == 1) & (status == "success")
    )
    transaction_SUM = SUM(successful_transactions.amount)

    return Merchants.WHERE(HAS(successful_transactions)).CALCULATE(
        merchants_id=mid,
        merchants_name=name,
        category=category,
        total_revenue=transaction_SUM,
        mrr=RANKING(by=transaction_SUM.DESC(), levels=1),
    )


def impl_defog_ewallet_adv9():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the PMDAU (Per Month Daily Active Users) for wallet transactions in
    the last 2 months excluding the current month? PMDAU (Per Month Daily
    Active Users) = COUNT(DISTINCT(sender_id) ... WHERE t.sender_type = 0.
    Truncate created_at to month for aggregation.

    """
    # Define the start date (2 months before the start of the current month)
    start_date = DATETIME("now", "start of month", "-2 months")

    # Define the end date (start of the current month)
    end_date = DATETIME("now", "start of month")

    # Filter successful transactions based on sender type and creation date range
    successful_transactions = Transactions.WHERE(
        (sender_type == 0) & (created_at >= start_date) & (created_at < end_date)
    ).CALCULATE(year_month=DATETIME(created_at, "start of month"))

    # Group transactions by month and calculate the number of distinct active users
    return PARTITION(successful_transactions, name="transc", by=year_month).CALCULATE(
        year_month=year_month, active_users=NDISTINCT(transc.sender_id)
    )


def impl_defog_ewallet_adv10():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the total number of wallet transactions sent by each user that is
    not a merchant? Return the user ID and total transaction count.
    """
    successful_transactions = transactions_sent.WHERE(sender_type == 0)
    # Group users who have successful transactions and calculate the number of distinct transactions per user
    return Users.WHERE(HAS(successful_transactions)).CALCULATE(
        user_id=uid, total_transactions=COUNT(successful_transactions)
    )


def impl_defog_ewallet_adv11():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the total session duration in seconds for each user between
    2023-06-01 inclusive and 2023-06-08 exclusive? Return the user ID and their
    total duration as an integer sorted by total duration with the longest
    duration first.
    """
    selected_sessions = sessions.WHERE(
        (session_start_ts >= "2023-06-01") & (session_end_ts < "2023-06-08")
    ).CALCULATE(
        duration=DATEDIFF("seconds", session_start_ts, session_end_ts)
    )  # Pydough cannot convert dates to seconds directly, DATEDIFF

    # Calculate the total session duration for each user and order by the total duration in descending order
    return (
        Users.CALCULATE(uid=uid, total_duration=SUM(selected_sessions.duration))
        .ORDER_BY(total_duration.DESC())
        .WHERE(HAS(selected_sessions))
    )


def impl_defog_ewallet_adv12():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the total transaction amount for each coupon offered by merchant
    with ID 1? Return the coupon ID and total amount transacted with it.
    """
    return Coupons.WHERE(merchant_id == "1").CALCULATE(
        coupon_id=cid, total_discount=SUM(transaction_used_in.amount)
    )


def impl_defog_ewallet_adv13():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the TUC in the past month, inclusive of 1 month ago? Return the
    total count. TUC = Total number of user sessions in the past month
    """
    selected_sessions = UserSessions.WHERE(
        session_start_ts >= DATETIME("now", "-1 month", "start of day")
    )

    return Ewallet.CALCULATE(TUC=COUNT(selected_sessions))


def impl_defog_ewallet_adv14():
    """
    PyDough implementation of the following question for the eWallet graph:

    What was the STR for wallet transactions in the previous month? STR
    (success transaction rate) = number of successful transactions / total
    number of transactions.
    """
    past_month_transactions = Transactions.WHERE(
        DATEDIFF("months", created_at, "now") == 1
    )

    successful_transactions = past_month_transactions.WHERE(status == "success")

    return Ewallet.CALCULATE(
        SUM(past_month_transactions.status == "success")
        / COUNT(past_month_transactions)
    )


def impl_defog_ewallet_adv15():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which merchant created the highest number of coupons within the same month
    that the merchant was created (coupon or merchant can be created earlier
    than the other)? Return the number of coupons along with the merchant's id
    and name.
    """
    return Merchants.CALCULATE(
        merchant_id=mid,
        merchant_name=name,
        coupons_per_merchant=COUNT(
            coupons.WHERE(DATEDIFF("months", merchant.created_at, created_at) == 0)
        ),
    ).TOP_K(1, by=coupons_per_merchant.DESC())


def impl_defog_ewallet_adv16():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which users from the US have unread promotional notifications? Return the
    username and the total number of unread promotional notifications. User
    country should be matched case-insensitively, e.g., LOWER(users.country) =
    'us'. Notification type and status should be matched exactly.
    """
    unread_notifs = notifications.WHERE(
        (notification_type == "promotion") & (status == "unread")
    )

    return Users.WHERE((LOWER(country) == "us") & HAS(unread_notifs)).CALCULATE(
        username=username, total_unread_notifs=COUNT(unread_notifs)
    )


def impl_defog_ewallet_basic1():
    """
    PyDough implementation of the following question for the eWallet graph:

    How many distinct active users sent money per month in 2023? Return the
    number of active users per month (as a date), starting from the earliest
    date. Do not include merchants in the query. Only include successful
    transactions.
    """
    selected_transactions = Transactions.WHERE(
        (status == "success")
        & (YEAR(created_at) == 2023)
        & (sending_user.status == "active")
        & (sender_type == 0)
    ).CALCULATE(month=DATETIME(created_at, "start of month"))

    return PARTITION(selected_transactions, name="t", by=month).CALCULATE(
        month, active_users=NDISTINCT(t.sender_id)
    )


def impl_defog_ewallet_basic10():
    """
    PyDough implementation of the following question for the eWallet graph:

    Who are the top 2 merchants (receiver type 1) by total transaction amount
    in the past 150 days (inclusive of 150 days ago)? Return the merchant name,
    total number of transactions, and total transaction amount.
    """
    selected_transactions = transactions_received.WHERE(
        (receiver_type == 1)
        & (created_at >= DATETIME("now", "-150 days", "start of day"))
    )

    return Merchants.CALCULATE(
        merchant_name=name,
        total_transactions=COUNT(selected_transactions),
        total_amount=SUM(selected_transactions.amount),
    ).TOP_K(2, by=total_amount.DESC())


def impl_defog_ewallet_basic2():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return merchants (merchant ID and name) who have not issued any coupons.
    """
    return Merchants.WHERE(HASNOT(coupons)).CALCULATE(
        merchant_id=mid, merchant_name=name
    )


def impl_defog_ewallet_basic3():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return the distinct list of merchant IDs that have received money from a
    transaction. Consider all transaction types in the results you return,
    but only include the merchant ids in your final answer.
    """
    return Merchants.WHERE(
        HAS(transactions_received.WHERE(receiver_type == 1))
    ).CALCULATE(merchant=mid)


def impl_defog_ewallet_basic4():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return the distinct list of user IDs who have received transaction
    notifications.
    """
    return Users.WHERE(
        HAS(notifications.WHERE(notification_type == "transaction"))
    ).CALCULATE(user_id=uid)


def impl_defog_ewallet_basic5():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return users (user ID and username) who have not received any
    notifications.
    """
    return Users.WHERE(HASNOT(notifications)).CALCULATE(uid, username)


def impl_defog_ewallet_basic6():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 2 most frequently used device types for user sessions
    and their respective counts?
    """
    selected_sessions = PARTITION(
        UserSessions, name="usession", by=device_type
    ).CALCULATE(device_type=device_type, count=COUNT(usession))

    return selected_sessions.TOP_K(2, count.DESC())


def impl_defog_ewallet_basic7():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 3 most common transaction statuses and their respective
    counts?
    """
    return (
        PARTITION(Transactions, name="t", by=status)
        .CALCULATE(status=status, count=COUNT(t))
        .TOP_K(3, count.DESC())
    )


def impl_defog_ewallet_basic8():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 3 most frequently used coupon codes? Return the coupon
    code, total number of redemptions, and total amount redeemed.
    """
    return Coupons.CALCULATE(
        coupon_code=code,
        redemption_count=COUNT(transaction_used_in.txid),
        total_discount=SUM(transaction_used_in.amount),
    ).TOP_K(3, redemption_count.DESC())


def impl_defog_ewallet_basic9():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which are the top 5 countries by total transaction amount sent by users,
    sender_type = 0? Return the country, number of distinct users who sent,
    and total transaction amount.
    """
    transactions_by_sending_users = (
        Transactions.WHERE(sender_type == 0)
        .CALCULATE(sender_id=sender_id, amount=amount)
        .sending_user
    )

    return (
        PARTITION(transactions_by_sending_users, name="t", by=country)
        .CALCULATE(
            country=country,
            user_count=NDISTINCT(t.sender_id),
            total_amount=SUM(t.amount),
        )
        .TOP_K(5, total_amount.DESC())
    )


def impl_defog_ewallet_gen1():
    """
    PyDough implementation of the following question for the eWallet graph:

    Give me today's median merchant wallet balance for all active merchants
    whose category contains 'retail'
    """
    active_merchants = Merchants.WHERE(
        (CONTAINS(LOWER(category), "%retail%")) & (status == "active")
    )

    latest_balance_today = balances.WHERE(
        DATE(updated_at)
        == DATE(DATETIME("now")) & (RANKING(updated_at.DESC(), levels=1) == 1)
    )

    return Ewallet.CALCULATE(MEDIAN(active_merchants.latest_balance_today.balance))


def impl_defog_ewallet_gen2():
    """
    PyDough implementation of the following question for the eWallet graph:

    What was the average transaction daily and monthly limit for the earliest
    setting snapshot in 2023?
    """
    snapshots_2023 = UserSettingSnapshots.WHERE(YEAR(snapshot_date) == 2023)

    return Ewallet.CALCULATE(min_date=MIN(snapshots_2023.snapshot_date)).CALCULATE(
        avg_daily_limit=AVG(
            snapshots_2023.WHERE(min_date == snapshot_date).tx_limit_daily
        ),
        avg_monthly_limit=AVG(
            snapshots_2023.WHERE(min_date == snapshot_date).tx_limit_monthly
        ),
    )


def impl_defog_ewallet_gen3():
    """
    PyDough implementation of the following question for the eWallet graph:

    what was the average user session duration in seconds split by device_type?
    """
    selected_user_sessions = UserSessions

    return PARTITION(selected_user_sessions, name="usession", by=device_type).CALCULATE(
        device_type=device_type,
        avg_session_duration_seconds=AVG(
            DATEDIFF("seconds", usession.session_start_ts, usession.session_end_ts)
        ),
    )


def impl_defog_ewallet_gen4():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which merchants earliest coupon start date was within a year of the
    merchant's registration? Return the merchant id, registration date, and
    earliest coupon id and start date
    """
    selected_coupons = (
        Merchants.CALCULATE(
            merchant_registration_date=created_at,
            merchants_id=mid,
            earliest_coupon_start_date=MIN(coupons.start_date),
        )
        .CALCULATE(
            earliest_coupon_id=MAX(
                coupons.WHERE(earliest_coupon_start_date == start_date).cid
            )
        )
        .coupons.WHERE(start_date <= DATETIME(merchant_registration_date, "+1 year"))
    )

    return selected_coupons.CALCULATE(
        merchants_id,
        merchant_registration_date,
        earliest_coupon_start_date,
        earliest_coupon_id,
    )


def impl_defog_ewallet_gen5():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which users did not get a notification within the first year of signing up?
    Return their usernames, emails and signup dates.
    """
    return Users.WHERE(
        HASNOT(
            notifications.WHERE(
                (created_at >= user.created_at)
                & (DATETIME(user.created_at, "+1 year") >= created_at)
            )
        )
    ).CALCULATE(username=username, email=email, created_at=created_at)
