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
    "impl_defog_dealership_adv1",
    "impl_defog_dealership_adv10",
    "impl_defog_dealership_adv11",
    "impl_defog_dealership_adv12",
    "impl_defog_dealership_adv13",
    "impl_defog_dealership_adv14",
    "impl_defog_dealership_adv15",
    "impl_defog_dealership_adv16",
    "impl_defog_dealership_adv2",
    "impl_defog_dealership_adv3",
    "impl_defog_dealership_adv4",
    "impl_defog_dealership_adv5",
    "impl_defog_dealership_adv6",
    "impl_defog_dealership_adv7",
    "impl_defog_dealership_adv8",
    "impl_defog_dealership_adv9",
    "impl_defog_dealership_basic1",
    "impl_defog_dealership_basic10",
    "impl_defog_dealership_basic2",
    "impl_defog_dealership_basic3",
    "impl_defog_dealership_basic4",
    "impl_defog_dealership_basic5",
    "impl_defog_dealership_basic6",
    "impl_defog_dealership_basic7",
    "impl_defog_dealership_basic8",
    "impl_defog_dealership_basic9",
    "impl_defog_dealership_gen1",
    "impl_defog_dealership_gen2",
    "impl_defog_dealership_gen3",
    "impl_defog_dealership_gen4",
    "impl_defog_dealership_gen5",
    "impl_defog_dermtreatment_adv1",
    "impl_defog_dermtreatment_adv10",
    "impl_defog_dermtreatment_adv11",
    "impl_defog_dermtreatment_adv12",
    "impl_defog_dermtreatment_adv13",
    "impl_defog_dermtreatment_adv14",
    "impl_defog_dermtreatment_adv15",
    "impl_defog_dermtreatment_adv16",
    "impl_defog_dermtreatment_adv2",
    "impl_defog_dermtreatment_adv3",
    "impl_defog_dermtreatment_adv4",
    "impl_defog_dermtreatment_adv5",
    "impl_defog_dermtreatment_adv6",
    "impl_defog_dermtreatment_adv7",
    "impl_defog_dermtreatment_adv8",
    "impl_defog_dermtreatment_adv9",
    "impl_defog_dermtreatment_basic1",
    "impl_defog_dermtreatment_basic10",
    "impl_defog_dermtreatment_basic2",
    "impl_defog_dermtreatment_basic3",
    "impl_defog_dermtreatment_basic4",
    "impl_defog_dermtreatment_basic5",
    "impl_defog_dermtreatment_basic6",
    "impl_defog_dermtreatment_basic7",
    "impl_defog_dermtreatment_basic8",
    "impl_defog_dermtreatment_basic9",
    "impl_defog_dermtreatment_gen1",
    "impl_defog_dermtreatment_gen2",
    "impl_defog_dermtreatment_gen3",
    "impl_defog_dermtreatment_gen4",
    "impl_defog_dermtreatment_gen5",
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
    return customers.CALCULATE(name, total_amount=SUM(transactions_made.amount)).TOP_K(
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
    return tickers.CALCULATE(symbol, tx_count=COUNT(selected_txns)).TOP_K(
        2, by=tx_count.DESC()
    )


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
        customers.WHERE(n_transactions >= 5)
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
    selected_prices = daily_prices.WHERE(
        (date >= datetime.date(2023, 4, 1)) & (date <= datetime.date(2023, 4, 4))
    )
    return tickers.CALCULATE(
        symbol, price_change=MAX(selected_prices.high) - MIN(selected_prices.low)
    ).TOP_K(3, price_change.DESC())


def impl_defog_broker_adv5():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the ticker symbol, month, average closing price, highest price,
    lowest price, and MoMC for each ticker by month? MoMC = month-over-month
    change in average closing price, which is calculated as:
    (avg_close_given_month - avg_close_previous_month) /
    avg_close_previous_month for each ticker symbol each month.
    """
    prev_month_avg_close = PREV(avg_close, by=month.ASC(), per="symbol")
    return (
        daily_prices.CALCULATE(
            month=JOIN_STRINGS("-", YEAR(date), LPAD(MONTH(date), 2, "0")),
            symbol=ticker.symbol,
        )
        .PARTITION(name="months", by=(symbol, month))
        .CALCULATE(
            avg_close=AVG(daily_prices.close),
            max_high=MAX(daily_prices.high),
            min_low=MIN(daily_prices.low),
        )
        .PARTITION(name="symbol", by=symbol)
        .months.CALCULATE(
            symbol,
            month,
            avg_close,
            max_high,
            min_low,
            momc=(avg_close - prev_month_avg_close) / prev_month_avg_close,
        )
    )


def impl_defog_broker_adv6():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer name, number of transactions, total transaction amount,
    and CR for all customers. CR = customer rank by total transaction amount,
    with rank 1 being the customer with the highest total transaction amount.
    """
    total_amount = SUM(transactions_made.amount)
    return customers.WHERE(HAS(transactions_made)).CALCULATE(
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
    selected_txns = customers.CALCULATE(
        join_year=YEAR(join_date),
        join_month=MONTH(join_date),
    ).transactions_made.WHERE(
        (YEAR(date_time) == join_year) & (MONTH(date_time) == join_month)
    )
    return (
        customers.WHERE(
            (join_date >= DATETIME("now", "-6 months", "start of month"))
            & (join_date < DATETIME("now", "start of month"))
        )
        .CALCULATE(
            month=JOIN_STRINGS("-", YEAR(join_date), LPAD(MONTH(join_date), 2, "0"))
        )
        .PARTITION(name="months", by=month)
        .CALCULATE(
            month,
            customer_signups=COUNT(customers),
            avg_tx_amount=AVG(selected_txns.amount),
        )
    )


def impl_defog_broker_adv8():
    """
    PyDough implementation of the following question for the Broker graph:

    How many transactions were made by customers from the USA last week
    (exclusive of the current week)? Return the number of transactions and
    total transaction amount.
    """
    is_american = HAS(customer.WHERE(LOWER(country) == "usa"))
    selected_txns = transactions.WHERE(
        is_american
        & (date_time < DATETIME("now", "start of week"))
        & (date_time >= DATETIME("now", "start of week", "-1 week"))
    )
    return Broker.CALCULATE(
        n_transactions=KEEP_IF(COUNT(selected_txns), COUNT(selected_txns) > 0),
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
    return (
        transactions.WHERE(
            (date_time < DATETIME("now", "start of week"))
            & (date_time >= DATETIME("now", "start of week", "-8 weeks"))
            & (ticker.ticker_type == "stock")
        )
        .CALCULATE(
            week=DATETIME(date_time, "start of week"),
            is_weekend=ISIN(DAYOFWEEK(date_time), (5, 6)),
        )
        .PARTITION(name="weeks", by=week)
        .CALCULATE(
            week,
            num_transactions=COUNT(transactions),
            weekend_transactions=SUM(transactions.is_weekend),
        )
    )


def impl_defog_broker_adv10():
    """
    PyDough implementation of the following question for the Broker graph:

    Which customer made the highest number of transactions in the same month as
    they signed up? Return the customer's id, name and number of transactions.
    """
    selected_txns = transactions_made.WHERE(
        (YEAR(date_time) == join_year) & (MONTH(date_time) == join_month)
    )
    return (
        customers.CALCULATE(join_year=YEAR(join_date), join_month=MONTH(join_date))
        .CALCULATE(_id, name, num_transactions=COUNT(selected_txns))
        .TOP_K(1, by=num_transactions.DESC())
    )


def impl_defog_broker_adv11():
    """
    PyDough implementation of the following question for the Broker graph:

    How many distinct customers with a .com email address bought stocks of
    FAANG companies (Amazon, Apple, Google, Meta or Netflix)?
    """
    faang = ("AMZN", "AAPL", "GOOGL", "META", "NFLX")
    selected_customers = customers.WHERE(
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
    selected_customers = customers.WHERE(
        (STARTSWITH(LOWER(name), "j") | ENDSWITH(LOWER(name), "ez"))
        & ENDSWITH(LOWER(state), "a")
    )
    return Broker.CALCULATE(n_customers=COUNT(selected_customers))


def impl_defog_broker_adv13():
    """
    PyDough implementation of the following question for the Broker graph:

    How many TAC are there from each country, for customers who joined on or
    after January 1, 2023? Return the country and the count. TAC = Total Active
    customers who joined on or after January 1, 2023.
    """
    selected_customers = customers.WHERE(join_date >= datetime.date(2023, 1, 1))
    countries = selected_customers.PARTITION(name="countries", by=country)
    return countries.CALCULATE(cust_country=country, TAC=COUNT(customers))


def impl_defog_broker_adv14():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the ACP for each ticker type in the past 7 days, inclusive of
    today? Return the ticker type and the average closing price.
    ACP = Average Closing Price of tickers in the last 7 days, inclusive of
    today.
    """
    return (
        tickers.CALCULATE(ticker_type)
        .daily_prices.WHERE(DATEDIFF("days", date, "now") <= 7)
        .PARTITION(name="ticker_types", by=ticker_type)
        .CALCULATE(ticker_type, ACP=AVG(daily_prices.close))
    )


def impl_defog_broker_adv15():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the AR for each country for customers who joined in 2022? Return
    the country and AR. AR (Activity Ratio) = (Number of Active customers with
    transactions / Total Number of customers with transactions) * 100.
    """
    n_active = SUM(customers.status == "active")
    n_custs = COUNT(customers)
    return (
        customers.WHERE((join_date >= "2022-01-01") & (join_date <= "2022-12-31"))
        .PARTITION(name="countries", by=country)
        .CALCULATE(
            country,
            ar=100 * DEFAULT_TO(n_active / n_custs, 0.0),
        )
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
    return (
        tickers.CALCULATE(symbol, SPM=spm)
        .WHERE(HAS(selected_txns))
        .ORDER_BY(symbol.ASC())
    )


def impl_defog_broker_basic1():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 5 countries by total transaction amount in the past 30
    days, inclusive of 30 days ago? Return the country name, number of
    transactions and total transaction amount.
    """
    selected_txns = customers.transactions_made.WHERE(
        date_time >= DATETIME("now", "-30 days", "start of day")
    )
    return customers.PARTITION(name="countries", by=country).CALCULATE(
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
    return (
        transactions.WHERE(
            (date_time >= datetime.date(2023, 1, 1))
            & (date_time <= datetime.date(2023, 3, 31))
        )
        .PARTITION(name="transaction_types", by=transaction_type)
        .CALCULATE(
            transaction_type,
            num_customers=NDISTINCT(transactions.customer_id),
            avg_shares=AVG(transactions.shares),
        )
        .TOP_K(3, by=(num_customers.DESC(), transaction_type.ASC()))
    )


def impl_defog_broker_basic3():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 10 ticker symbols by total transaction amount? Return the
    ticker symbol, number of transactions and total transaction amount.
    """
    return tickers.CALCULATE(
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
    return (
        transactions.CALCULATE(ticker_type=ticker.ticker_type, state=customer.state)
        .PARTITION(name="combinations", by=(state, ticker_type))
        .CALCULATE(
            state,
            ticker_type,
            num_transactions=COUNT(transactions),
        )
        .TOP_K(5, by=num_transactions.DESC())
    )


def impl_defog_broker_basic5():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the distinct list of customer IDs who have made a 'buy' transaction.
    """
    return customers.WHERE(
        HAS(transactions_made.WHERE(transaction_type == "buy"))
    ).CALCULATE(_id)


def impl_defog_broker_basic6():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the distinct list of ticker IDs that have daily price records on or
    after Apr 1, 2023.
    """
    return tickers.WHERE(
        HAS(daily_prices.WHERE(date >= datetime.date(2023, 4, 1)))
    ).CALCULATE(_id)


def impl_defog_broker_basic7():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 3 transaction statuses by number of transactions? Return
    the status and number of transactions.
    """
    return (
        transactions.PARTITION(name="statuses", by=status)
        .CALCULATE(status, num_transactions=COUNT(transactions))
        .TOP_K(3, by=num_transactions.DESC())
    )


def impl_defog_broker_basic8():
    """
    PyDough implementation of the following question for the Broker graph:

    What are the top 5 countries by number of customers? Return the country
    name and number of customers.
    """
    return (
        customers.PARTITION(name="countries", by=country)
        .CALCULATE(country, num_customers=COUNT(customers))
        .TOP_K(5, by=num_customers.DESC())
    )


def impl_defog_broker_basic9():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer ID and name of customers who have not made any
    transactions.
    """
    return customers.WHERE(HASNOT(transactions_made)).CALCULATE(_id, name)


def impl_defog_broker_basic10():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the ticker ID and symbol of tickers that do not have any daily
    price records.
    """
    return tickers.WHERE(HASNOT(daily_prices)).CALCULATE(_id, symbol)


def impl_defog_broker_gen1():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the lowest daily closest price for symbol `VTI` in the past 7
    days.
    """
    selected_prices = daily_prices.WHERE(
        (ticker.symbol == "VTI") & (DATEDIFF("days", date, "now") <= 7)
    )

    return Broker.CALCULATE(lowest_price=MIN(selected_prices.close))


def impl_defog_broker_gen2():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the number of transactions by users who joined in the past 70
    days.
    """
    selected_tx = transactions.WHERE(
        customer.join_date >= DATETIME("now", "-70 days", "start of day")
    )

    return Broker.CALCULATE(transaction_count=COUNT(selected_tx.customer_id))


def impl_defog_broker_gen3():
    """
    PyDough implementation of the following question for the Broker graph:

    Return the customer id and the difference between their time from
    joining to their first transaction. Ignore customers who haven't made
    any transactions.
    """
    return customers.WHERE(HAS(transactions_made)).CALCULATE(
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
    return customers.CALCULATE(_id, name, num_tx=COUNT(selected_transactions)).TOP_K(
        1, by=num_tx.DESC()
    )


def impl_defog_broker_gen5():
    """
    PyDough implementation of the following question for the Broker graph:

    What is the monthly average transaction price for successful
    transactions in the 1st quarter of 2023?
    """
    return (
        transactions.WHERE(
            (YEAR(date_time) == 2023)
            & (QUARTER(date_time) == 1)
            & (status == "success")
        )
        .CALCULATE(month=DATETIME(date_time, "start of month"))
        .PARTITION(name="months", by=month)
        .CALCULATE(month=month, avg_price=AVG(transactions.price))
        .ORDER_BY(month.ASC())
    )


def impl_defog_dealership_adv1():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    For sales with sale price over $30,000, how many payments were received in
    total and on weekends in each of the last 8 calendar weeks (excluding the
    current week)? Return the week (as a date), total payments received, and
    weekend payments received in ascending order.
    """
    return (
        payments_received.WHERE(
            MONOTONIC(1, DATEDIFF("weeks", payment_date, "now"), 8)
            & (sale_record.sale_price > 30000)
        )
        .CALCULATE(
            payment_week=DATETIME(payment_date, "start of week"),
            is_weekend=ISIN(DAYOFWEEK(payment_date), (5, 6)),
        )
        .PARTITION(name="weeks", by=payment_week)
        .CALCULATE(
            payment_week,
            total_payments=COUNT(payments_received),
            weekend_payments=SUM(payments_received.is_weekend),
        )
    )


def impl_defog_dealership_adv2():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    How many sales did each salesperson make in the past 30 days, inclusive of
    today's date? Return their ID, first name, last name and number of sales
    made, ordered from most to least sales.
    """
    selected_sales = sales_made.WHERE(DATEDIFF("days", sale_date, "now") <= 30)

    return (
        salespeople.WHERE(HAS(selected_sales))
        .CALCULATE(_id, first_name, last_name, num_sales=COUNT(selected_sales))
        .ORDER_BY(num_sales.DESC(), _id.ASC())
    )


def impl_defog_dealership_adv3():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    How many sales were made for each car model that has 'M5' in its VIN
    number? Return the make, model and number of sales. When using car makes,
    model names, engine_type and vin_number, match case-insensitively and allow
    partial matches using LIKE with wildcards.
    """
    return cars.WHERE(CONTAINS(LOWER(vin_number), "m5")).CALCULATE(
        make, model, num_sales=COUNT(sale_records)
    )


def impl_defog_dealership_adv4():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    How many Toyota cars were sold in the last 30 days inclusive of today?
    Return the number of sales and total revenue.
    """
    date_threshold = DATETIME("now", "-30 days")

    selected_sales = sale_records.WHERE(sale_date >= date_threshold)

    return cars.WHERE(CONTAINS(LOWER(make), "toyota")).CALCULATE(
        num_sales=COUNT(selected_sales),
        total_revenue=KEEP_IF(
            SUM(selected_sales.sale_price), COUNT(selected_sales) > 0
        ),
    )


def impl_defog_dealership_adv5():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the first name, last name, total sales amount, number of sales, and
    SR for each salesperson. SR = sales rank of each salesperson ordered by
    their total sales amount descending
    """
    total_sales = SUM(sales_made.sale_price)

    return (
        salespeople.WHERE(HAS(sales_made))
        .CALCULATE(
            first_name,
            last_name,
            total_sales=total_sales,
            num_sales=COUNT(sales_made),
            sales_rank=RANKING(by=total_sales.DESC(), allow_ties=True),
        )
        .ORDER_BY(total_sales.DESC())
    )


def impl_defog_dealership_adv6():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the highest sale price for each make and model of cars that have
    been sold and are no longer in inventory, ordered by the sale price from
    highest to lowest. Use the most recent date in the inventory_snapshots
    table to determine that car's inventory status. When getting a car's
    inventory status, always take the latest status from the
    inventory_snapshots table
    """
    latest_snapshot = inventory_snapshots.BEST(by=snapshot_date.DESC(), per="cars")

    return (
        cars.WHERE(HAS(latest_snapshot) & (~latest_snapshot.is_in_inventory))
        .CALCULATE(make, model, highest_sale_price=MAX(sale_records.sale_price))
        .ORDER_BY(highest_sale_price.DESC())
    )


def impl_defog_dealership_adv7():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What are the details and average sale price for cars that have 'Ford' in
    their make name or 'Mustang' in the model name? Return make, model, year,
    color, vin_number and avg_sale_price. When using car makes, model names,
    engine_type and vin_number, match case-insensitively and allow partial
    matches using LIKE with wildcards.
    """
    return cars.WHERE(
        CONTAINS(LOWER(make), "fords") | CONTAINS(LOWER(model), "mustang")
    ).CALCULATE(
        make,
        model,
        year,
        color,
        vin_number,
        avg_sale_price=AVG(sale_records.sale_price),
    )


def impl_defog_dealership_adv8():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What are the PMSPS and PMSR in the last 6 months excluding the current
    month, for salespersons hired between 2022 and 2023 (both inclusive)?
    Return all months in your answer, including those where metrics are 0.
    Order by month ascending. PMSPS = per month salesperson sales count. PMSR =
    per month sales revenue in dollars. Truncate date to month for aggregation.
    """
    eligible_salespersons = salespeople.WHERE(
        (YEAR(hire_date) >= 2022) & (YEAR(hire_date) <= 2023)
    )
    return (
        sales.WHERE(
            MONOTONIC(
                1, DATEDIFF("months", sale_date, DATETIME("now", "start of month")), 6
            )
            & HAS(
                salespeople.WHERE((YEAR(hire_date) >= 2022) & (YEAR(hire_date) <= 2023))
            )
        )
        .CALCULATE(sale_price, sale_month=DATETIME(sale_date, "start of month"))
        .PARTITION(name="months", by=sale_month)
        .CALCULATE(sale_month, PMSPS=COUNT(sales), PMSR=SUM(sales.sale_price))
        .ORDER_BY(sale_month.ASC())
    )


def impl_defog_dealership_adv9():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    What is the ASP for sales made in the first quarter of 2023? ASP = Average
    Sale Price in the first quarter of 2023.
    """
    selected_sales = sales.WHERE(
        (sale_date >= "2023-01-01") & (sale_date <= "2023-03-31")
    )
    return Dealership.CALCULATE(ASP=AVG(selected_sales.sale_price))


def impl_defog_dealership_adv10():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What is the average number of days between the sale date and payment
    received date, rounded to 2 decimal places?
    """
    payment_info = sales.CALCULATE(
        sale_pay_diff=DATEDIFF(
            "days",
            sale_date,
            MAX(payment.payment_date),
        )
    )
    return Dealership.CALCULATE(
        avg_days_to_payment=ROUND(AVG(payment_info.sale_pay_diff), 2)
    )


def impl_defog_dealership_adv11():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What is the GPM for all car sales in 2023? GPM (gross profit margin) =
    (total revenue - total cost) / total cost * 100
    """
    sales_2023 = (
        sales.WHERE(YEAR(sale_date) == 2023)
        .WHERE(HAS(car))
        .CALCULATE(car_cost=car.cost)
    )

    return Dealership.CALCULATE(
        GPM=(
            (SUM(sales_2023.sale_price) - SUM(sales_2023.car_cost))
            / SUM(sales_2023.car_cost)
        )
        * 100
    )


def impl_defog_dealership_adv12():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What is the make, model and sale price of the car with the highest sale
    price that was sold on the same day it went out of inventory?
    """
    same_date_snapshot = car.inventory_snapshots.WHERE(
        (snapshot_date == sale_date) & (~is_in_inventory)
    )

    return (
        sales.CALCULATE(sale_date=sale_date)
        .WHERE(HAS(same_date_snapshot))
        .CALCULATE(
            make=car.make,
            model=car.model,
            sale_price=sale_price,
        )
        .TOP_K(1, by=sale_price.DESC())
    )


def impl_defog_dealership_adv13():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What is the total payments received per month? Also calculate the MoM
    change for each month. MoM change = (current month value - prev month
    value). Return all months in your answer, including those where there were
    no payments.
    """
    # TODO (gh #162): add user created collections support to PyDough

    filtered_payments = payments_received.CALCULATE(
        payment_amount,
        month=DATETIME(payment_date, "start of month"),
    )

    monthly_totals = filtered_payments.PARTITIOn(name="months", by=month).CALCULATE(
        total_payments=SUM(payments_received.payment_amount)
    )

    return monthly_totals.CALCULATE(
        month,
        total_payments,
        MoM_change=total_payments - PREV(total_payments, by=month.ASC()),
    ).ORDER_BY(month.ASC())


def impl_defog_dealership_adv14():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    What is the TSC in the past 7 days, inclusive of today? TSC = Total sales
    Count.
    """
    selected_sales = sales.WHERE(DATEDIFF("DAYS", sale_date, "now") <= 7)
    return Dealership.CALCULATE(TSC=COUNT(selected_sales))


def impl_defog_dealership_adv15():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Who are the top 3 salespersons by ASP? Return their first name, last name
    and ASP. ASP (average selling price) = total sales amount / number of sales
    """
    return salespeople.CALCULATE(
        first_name, last_name, ASP=AVG(sales_made.sale_price)
    ).TOP_K(3, by=ASP.DESC())


def impl_defog_dealership_adv16():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Who are the top 5 salespersons by total sales amount? Return their ID,
    first name, last name and total sales amount.
    """
    return salespeople.CALCULATE(
        _id, first_name, last_name, total=SUM(sales_made.sale_price)
    ).TOP_K(5, by=total.DESC())


def impl_defog_dealership_basic1():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the car ID, make, model and year for cars that have no sales
    records, by doing a left join from the cars to sales table.
    """
    return cars.WHERE(HASNOT(sale_records)).CALCULATE(_id, make, model, year)


def impl_defog_dealership_basic2():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the distinct list of customer IDs that have made a purchase, based
    on joining the customers and sales tables.
    """
    return customers.WHERE(HAS(car_purchases)).CALCULATE(_id)


def impl_defog_dealership_basic3():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the distinct list of salesperson IDs that have received a cash
    payment, based on joining the salespersons, sales and payments_received
    tables.
    """
    return salespeople.WHERE(
        HAS(sales_made.payment.WHERE(payment_method == "cash"))
    ).CALCULATE(salesperson_id=_id)


def impl_defog_dealership_basic4():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the salesperson ID, first name and last name for salespersons that
    have no sales records, by doing a left join from the salespersons to sales
    table.
    """
    return salespeople.WHERE(HASNOT(sales_made)).CALCULATE(_id, first_name, last_name)


def impl_defog_dealership_basic5():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the top 5 salespersons by number of sales in the past 30 days?
    Return their first and last name, total sales count and total revenue
    amount.
    """
    latest_sales = sales_made.WHERE(DATEDIFF("days", sale_date, "now") <= 30)

    return (
        salespeople.WHERE(HAS(latest_sales))
        .CALCULATE(
            first_name,
            last_name,
            total_sales=COUNT(latest_sales),
            total_revenue=SUM(latest_sales.sale_price),
        )
        .TOP_K(5, by=total_sales.DESC())
    )


def impl_defog_dealership_basic6():
    """
    PyDough implementation of the following question for the Car Dealership
    graph:

    Return the top 5 states by total revenue, showing the number of unique
    customers and total revenue (based on sale price) for each state.
    """
    return (
        sales.CALCULATE(customer.state)
        .PARTITION(name="states", by=state)
        .CALCULATE(
            state,
            unique_customers=NDISTINCT(sales.customer_id),
            total_revenue=SUM(sales.sale_price),
        )
        .TOP_K(5, by=total_revenue.DESC())
    )


def impl_defog_dealership_basic7():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    What are the top 3 payment methods by total payment amount received? Return
    the payment method, total number of payments and total amount.
    """
    return (
        payments_received.PARTITION(name="payment_methods", by=payment_method)
        .CALCULATE(
            payment_method,
            total_payments=COUNT(payments_received),
            total_amount=SUM(payments_received.payment_amount),
        )
        .TOP_K(3, by=total_amount.DESC())
    )


def impl_defog_dealership_basic8():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    What are the top 5 best selling car models by total revenue? Return the
    make, model, total number of sales and total revenue.
    """
    return cars.CALCULATE(
        make,
        model,
        total_sales=COUNT(sale_records),
        total_revenue=SUM(sale_records.sale_price),
    ).TOP_K(5, by=total_revenue.DESC())


def impl_defog_dealership_basic9():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    What are the total number of customer signups for the top 2 states? Return
    the state and total signups, starting from the top.
    """
    return (
        customers.PARTITION(name="grouped", by=state)
        .CALCULATE(state, total_signups=COUNT(customers))
        .TOP_K(2, by=total_signups.DESC())
    )


def impl_defog_dealership_basic10():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    Who were the top 3 sales representatives by total revenue in the past 3
    months, inclusive of today's date? Return their first name, last name,
    total number of sales and total revenue. Note that revenue refers to the
    sum of sale_price in the sales table.
    """
    date_threshold = DATETIME("now", "-3 months")

    return salespeople.CALCULATE(
        first_name,
        last_name,
        total_sales=COUNT(sales_made.WHERE(sale_date >= date_threshold)),
        total_revenue=SUM(sales_made.WHERE(sale_date >= date_threshold).sale_price),
    ).TOP_K(3, by=total_revenue.DESC())


def impl_defog_dealership_gen1():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    Return the name and phone number of the salesperson with the shortest time
    from being hired to getting fired. Return the number of days he/she was
    employed for.

    NOTE: Code adjusted by multiplying by 1.0 to match result type
    """
    return (
        salespeople.WHERE(PRESENT(termination_date))
        .CALCULATE(
            first_name,
            last_name,
            phone,
            days_employed=DATEDIFF("days", hire_date, termination_date) * 1.0,
        )
        .TOP_K(1, days_employed.ASC())
    )


def impl_defog_dealership_gen2():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    Return the number of payments made on weekends to the vendor named 'Utility
    Company'
    """
    return Dealership.CALCULATE(
        weekend_payments=COUNT(
            payments_made.WHERE(
                (vendor_name == "Utility Company")
                & (ISIN(DAYOFWEEK(payment_date), (5, 6)))
            )
        )
    )


def impl_defog_dealership_gen3():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    Show me the daily total amount of payments received in the whole of the
    previous ISO week not including the current week, split by the
    payment_method.
    """
    return (
        payments_received.WHERE((DATEDIFF("week", payment_date, "now") == 1))
        .PARTITION(name="groups", by=(payment_date, payment_method))
        .CALCULATE(
            payment_date,
            payment_method,
            total_amount=SUM(payments_received.payment_amount),
        )
        .ORDER_BY(payment_date.DESC(), payment_method.ASC())
    )


def impl_defog_dealership_gen4():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    What were the total quarterly sales in 2023 grouped by customer's state?
    Represent each quarter as the first date in the quarter.
    """

    return (
        sales.WHERE(YEAR(sale_date) == 2023)
        .CALCULATE(
            quarter=DATETIME(sale_date, "start of quarter"),
            customer_state=customer.state,
        )
        .PARTITION(name="groups", by=(quarter, customer_state))
        .CALCULATE(quarter, customer_state, total_sales=SUM(sales.sale_price))
        .WHERE(total_sales > 0)
        .ORDER_BY(quarter.ASC(), customer_state.ASC())
    )


def impl_defog_dealership_gen5():
    """
    PyDough implementation of the following question for the Car Dealership graph:

    Which cars were in inventory in the latest snapshot for march 2023? Return
    the car id, make, model, and year. cars are considered to be in inventory"
    if is_in_inventory is True."
    """
    return (
        inventory_snapshots.WHERE(
            (YEAR(snapshot_date) == 2023) & (MONTH(snapshot_date) == 3)
        )
        .BEST(by=snapshot_date.DESC(), allow_ties=True)
        .WHERE(is_in_inventory)
        .car.CALCULATE(_id, make, model, year)
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
    return merchants.WHERE(HAS(successful_transactions)).CALCULATE(
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
    return (
        notifications.WHERE(
            (created_at < DATETIME("now", "start of week"))
            & (created_at >= DATETIME("now", "start of week", "-3 weeks"))
            & ISIN(user.country, ("US", "CA"))
        )
        .CALCULATE(
            week=DATETIME(created_at, "start of week"),
            is_weekend=ISIN(DAYOFWEEK(created_at), (5, 6)),
        )
        .PARTITION(name="weeks", by=week)
        .CALCULATE(
            week,
            num_notifs=COUNT(notifications),
            weekend_notifs=SUM(notifications.is_weekend),
        )
    )


def impl_defog_ewallet_adv3():
    """
    PyDough implementation of the following question for the eWallet graph:

    How many active retail merchants have issued coupons? Return the merchant
    name and the total number of coupons issued. Merchant category should be
    matched case-insensitively.
    """
    # Retrieve merchant summary for active merchants in the "retail" category who have coupons
    return merchants.WHERE(
        (status == "active") & (CONTAINS(LOWER(category), "retail")) & HAS(coupons)
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
    us_transactions = transactions.WHERE(
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
    selected_user_balances = user_balances.WHERE(
        DATEDIFF("days", updated_at, "now") <= 7
    )

    return Ewallet.CALCULATE(AMB=AVG(selected_user_balances.balance))


def impl_defog_ewallet_adv6():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the LUB for each user. LUB = Latest User Balance, which is the most
    recent balance for each user
    """
    latest_balance_record = balances.BEST(by=updated_at.DESC(), per="users")

    return users.WHERE(HAS(latest_balance_record)).CALCULATE(
        user_id=uid, latest_balance=latest_balance_record.balance
    )


def impl_defog_ewallet_adv7():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the marketing opt-in preference for each user? Return the user ID
    and boolean opt-in value. To get any user's settings, only select the
    latest snapshot of user_setting_snapshot for each user.
    """
    latest_snapshot = setting_snapshots.BEST(by=created_at.DESC(), per="users")

    return users.WHERE(HAS(latest_snapshot)).CALCULATE(
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
    transaction_sum = SUM(successful_transactions.amount)

    return merchants.WHERE(HAS(successful_transactions)).CALCULATE(
        merchants_id=mid,
        merchants_name=name,
        category=category,
        total_revenue=transaction_sum,
        mrr=RANKING(by=transaction_sum.DESC()),
    )


def impl_defog_ewallet_adv9():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the PMDAU (Per Month Daily Active users) for wallet transactions in
    the last 2 months excluding the current month? PMDAU (Per Month Daily
    Active users) = COUNT(DISTINCT(sender_id) ... WHERE t.sender_type = 0.
    Truncate created_at to month for aggregation.
    """
    start_date = DATETIME("now", "start of month", "-2 months")
    end_date = DATETIME("now", "start of month")
    return (
        transactions.WHERE(
            (sender_type == 0) & (created_at >= start_date) & (created_at < end_date)
        )
        .CALCULATE(year_month=DATETIME(created_at, "start of month"))
        .PARTITION(name="months", by=year_month)
        .CALCULATE(
            year_month=year_month, active_users=NDISTINCT(transactions.sender_id)
        )
    )


def impl_defog_ewallet_adv10():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the total number of wallet transactions sent by each user that is
    not a merchant? Return the user ID and total transaction count.
    """
    successful_transactions = transactions_sent.WHERE(sender_type == 0)
    return users.WHERE(HAS(successful_transactions)).CALCULATE(
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
        (session_start >= "2023-06-01") & (session_end < "2023-06-08")
    ).CALCULATE(duration=DATEDIFF("seconds", session_start, session_end))
    return (
        users.WHERE(HAS(selected_sessions))
        .CALCULATE(uid=uid, total_duration=SUM(selected_sessions.duration))
        .ORDER_BY(total_duration.DESC())
    )


def impl_defog_ewallet_adv12():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the total transaction amount for each coupon offered by merchant
    with ID 1? Return the coupon ID and total amount transacted with it.
    """
    return coupons.WHERE(merchant_id == "1").CALCULATE(
        coupon_id=cid, total_discount=SUM(transaction_used_in.amount)
    )


def impl_defog_ewallet_adv13():
    """
    PyDough implementation of the following question for the eWallet graph:

    What is the TUC in the past month, inclusive of 1 month ago? Return the
    total count. TUC = Total number of user sessions in the past month
    """
    selected_sessions = user_sessions.WHERE(
        (session_start >= DATETIME("now", "-1 month", "start of day"))
        | (session_end >= DATETIME("now", "-1 month", "start of day"))
    )

    return Ewallet.CALCULATE(TUC=COUNT(selected_sessions))


def impl_defog_ewallet_adv14():
    """
    PyDough implementation of the following question for the eWallet graph:

    What was the STR for wallet transactions in the previous month? STR
    (success transaction rate) = number of successful transactions / total
    number of transactions.
    """
    past_month_transactions = transactions.WHERE(
        DATEDIFF("months", created_at, "now") == 1
    )

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
    return merchants.CALCULATE(
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

    return users.WHERE((LOWER(country) == "us") & HAS(unread_notifs)).CALCULATE(
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
    return (
        transactions.WHERE(
            (status == "success")
            & (YEAR(created_at) == 2023)
            & (sending_user.status == "active")
            & (sender_type == 0)
        )
        .CALCULATE(month=DATETIME(created_at, "start of month"))
        .PARTITION(name="months", by=month)
        .CALCULATE(month, active_users=NDISTINCT(transactions.sender_id))
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

    return merchants.CALCULATE(
        merchant_name=name,
        total_transactions=COUNT(selected_transactions),
        total_amount=SUM(selected_transactions.amount),
    ).TOP_K(2, by=total_amount.DESC())


def impl_defog_ewallet_basic2():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return merchants (merchant ID and name) who have not issued any coupons.
    """
    return merchants.WHERE(HASNOT(coupons)).CALCULATE(
        merchant_id=mid, merchant_name=name
    )


def impl_defog_ewallet_basic3():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return the distinct list of merchant IDs that have received money from a
    transaction. Consider all transaction types in the results you return,
    but only include the merchant ids in your final answer.
    """
    return merchants.WHERE(
        HAS(transactions_received.WHERE(receiver_type == 1))
    ).CALCULATE(merchant=mid)


def impl_defog_ewallet_basic4():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return the distinct list of user IDs who have received transaction
    notifications.
    """
    return users.WHERE(
        HAS(notifications.WHERE(notification_type == "transaction"))
    ).CALCULATE(user_id=uid)


def impl_defog_ewallet_basic5():
    """
    PyDough implementation of the following question for the eWallet graph:

    Return users (user ID and username) who have not received any
    notifications.
    """
    return users.WHERE(HASNOT(notifications)).CALCULATE(uid, username)


def impl_defog_ewallet_basic6():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 2 most frequently used device types for user sessions
    and their respective counts?
    """
    return (
        user_sessions.PARTITION(name="device_types", by=device_type)
        .CALCULATE(device_type=device_type, count=COUNT(user_sessions))
        .TOP_K(2, count.DESC())
    )


def impl_defog_ewallet_basic7():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 3 most common transaction statuses and their respective
    counts?
    """
    return (
        transactions.PARTITION(name="statuses", by=status)
        .CALCULATE(status=status, count=COUNT(transactions))
        .TOP_K(3, count.DESC())
    )


def impl_defog_ewallet_basic8():
    """
    PyDough implementation of the following question for the eWallet graph:

    What are the top 3 most frequently used coupon codes? Return the coupon
    code, total number of redemptions, and total amount redeemed.
    """
    return coupons.CALCULATE(
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
    return (
        transactions.WHERE(sender_type == 0)
        .CALCULATE(country=sending_user.country)
        .PARTITION(name="countries", by=country)
        .CALCULATE(
            country=country,
            user_count=NDISTINCT(transactions.sender_id),
            total_amount=SUM(transactions.amount),
        )
        .TOP_K(5, total_amount.DESC())
    )


def impl_defog_ewallet_gen1():
    """
    PyDough implementation of the following question for the eWallet graph:

    Give me today's median merchant wallet balance for all active merchants
    whose category contains 'retail'
    """

    latest_balance_today = merchant_balances.WHERE(
        (DATETIME(updated_at, "start of day") == DATETIME("now", "start of day"))
        & (merchant.status == "active")
        & CONTAINS(LOWER(merchant.category), "retail")
    )

    return Ewallet.CALCULATE(MEDIAN(latest_balance_today.balance))


def impl_defog_ewallet_gen2():
    """
    PyDough implementation of the following question for the eWallet graph:

    What was the average transaction daily and monthly limit for the earliest
    setting snapshot in 2023?
    """
    snapshots_2023 = user_setting_snapshots.WHERE(YEAR(snapshot_date) == 2023)
    selected_snapshots = snapshots_2023.WHERE(min_date == snapshot_date)

    return Ewallet.CALCULATE(min_date=MIN(snapshots_2023.snapshot_date)).CALCULATE(
        avg_daily_limit=AVG(selected_snapshots.daily_transaction_limit),
        avg_monthly_limit=AVG(selected_snapshots.monthly_transaction_limit),
    )


def impl_defog_ewallet_gen3():
    """
    PyDough implementation of the following question for the eWallet graph:

    what was the average user session duration in seconds split by device_type?
    """
    return user_sessions.PARTITION(name="device_types", by=device_type).CALCULATE(
        device_type=device_type,
        avg_session_duration_seconds=AVG(
            DATEDIFF("seconds", user_sessions.session_start, user_sessions.session_end)
        ),
    )


def impl_defog_ewallet_gen4():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which merchants earliest coupon start date was within a year of the
    merchant's registration? Return the merchant id, registration date, and
    earliest coupon id and start date
    """
    return (
        merchants.CALCULATE(
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
        .CALCULATE(
            merchants_id,
            merchant_registration_date,
            earliest_coupon_start_date,
            earliest_coupon_id,
        )
    )


def impl_defog_ewallet_gen5():
    """
    PyDough implementation of the following question for the eWallet graph:

    Which users did not get a notification within the first year of signing up?
    Return their usernames, emails and signup dates.
    """
    selected_notifications = notifications.WHERE(
        MONOTONIC(user.created_at, created_at, DATETIME(user.created_at, "+1 year"))
    )
    return users.WHERE(HASNOT(selected_notifications)).CALCULATE(
        username=username, email=email, created_at=created_at
    )


def impl_defog_dermtreatment_basic1():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the top 3 doctor specialties by total drug amount prescribed for
    treatments started in the past 6 calendar months? Return the specialty,
    number of treatments, and total drug amount.
    """
    # Obtain the specialty groups
    specialties = doctors.PARTITION(name="specialties", by=specialty)

    # Find the treatments from the doctors within the specialty in the past 6 months
    recent_treatments = doctors.prescribed_treatments.WHERE(
        DATEDIFF("months", start_date, DATETIME("now")) <= 6
    )

    # Calculate totals for each specialty
    return (
        specialties.WHERE(HAS(recent_treatments))
        .CALCULATE(
            specialty,
            num_treatments=COUNT(recent_treatments),
            total_drug_amount=SUM(recent_treatments.total_drug_amount),
        )
        .TOP_K(3, by=total_drug_amount.DESC())
    )


def impl_defog_dermtreatment_basic2():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    For treatments that ended in the year 2022 (from Jan 1st to Dec 31st inclusive),
    what is the average PASI score at day 100 and number of distinct patients
    per insurance type? Return the top 5 insurance types sorted by lowest average
    PASI score first.
    """

    # First, filter treatments to those that ended in 2022 and have a recorded day 100 PASI score.
    # Then, extract the insurance type from the associated patient to use as a partition key.
    treatments_info = treatments.WHERE(
        (YEAR(end_date) == 2022)
        & (HAS(outcome_records.WHERE(PRESENT(day100_pasi_score))))
    ).CALCULATE(insurance_type=patient.insurance_type)

    # Partition the filtered treatments by insurance type. For each type, calculate the
    # average day 100 PASI score and the count of distinct patients.
    return (
        treatments_info.PARTITION(name="insurance_groups", by=insurance_type)
        .CALCULATE(
            insurance_type=insurance_type,
            num_distinct_patients=NDISTINCT(treatments.patient_id),
            avg_pasi_score_day100=AVG(treatments.outcome_records.day100_pasi_score),
        )
        .TOP_K(5, by=avg_pasi_score_day100.ASC())
    )


def impl_defog_dermtreatment_basic3():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the top 5 drugs by number of treatments and average drug amount per
    treatment? Return the drug name, number of treatments, and average drug amount.
    """

    return drugs.CALCULATE(
        drug_name,
        num_treatments=COUNT(treatments_used_in),
        avg_drug_amount=AVG(treatments_used_in.total_drug_amount),
    ).TOP_K(
        5,
        by=(
            num_treatments.DESC(),
            avg_drug_amount.DESC(),
            drug_name.ASC(),
        ),
    )


def impl_defog_dermtreatment_basic4():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the top 3 diagnoses by maximum itch VAS score at day 100 and number
    of distinct patients? Return the diagnosis name, number of patients, and
    maximum itch score. Only include patients with a registered outcome
    """

    selected_treatments = treatments_for.WHERE(HAS(outcome_records))
    return (
        diagnoses.WHERE(HAS(selected_treatments)).CALCULATE(
            diagnosis_name=name,
            num_patients=NDISTINCT(selected_treatments.patient_id),
            max_itch_score=MAX(selected_treatments.outcome_records.day100_itch_vas),
        )
    ).TOP_K(3, by=(max_itch_score.DESC(), num_patients.DESC()))


def impl_defog_dermtreatment_basic5():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the distinct list of doctor IDs, first names and last names that have
    prescribed treatments.
    """

    return doctors.WHERE(HAS(prescribed_treatments)).CALCULATE(
        doc_id, first_name, last_name
    )


def impl_defog_dermtreatment_basic6():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the distinct list of patient IDs, first names and last names that have
    outcome assessments.
    """
    return patients.WHERE(HAS(treatments_received.outcome_records)).CALCULATE(
        patient_id, first_name, last_name
    )


def impl_defog_dermtreatment_basic7():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the top 3 insurance types by average patient height in cm? Return
    the insurance type, average height and average weight.
    """

    return (
        patients.PARTITION(name="insurance_groups", by=insurance_type)
        .CALCULATE(
            insurance_type,
            avg_height=AVG(patients.height),
            avg_weight=AVG(patients.weight),
        )
        .TOP_K(3, by=avg_height.DESC())
    )


def impl_defog_dermtreatment_basic8():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the top 2 specialties by number of doctors? Return the specialty
    and number of doctors.
    """

    return (
        doctors.PARTITION(name="specialty_groups", by=specialty)
        .CALCULATE(specialty, num_doctors=COUNT(doctors))
        .TOP_K(2, by=num_doctors.DESC())
    )


def impl_defog_dermtreatment_basic9():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the patient IDs, first names and last names of patients who have not
    received any treatments.
    """

    return patients.WHERE(HASNOT(treatments_received)).CALCULATE(
        patient_id, first_name, last_name
    )


def impl_defog_dermtreatment_basic10():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the drug IDs and names of drugs that have not been used in any
    treatments.
    """

    return drugs.WHERE(HASNOT(treatments_used_in)).CALCULATE(drug_id, drug_name)


def impl_defog_dermtreatment_adv1():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Which states do doctors who have prescribed biologic drugs reside in?
    Return the distinct states.
    """

    doctors_biologic_prescribed = doctors.prescribed_treatments.WHERE(
        drug.drug_type == "biologic"
    ).CALCULATE(state=doctor.state)

    return doctors_biologic_prescribed.PARTITION(name="states", by=state).CALCULATE(
        state
    )


def impl_defog_dermtreatment_adv2():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is the average weight in kg of patients treated with the drug named
    'Drugalin'? Return the average weight.
    """
    return DermTreatment.CALCULATE(
        avg_weight=AVG(
            treatments.WHERE(LOWER(drug.drug_name) == "drugalin").patient.weight
        )
    )


def impl_defog_dermtreatment_adv3():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    I want the adverse events that have been reported for treatments involving
    topical drugs. Give me the description, treatment id, drug id and name.
    """

    return adverse_events.WHERE(treatment.drug.drug_type == "topical").CALCULATE(
        description,
        treatment_id,
        treatment.drug.drug_id,
        treatment.drug.drug_name,
    )


def impl_defog_dermtreatment_adv4():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    How many patients have been diagnosed with 'Psoriasis vulgaris' and treated
    with a biologic drug? Return the distinct count of patients.
    """

    return DermTreatment.CALCULATE(
        patient_count=COUNT(
            patients.WHERE(
                HAS(
                    treatments_received.WHERE(
                        (LOWER(diagnosis.name) == "psoriasis vulgaris")
                        & (LOWER(drug.drug_type) == "biologic")
                    )
                )
            )
        )
    )


def impl_defog_dermtreatment_adv5():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is the NPI for each year? Return the year, number of new patients,
    and NPI
    """

    # Step 1: For each patient who has received treatment, find their first treatment year
    patients_with_first_treatment = patients.WHERE(HAS(treatments_received)).CALCULATE(
        first_treatment_year=MIN(
            treatments_received.CALCULATE(start_year=YEAR(start_date)).start_year
        ),
    )

    # Step 2: Group by year to count new patients per year
    new_patients_by_year = patients_with_first_treatment.PARTITION(
        name="years", by=first_treatment_year
    ).CALCULATE(number_of_new_patients=COUNT(patients))

    # Step 3: Calculate NPI (increase compared to previous year)
    return (
        new_patients_by_year.CALCULATE(
            npi=number_of_new_patients
            - DEFAULT_TO(
                PREV(number_of_new_patients, by=first_treatment_year.ASC()),
                number_of_new_patients,
            ),
        )
        .CALCULATE(
            year=STRING(first_treatment_year),
            number_of_new_patients=number_of_new_patients,
            npi=KEEP_IF(npi, npi != 0),
        )
        .ORDER_BY(year.ASC())
    )


def impl_defog_dermtreatment_adv6():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return each doctor's doc_id, specialty, number of distinct drugs prescribed,
    and SDR
    """

    # First, calculate the number of distinct drugs prescribed by each doctor
    doctor_drug_counts = doctors.CALCULATE(
        num_distinct_drugs=NDISTINCT(prescribed_treatments.drug_id)
    ).WHERE(HAS(prescribed_treatments))

    # Then partition by specialty to enable ranking within each specialty
    specialty_groups = doctor_drug_counts.PARTITION(name="specialties", by=specialty)

    # Calculate the rank within each specialty
    return specialty_groups.doctors.CALCULATE(
        doc_id=doc_id,
        specialty=specialty,
        num_distinct_drugs=num_distinct_drugs,
        SDRSDR=RANKING(
            by=num_distinct_drugs.DESC(), per="specialties", allow_ties=True, dense=True
        ),
    )


def impl_defog_dermtreatment_adv7():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    How many treatments did the patient Alice have in the last 6 months, not
    including the current month?
    """

    start_of_current_month = DATETIME("now", "start of month")
    start_of_period = DATETIME("now", "start of month", "-6 months")

    selected_treatments = treatments.WHERE(
        (start_date >= start_of_period)
        & (start_date < start_of_current_month)
        & (LOWER(patient.first_name) == "alice")
    )

    return DermTreatment.CALCULATE(num_treatments=COUNT(selected_treatments))


def impl_defog_dermtreatment_adv8():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What are the PMPD and PMTC for each of the last 12 months, not including
    the current month
    """

    # Get treatments with month info
    treatment_info = treatments.CALCULATE(
        start_month=DATETIME(start_date, "start of month"),
    ).WHERE(
        (start_month < DATETIME("now", "start of month"))
        & (start_month >= DATETIME("now", "start of month", "-12 months"))
    )

    # Partition by month and calculate counts
    return (
        treatment_info.PARTITION(name="months", by=start_month)
        .CALCULATE(
            start_month=JOIN_STRINGS(
                "-", YEAR(start_month), LPAD(MONTH(start_month), 2, "0")
            ),
            PMPD=NDISTINCT(treatments.diagnosis_id),  # Distinct diagnoses per month
            PMTC=COUNT(treatments),  # Total treatments per month
        )
        .ORDER_BY(start_month.DESC())
    )


def impl_defog_dermtreatment_adv9():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    How many distinct patients had treatments in each of the last 3 months, not
    including the current month? Out of these, how many had treatments with
    biologic drugs? Return the month, patient count, and biologic treatment count.
    """

    # Get treatments from the last 3 months (excluding current month)
    # First, calculate the date range for the last 3 months
    recent_treatments = treatments.WHERE(
        (start_date >= DATETIME("now", "start of month", "-3 months"))
        & (start_date < DATETIME("now", "start of month"))
    ).CALCULATE(
        patient_id,
        treatment_month=JOIN_STRINGS(
            "-", YEAR(start_date), LPAD(MONTH(start_date), 2, "0")
        ),
        is_biologic=drug.drug_type == "biologic",
    )

    # Partition by month to group treatments
    monthly_groups = recent_treatments.PARTITION(name="months", by=treatment_month)

    # Calculate distinct patient counts for each month
    return monthly_groups.CALCULATE(
        month=treatment_month,
        patient_count=NDISTINCT(treatments.patient_id),
        biologic_treatment_count=NDISTINCT(treatments.WHERE(is_biologic).patient_id),
    ).ORDER_BY(month.DESC())


def impl_defog_dermtreatment_adv10():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Which drug had the highest number of adverse events reported within the same
    month as the treatment start date (adverse event or treatment can be earlier
    than the other)? Return the number of adverse events along with the drug's
    id and name.
    """

    # Identify all the adverse events for each drug that were the same month the
    # the treatment started in.
    same_month_adverse_events = treatments_used_in.CALCULATE(
        treatment_start_date=start_date
    ).adverse_events.WHERE(
        DATETIME(treatment_start_date, "start of month")
        == DATETIME(reported_date, "start of month")
    )

    # For each drug count the number of such events
    drug_ae_counts = drugs.WHERE(HAS(same_month_adverse_events)).CALCULATE(
        drug_id, drug_name, num_adverse_events=COUNT(same_month_adverse_events)
    )

    # Find the drug with the highest number of adverse events
    return drug_ae_counts.TOP_K(1, by=num_adverse_events.DESC())


def impl_defog_dermtreatment_adv11():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    How many patients have a Gmail or Yahoo email address?
    """

    return DermTreatment.CALCULATE(
        num_patients_with_gmail_or_yahoo=COUNT(
            patients.WHERE(
                ENDSWITH(email, "@gmail.com") | ENDSWITH(email, "@yahoo.com")
            )
        )
    )


def impl_defog_dermtreatment_adv12():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the first name, last name and specialty of doctors whose first name
    starts with 'J' or last name contains 'son', case-insensitive.
    """

    return doctors.WHERE(
        STARTSWITH(LOWER(first_name), "j") | CONTAINS(LOWER(last_name), "son")
    ).CALCULATE(first_name, last_name, specialty)


def impl_defog_dermtreatment_adv13():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is the PIC for female patients?
    """

    return DermTreatment.CALCULATE(
        PIC_female=COUNT(
            patients.WHERE((gender == "Female") & (insurance_type == "private"))
        )
    )


def impl_defog_dermtreatment_adv14():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is the CAW for male patients
    """

    return DermTreatment.CALCULATE(
        CAW_male=AVG(patients.WHERE(gender == "Male").weight)
    )


def impl_defog_dermtreatment_adv15():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Calculate the average DDD for each drug. Return the drug name and average
    DDD value.
    """
    # Find all treatments the drug was used in that have finished
    selected_treatments = treatments_used_in.WHERE(PRESENT(end_date))

    return drugs.WHERE(HAS(selected_treatments)).CALCULATE(
        drug_name,
        avg_ddd=AVG(
            selected_treatments.CALCULATE(
                ddd=total_drug_amount / DATEDIFF("days", start_date, end_date),
            ).ddd
        ),
    )


def impl_defog_dermtreatment_adv16():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is the overall D7D100PIR across all treatments? Return the percentage
    value.
    """

    # Filter outcomes to only include those with non-null PASI scores for both
    # day 7 and day 100
    valid_outcomes = outcomes.WHERE(
        PRESENT(day7_pasi_score) & PRESENT(day100_pasi_score)
    )

    # Calculate the overall D7D100PIR
    return DermTreatment.CALCULATE(
        d7d100pir=(
            AVG(valid_outcomes.day100_pasi_score) - AVG(valid_outcomes.day7_pasi_score)
        )
        / AVG(valid_outcomes.day7_pasi_score)
        * 100
    )


def impl_defog_dermtreatment_gen1():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Return the treatment id, treatment start date, adverse event date and
    description of all adverse events that occured within 10 days after starting
    treatment
    """
    return adverse_events.WHERE(
        DATEDIFF("days", treatment.start_date, reported_date) <= 10
    ).CALCULATE(
        treatment_id,
        treatment_start_date=treatment.start_date,
        adverse_event_date=reported_date,
        description=description,
    )


def impl_defog_dermtreatment_gen2():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    List the last name, year of registration, and first treatment (date and id)
    by doctors who were registered 2 years ago.
    """
    # Doctor's first treatment
    first_treatment = prescribed_treatments.BEST(per="doctors", by=start_date.ASC())

    # Find doctors registered 2 years ago and their first treatment
    return doctors.WHERE(year_reg == YEAR(DATETIME("now", "-2 years"))).CALCULATE(
        last_name,
        year_reg,
        first_treatment_date=first_treatment.start_date,
        first_treatment_id=first_treatment.treatment_id,
    )


def impl_defog_dermtreatment_gen3():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    What is average age (in integer years) of all registered male patients with
    private insurance currently?
    """
    return DermTreatment.CALCULATE(
        average_age=AVG(
            patients.WHERE((gender == "Male") & (insurance_type == "private"))
            .CALCULATE(age_in_years=DATEDIFF("years", date_of_birth, DATETIME("now")))
            .age_in_years
        )
    )


def impl_defog_dermtreatment_gen4():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    Show all placebo treatment id, start and end date, where there
    concomitant_meds were started within 2 weeks of starting the treatment.
    Also return the start and end dates of all concomitant drug usage.
    """
    return (
        treatments.WHERE(is_placebo == True)
        .concomitant_meds.WHERE(
            treatment.is_placebo
            & (DATEDIFF("days", treatment.start_date, start_date) <= 14)
        )
        .CALCULATE(
            treatment.treatment_id,
            treatment_start_date=treatment.start_date,
            treatment_end_date=treatment.end_date,
            concomitant_med_start_date=start_date,
            concomitant_med_end_date=end_date,
        )
    )


def impl_defog_dermtreatment_gen5():
    """
    PyDough implementation of the following question for the DermTreatment
    graph:

    How many treatments for diagnoses containing 'psoriasis' (match with
    wildcards case-insensitively) involve drugs that have been FDA-approved and
    the treatments have ended within the last 6 months from today?
    """
    return DermTreatment.CALCULATE(
        num_treatments=COUNT(
            treatments.WHERE(
                CONTAINS(LOWER(diagnosis.name), "psoriasis")
                & PRESENT(drug.fda_approval_date)
                & PRESENT(end_date)
                & (end_date >= DATETIME("now", "-6 months", "start of day"))
            )
        )
    )


def impl_defog_academic_gen1():
    """
    PyDough implementation of the following question for the Academic
    graph:

    Which authors have written publications in both the domain
    'Machine Learning' and the domain 'Data Science'?
    """
    selected_domains = author_publications.publication.publication_domains.WHERE(
        ISIN(domain.name, ("Data Science", "Machine Learning"))
    )

    return authors.WHERE(NDISTINCT(selected_domains.domain_id) == 2).CALCULATE(name)


def impl_defog_academic_gen2():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the total number of citations received by each author?
    """
    publications_selected = author_publications.publication

    return authors.WHERE(HAS(publications_selected)).CALCULATE(
        name, total_citations=SUM(publications_selected.citation_num)
    )


def impl_defog_academic_gen3():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the total number of publications published in each year?
    """
    return publications.PARTITION(name="years", by=year).CALCULATE(
        year, COUNT(publications)
    )


def impl_defog_academic_gen4():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the average number of references cited by publications in each
    domain name?
    """
    return domains.CALCULATE(
        name, average_references=AVG(domain_publications.publication.reference_num)
    )


def impl_defog_academic_gen5():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the average number of citations received by publications in each year?
    """
    return publications.PARTITION(name="years", by=year).CALCULATE(
        year, average_citations=AVG(publications.citation_num)
    )


def impl_defog_academic_gen6():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the title of the publication that has received the highest number of
    citations?
    """
    return publications.CALCULATE(title).TOP_K(1, by=citation_num.DESC())


def impl_defog_academic_gen7():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the top 5 domains with the highest number of authors associated
    with them?
    """
    return (
        domains.PARTITION(name="names", by=name)
        .CALCULATE(name, author_count=NDISTINCT(domains.domain_authors.author_id))
        .TOP_K(5, by=author_count)
        .ORDER_BY(author_count.DESC(), name.DESC())
    )


def impl_defog_academic_gen8():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the top 3 titles of the publications that have the highest number
    of references cited, ordered by the number of references cited in descending
    order?
    """
    return publications.CALCULATE(title).TOP_K(3, by=reference_num.DESC())


def impl_defog_academic_gen9():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the top 3 publications with the highest number of citations?
    """
    return publications.CALCULATE(title, citation_num).TOP_K(3, by=citation_num.DESC())


def impl_defog_academic_gen10():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the titles of all publications ordered alphabetically?
    """
    return publications.CALCULATE(title).ORDER_BY(title.ASC())


def impl_defog_academic_gen11():
    """
    PyDough implementation of the following question for the Academic
    graph:


    What is the ratio of publications to authors in the database?
    """
    n_pub = COUNT(publications)
    n_auth = COUNT(authors)
    return Academic.CALCULATE(
        publication_to_author_ratio=n_pub / KEEP_IF(n_auth, n_auth > 0)
    )


def impl_defog_academic_gen12():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the ratio of publications presented in conferences to publications
    published in journals?
    """
    n_confs = SUM(PRESENT(publications.conference_id))
    n_jours = SUM(PRESENT(publications.journal_id))
    return Academic.CALCULATE(ratio=n_pubs / KEEP_IF(n_jours, n_jours > 0))


def impl_defog_academic_gen13():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the ratio of the total number of publications to the total number of
    keywords within each domain ID? Show all domain IDs.
    """

    ratio_calc = IFF(
        HAS(domains_publications.domain.domain_keywords),
        COUNT(domains_publications.publication)
        / COUNT(domains_publications.domain.domain_keywords),
        None,
    )
    return domains_publications.PARTITION(name="domains", by=domain_id).CALCULATE(
        domain_id, ratio=ratio_calc
    )


def impl_defog_academic_gen14():
    """
    PyDough implementation of the following question for the Academic
    graph:

    How does the ratio of publications to journals change over the years? Return
    the annual numbers of publications and journals as well.
    """
    return (
        publications.PARTITION(name="years", by=year)
        .CALCULATE(
            year,
            num_publications=NDISTINCT(publications.publication_id),
            num_journals=NDISTINCT(publications.journal_id),
        )
        .CALCULATE(
            year,
            num_publications,
            num_journals,
            ratio=IFF(num_journals > 0, num_publications / num_journals, None),
        )
    )


def impl_defog_academic_gen15():
    """
    PyDough implementation of the following question for the Academic
    graph:

    How does the ratio of authors to organizations differ by continent?
    """
    return (
        organizations.PARTITION(name="continents", by=continent)
        .CALCULATE(
            continent,
            ratio=IFF(
                HAS(organizations.authors),
                NDISTINCT(organizations.authors.author_id)
                / NDISTINCT(organizations.organization_id),
                0,
            ),
        )
        .ORDER_BY(ratio.DESC())
    )


def impl_defog_academic_gen16():
    """
    PyDough implementation of the following question for the Academic
    graph:

    Which author had the most publications in the year 2021 and how many
    publications did he/she have that year?
    """
    return (
        writes.CALCULATE(author_name=author.name)
        .PARTITION(name="authors", by=author_name)
        .CALCULATE(
            author_name,
            count_publication=NDISTINCT(
                writes.WHERE(publication.year == 2021).publication_id
            ),
        )
        .TOP_K(1, by=count_publication.DESC())
    )


def impl_defog_academic_gen17():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the total number of publications presented in each conference?
    """
    return conferences.CALCULATE(name, count_publications=COUNT(proceedings)).ORDER_BY(
        count_publications.DESC(), name.DESC()
    )


def impl_defog_academic_gen18():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What is the total number of publications in each journal, ordered by the
    number of publications in descending order?
    """
    return journals.CALCULATE(
        name, jid=journal_id, num_publications=COUNT(archives)
    ).ORDER_BY(num_publications.DESC())


def impl_defog_academic_gen19():
    """
    PyDough implementation of the following question for the Academic
    graph:

    How many publications were presented at each conference, ordered by the
    number of publications in descending order? Give the names of the conferences
    and their corresponding number of publications.
    """
    return conferences.CALCULATE(name, num_publications=COUNT(proceedings)).ORDER_BY(
        num_publications.DESC(), name
    )


def impl_defog_academic_gen20():
    """
    PyDough implementation of the following question for the Academic
    graph:

    How many publications were published in journals whose names start with the
    letter 'J'?
    """
    selected_publications = publications.WHERE(STARTSWITH(LOWER(publisher.name), "j"))
    return Academic.CALCULATE(n=COUNT(selected_publications))


def impl_defog_academic_gen21():
    """
    PyDough implementation of the following question for the Academic
    graph:

    Which organizations have authors who have written publications in the
    domain 'Machine Learning'?
    """
    ml_publications = (
        authors.author_publications.publication.publication_domains.domain.WHERE(
            name == "Machine Learning"
        )
    )
    return organizations.CALCULATE(
        oranization_name=name, organization_id=organization_id
    ).WHERE(HAS(ml_publications))


def impl_defog_academic_gen22():
    """
    PyDough implementation of the following question for the Academic
    graph:

    Which authors belong to the same domain as Martin?,Always filter names using
    LIKE with percent sign wildcards
    """
    martin_domains = author_domains.domain.domain_authors.author.WHERE(
        CONTAINS(LOWER(name), "martin")
    )

    return authors.WHERE(HAS(martin_domains)).CALCULATE(name, author_id)


def impl_defog_academic_gen23():
    """
    PyDough implementation of the following question for the Academic
    graph:

    Which authors are not part of any organization?
    """
    return authors.WHERE(HASNOT(organization)).CALCULATE(name, author_id)


def impl_defog_academic_gen24():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the publications written by authors from the 'Sociology' domain and
    presented at conferences in the year 2020?
    """

    sociology_author = publication_authors.author.author_domains.domain.WHERE(
        CONTAINS(LOWER(name), "sociology")
    ).domain_conferences.WHERE((conference_id == publication_conference_id))
    return (
        publications.CALCULATE(publication_conference_id=conference_id)
        .WHERE((year == 2020) & HAS(sociology_author))
        .CALCULATE(title)
    )


def impl_defog_academic_gen25():
    """
    PyDough implementation of the following question for the Academic
    graph:

    What are the names of the authors who have written publications in the
    domain 'Computer Science'?
    """
    return (
        authors.CALCULATE(author_name=name)
        .author_publications.publication.publication_domains.domain.WHERE(
            name == "Computer Science"
        )
        .PARTITION(name="authors", by=author_name)
        .CALCULATE(author_name)
    )
