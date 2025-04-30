"""
File that holds expected outputs for the defog queries.
"""

__all__ = [
    "defog_sql_text_broker_adv1",
    "defog_sql_text_broker_adv10",
    "defog_sql_text_broker_adv11",
    "defog_sql_text_broker_adv12",
    "defog_sql_text_broker_adv13",
    "defog_sql_text_broker_adv14",
    "defog_sql_text_broker_adv15",
    "defog_sql_text_broker_adv16",
    "defog_sql_text_broker_adv2",
    "defog_sql_text_broker_adv3",
    "defog_sql_text_broker_adv4",
    "defog_sql_text_broker_adv5",
    "defog_sql_text_broker_adv6",
    "defog_sql_text_broker_adv7",
    "defog_sql_text_broker_adv8",
    "defog_sql_text_broker_adv9",
    "defog_sql_text_broker_basic1",
    "defog_sql_text_broker_basic10",
    "defog_sql_text_broker_basic2",
    "defog_sql_text_broker_basic3",
    "defog_sql_text_broker_basic4",
    "defog_sql_text_broker_basic5",
    "defog_sql_text_broker_basic6",
    "defog_sql_text_broker_basic7",
    "defog_sql_text_broker_basic8",
    "defog_sql_text_broker_basic9",
    "defog_sql_text_broker_gen1",
    "defog_sql_text_broker_gen2",
    "defog_sql_text_broker_gen3",
    "defog_sql_text_broker_gen4",
    "defog_sql_text_broker_gen5",
    "defog_sql_text_dealership_adv1",
    "defog_sql_text_dealership_adv10",
    "defog_sql_text_dealership_adv11",
    "defog_sql_text_dealership_adv12",
    "defog_sql_text_dealership_adv13",
    "defog_sql_text_dealership_adv14",
    "defog_sql_text_dealership_adv15",
    "defog_sql_text_dealership_adv16",
    "defog_sql_text_dealership_adv2",
    "defog_sql_text_dealership_adv3",
    "defog_sql_text_dealership_adv4",
    "defog_sql_text_dealership_adv5",
    "defog_sql_text_dealership_adv6",
    "defog_sql_text_dealership_adv7",
    "defog_sql_text_dealership_adv8",
    "defog_sql_text_dealership_adv9",
    "defog_sql_text_dealership_basic1",
    "defog_sql_text_dealership_basic10",
    "defog_sql_text_dealership_basic2",
    "defog_sql_text_dealership_basic3",
    "defog_sql_text_dealership_basic4",
    "defog_sql_text_dealership_basic5",
    "defog_sql_text_dealership_basic6",
    "defog_sql_text_dealership_basic7",
    "defog_sql_text_dealership_basic8",
    "defog_sql_text_dealership_basic9",
    "defog_sql_text_dealership_gen1",
    "defog_sql_text_dealership_gen2",
    "defog_sql_text_dealership_gen3",
    "defog_sql_text_dealership_gen4",
    "defog_sql_text_dealership_gen5",
    "defog_sql_text_ewallet_adv1",
    "defog_sql_text_ewallet_adv10",
    "defog_sql_text_ewallet_adv11",
    "defog_sql_text_ewallet_adv12",
    "defog_sql_text_ewallet_adv13",
    "defog_sql_text_ewallet_adv14",
    "defog_sql_text_ewallet_adv15",
    "defog_sql_text_ewallet_adv16",
    "defog_sql_text_ewallet_adv2",
    "defog_sql_text_ewallet_adv3",
    "defog_sql_text_ewallet_adv4",
    "defog_sql_text_ewallet_adv5",
    "defog_sql_text_ewallet_adv6",
    "defog_sql_text_ewallet_adv7",
    "defog_sql_text_ewallet_adv8",
    "defog_sql_text_ewallet_adv9",
    "defog_sql_text_ewallet_basic1",
    "defog_sql_text_ewallet_basic10",
    "defog_sql_text_ewallet_basic2",
    "defog_sql_text_ewallet_basic3",
    "defog_sql_text_ewallet_basic4",
    "defog_sql_text_ewallet_basic5",
    "defog_sql_text_ewallet_basic6",
    "defog_sql_text_ewallet_basic7",
    "defog_sql_text_ewallet_basic8",
    "defog_sql_text_ewallet_basic9",
    "defog_sql_text_ewallet_gen1",
    "defog_sql_text_ewallet_gen2",
    "defog_sql_text_ewallet_gen3",
    "defog_sql_text_ewallet_gen4",
    "defog_sql_text_ewallet_gen5",
]


def defog_sql_text_broker_adv1() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Who are the top 5 customers by total transaction amount? Return their name
    and total amount.
    """
    return """
    WITH cust_tx AS (
        SELECT c.sbCustId, c.sbCustName, SUM(t.sbTxAmount) AS total_amount
        FROM sbCustomer AS c
        JOIN sbTransaction AS t ON c.sbCustId = t.sbTxCustId
        GROUP BY c.sbCustId, c.sbCustName)
    SELECT sbCustName, total_amount
    FROM cust_tx
    ORDER BY CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END DESC, total_amount DESC
    LIMIT 5
    """


def defog_sql_text_broker_adv2() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the 2 most frequently bought stock ticker symbols in the past 10
    days? Return the ticker symbol and number of buy transactions.
    """
    return """
    WITH popular_stocks AS (
        SELECT t.sbTickerSymbol, COUNT(*) AS tx_count FROM sbTransaction AS tx
        JOIN sbTicker AS t
        ON tx.sbTxTickerId = t.sbTickerId
        WHERE tx.sbTxType = 'buy' AND tx.sbTxDateTime >= DATE('now', '-10 days')
        GROUP BY t.sbTickerSymbol
    )
    SELECT sbTickerSymbol, tx_count
    FROM popular_stocks
    ORDER BY tx_count DESC
    LIMIT 2
    """


def defog_sql_text_broker_adv3() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    For customers with at least 5 total transactions, what is their
    transaction success rate? Return the customer name and success rate,
    ordered from lowest to highest success rate.
    """
    return """
    WITH cust_tx_stats AS (
        SELECT c.sbCustId, c.sbCustName, COUNT(t.sbTxId) AS total_tx, SUM(CASE WHEN t.sbTxStatus = 'success' THEN 1 ELSE 0 END) AS success_tx
        FROM sbCustomer AS c
        JOIN sbTransaction AS t
        ON c.sbCustId = t.sbTxCustId
        GROUP BY c.sbCustId, c.sbCustName)
    SELECT sbCustName, CAST(success_tx AS FLOAT) / total_tx * 100 AS success_rate
    FROM cust_tx_stats
    WHERE total_tx >= 5
    ORDER BY CASE WHEN success_rate IS NULL THEN 1 ELSE 0 END, success_rate
    """


def defog_sql_text_broker_adv4() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Which 3 distinct stocks had the highest price change between the low and
    high from April 1 2023 to April 4 2023? I want the different in the low and
    high throughout this timerange, not just the intraday price changes. Return
    the ticker symbol and price change.
    """
    return """
    WITH stock_stats AS (
        SELECT t.sbTickerSymbol, MIN(d.sbDpLow) AS min_price, MAX(d.sbDpHigh) AS max_price
        FROM sbDailyPrice AS d
        JOIN sbTicker AS t
        ON d.sbDpTickerId = t.sbTickerId
        WHERE d.sbDpDate BETWEEN '2023-04-01' AND '2023-04-04'
        GROUP BY t.sbTickerSymbol
    )
    SELECT sbTickerSymbol, max_price - min_price AS price_change
    FROM stock_stats ORDER BY CASE WHEN price_change IS NULL THEN 1 ELSE 0 END DESC, price_change DESC
    LIMIT 3
    """


def defog_sql_text_broker_adv5() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the ticker symbol, month, average closing price, highest price,
    lowest price, and MoMC for each ticker by month? MoMC = month-over-month
    change in average closing price, which is calculated as:
    (avg_close_given_month - avg_close_previous_month) /
    avg_close_previous_month for each ticker symbol each month.
    """
    return """
    WITH monthly_price_stats AS (
        SELECT
            strftime('%Y-%m', sbDpDate) AS month,
            sbDpTickerId,
            AVG(sbDpClose) AS avg_close,
            MAX(sbDpHigh) AS max_high,
            MIN(sbDpLow) AS min_low
        FROM sbDailyPrice
        GROUP BY month, sbDpTickerId
    )
    SELECT
        t.sbTickerSymbol,
        mps.month,
        mps.avg_close,
        mps.max_high,
        mps.min_low,
        (mps.avg_close - LAG(mps.avg_close) OVER (PARTITION BY mps.sbDpTickerId ORDER BY mps.month)) / LAG(mps.avg_close) OVER (PARTITION BY mps.sbDpTickerId ORDER BY mps.month) AS mom_change
    FROM monthly_price_stats AS mps
    JOIN sbTicker AS t
    ON mps.sbDpTickerId = t.sbTickerId
    """


def defog_sql_text_broker_adv6() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the customer name, number of transactions, total transaction amount,
    and CR for all customers. CR = customer rank by total transaction amount,
    with rank 1 being the customer with the highest total transaction amount.
    """
    return """
    WITH cust_tx_counts AS (
        SELECT sbTxCustId, COUNT(*) AS num_tx, SUM(sbTxAmount) AS total_amount
        FROM sbTransaction GROUP BY sbTxCustId)
    SELECT c.sbCustName, ct.num_tx, ct.total_amount, RANK() OVER (ORDER BY CASE WHEN ct.total_amount IS NULL THEN 1 ELSE 0 END DESC, ct.total_amount DESC) AS cust_rank
    FROM cust_tx_counts AS ct
    JOIN sbCustomer AS c
    ON ct.sbTxCustId = c.sbCustId
    """


def defog_sql_text_broker_adv7() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the PMCS and PMAT for customers who signed up in the last 6 months
    excluding the current month? PMCS = per month customer signups. PMAT = per
    month average transaction amount. Truncate date to month for aggregation.
    """
    return """
    SELECT
        strftime('%Y-%m', sbCustJoinDate) AS MONTH,
        COUNT(sbCustId) AS customer_signups,
        AVG(t.sbTxAmount) AS avg_tx_amount
    FROM sbCustomer AS c
    LEFT JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    AND strftime('%Y-%m', t.sbTxDateTime) = strftime('%Y-%m', c.sbCustJoinDate)
    WHERE sbCustJoinDate >= date('now', '-6 months', 'start of month')
    AND sbCustJoinDate < date('now', 'start of month')
    GROUP BY MONTH
    """


def defog_sql_text_broker_adv8() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    How many transactions were made by customers from the USA last week
    (exclusive of the current week)? Return the number of transactions and
    total transaction amount.
    """
    return """
    SELECT COUNT(DISTINCT sb.sbTxId) AS num_transactions, SUM(sb.sbTxAmount) AS total_transaction_amount
    FROM sbTransaction AS sb
    JOIN sbCustomer AS sc
    ON sb.sbTxCustId = sc.sbCustId
    WHERE LOWER(sc.sbCustCountry) = 'usa'
    AND sb.sbTxDateTime >= DATE('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days', '-7 days')
    AND sb.sbTxDateTime < DATE('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days')
    """


def defog_sql_text_broker_adv9() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    How many transactions for stocks occurred in each of the last 8 weeks
    excluding the current week? How many of these transactions happened on
    weekends? Weekend days are Saturday and Sunday. Truncate date to week for
    aggregation.
    """
    return """
    SELECT
        DATE(t.sbTxDateTime, '-' || ((strftime('%w', t.sbTxDateTime) + 6) % 7) || ' days') AS WEEK,
        COUNT(t.sbTxId) AS num_transactions,
        COUNT(CASE WHEN strftime('%w', t.sbTxDateTime) IN ('0', '6') THEN 1 END) AS weekend_transactions
    FROM sbTransaction AS t
    JOIN sbTicker AS tk
    ON t.sbTxTickerId = tk.sbTickerId
    WHERE tk.sbTickerType = 'stock'
    AND t.sbTxDateTime >= DATE('now', '-' || ((strftime('%w', 'now') + 6) % 7) || ' days', '-56 days')
    AND t.sbTxDateTime < DATE('now', '-' || ((strftime('%w', 'now') + 6) % 7) || ' days')
    GROUP BY WEEK
    """


def defog_sql_text_broker_adv10() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Which customer made the highest number of transactions in the same month as
    they signed up? Return the customer's id, name and number of transactions.
    """
    return """
    WITH active_customers AS (
        SELECT c.sbCustId, COUNT(t.sbTxId) AS num_transactions
        FROM sbCustomer AS c
        JOIN sbTransaction AS t
        ON c.sbCustId = t.sbTxCustId
        AND strftime('%Y-%m', c.sbCustJoinDate) = strftime('%Y-%m', t.sbTxDateTime) 
        GROUP BY c.sbCustId
    )
    SELECT ac.sbCustId, c.sbCustName, ac.num_transactions
    FROM active_customers AS ac
    JOIN sbCustomer AS c
    ON ac.sbCustId = c.sbCustId
    ORDER BY ac.num_transactions
    DESC LIMIT 1
    """


def defog_sql_text_broker_adv11() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    How many distinct customers with a .com email address bought stocks of
    FAANG companies (Amazon, Apple, Google, Meta or Netflix)?
    """
    return """
    SELECT COUNT(DISTINCT t.sbTxCustId)
    FROM sbTransaction AS t
    JOIN sbCustomer AS c
    ON t.sbTxCustId = c.sbCustId
    JOIN sbTicker AS tk
    ON t.sbTxTickerId = tk.sbTickerId
    WHERE c.sbCustEmail LIKE '%.com'
    AND (tk.sbTickerSymbol LIKE 'AMZN' OR tk.sbTickerSymbol LIKE 'AAPL' OR tk.sbTickerSymbol LIKE 'GOOGL' OR tk.sbTickerSymbol LIKE 'META' OR tk.sbTickerSymbol LIKE 'NFLX')
    """


def defog_sql_text_broker_adv12() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the number of customers whose name starts with J or ends with
    'ez', and who live in a state ending with the letter 'a'?
    """
    return """
    SELECT COUNT(sbCustId)
    FROM sbCustomer
    WHERE (LOWER(sbCustName) LIKE 'j%' OR LOWER(sbCustName) LIKE '%ez')
    AND LOWER(sbCustState) LIKE '%a'
    """


def defog_sql_text_broker_adv13():
    """
    SQLite query text for the following question for the Broker graph:

    How many TAC are there from each country, for customers who joined on or
    after January 1, 2023? Return the country and the count. TAC = Total Active
    customers who joined on or after January 1, 2023
    """
    return """
    SELECT sbCustCountry, COUNT(sbCustId) AS TAC
    FROM sbCustomer
    WHERE sbCustJoinDate >= '2023-01-01'
    GROUP BY sbCustCountry
    """


def defog_sql_text_broker_adv14():
    """
    SQLite query text for the following question for the Broker graph:

    What is the ACP for each ticker type in the past 7 days, inclusive of
    today? Return the ticker type and the average closing price.
    ACP = Average Closing Price of tickers in the last 7 days, inclusive of
    today.
    """
    return """
    SELECT sbTicker.sbTickerType, AVG(sbDailyPrice.sbDpClose) AS ACP
    FROM sbDailyPrice
    JOIN sbTicker
    ON sbDailyPrice.sbDpTickerId = sbTicker.sbTickerId
    WHERE sbDpDate >= DATE('now', '-7 days')
    GROUP BY sbTicker.sbTickerType
    """


def defog_sql_text_broker_adv15() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the AR for each country for customers who joined in 2022? Return
    the country and AR. AR (Activity Ratio) = (Number of Active customers with
    Transactions / Total Number of customers with Transactions) * 100.
    """
    return """
    SELECT
        c.sbCustCountry,
        COALESCE(100.0 * COUNT(DISTINCT CASE WHEN c.sbCustStatus = 'active' THEN c.sbCustId END) / NULLIF(COUNT(DISTINCT t.sbTxCustId), 0), 0) AS AR
    FROM sbCustomer AS c
    JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE c.sbCustJoinDate BETWEEN '2022-01-01' AND '2022-12-31'
    GROUP BY c.sbCustCountry
    """


def defog_sql_text_broker_adv16() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the SPM for each ticker symbol from sell transactions in the past
    month, inclusive of 1 month ago? Return the ticker symbol and SPM.
    SPM (Selling Profit Margin) = (Total Amount from Sells - (Tax + Commission))
    / Total Amount from Sells * 100.

    NOTE: query adjusted to ensure the division is performed as a float
    division instead of integer.
    """
    return """
    SELECT
        sbTickerSymbol,
        CASE WHEN SUM(sbTxAmount) = 0 THEN NULL ELSE (SUM(sbTxAmount) - SUM(sbTxTax + sbTxCommission)) / (1.0 * SUM(sbTxAmount)) * 100 END AS SPM
    FROM sbTransaction
    JOIN sbTicker
    ON sbTransaction.sbTxTickerId = sbTicker.sbTickerId
    WHERE sbTxType = 'sell'
    AND sbTxDateTime >= DATE('now', '-1 month')
    GROUP BY sbTickerSymbol
    """


def defog_sql_text_broker_basic1() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 5 countries by total transaction amount in the past 30
    days, inclusive of 30 days ago? Return the country name, number of
    transactions and total transaction amount.
    """
    return """
    SELECT
        c.sbCustCountry,
        COUNT(t.sbTxId) AS num_transactions,
        SUM(t.sbTxAmount) AS total_amount
    FROM sbCustomer AS c
    JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxDateTime >= DATE('now', '-30 days')
    GROUP BY c.sbCustCountry
    ORDER BY total_amount DESC
    LIMIT 5
    """


def defog_sql_text_broker_basic2() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    How many distinct customers made each type of transaction between Jan 1,
    2023 and Mar 31, 2023 (inclusive of start and end dates)? Return the
    transaction type, number of distinct customers and average number of
    shares, for the top 3 transaction types by number of customers.
    """
    return """
    SELECT
        t.sbTxType,
        COUNT(DISTINCT t.sbTxCustId) AS num_customers,
        AVG(t.sbTxShares) AS avg_shares
    FROM sbTransaction AS t
    WHERE t.sbTxDateTime BETWEEN '2023-01-01' AND '2023-03-31 23:59:59'
    GROUP BY t.sbTxType
    ORDER BY CASE WHEN num_customers IS NULL THEN 1 ELSE 0 END DESC, num_customers DESC
    LIMIT 3
    """


def defog_sql_text_broker_basic3() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Question: What are the top 10 ticker symbols by total transaction amount?
    Return the ticker symbol, number of transactions and total transaction
    amount.
    """
    return """
    SELECT tk.sbTickerSymbol, COUNT(tx.sbTxId) AS num_transactions, SUM(tx.sbTxAmount) AS total_amount
    FROM sbTicker AS tk
    JOIN sbTransaction AS tx
    ON tk.sbTickerId = tx.sbTxTickerId
    GROUP BY tk.sbTickerSymbol
    ORDER BY CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END DESC, total_amount DESC
    LIMIT 10
    """


def defog_sql_text_broker_basic4() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 5 combinations of customer state and ticker type by
    number of transactions? Return the customer state, ticker type and
    number of transactions.
    """
    return """
    SELECT c.sbCustState, t.sbTickerType, COUNT(*) AS num_transactions
    FROM sbTransaction AS tx
    JOIN sbCustomer AS c
    ON tx.sbTxCustId = c.sbCustId
    JOIN sbTicker AS t
    ON tx.sbTxTickerId = t.sbTickerId
    GROUP BY c.sbCustState, t.sbTickerType
    ORDER BY CASE WHEN num_transactions IS NULL THEN 1 ELSE 0 END DESC, num_transactions DESC
    LIMIT 5
    """


def defog_sql_text_broker_basic5() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the distinct list of customer IDs who have made a 'buy' transaction.
    """
    return """
    SELECT DISTINCT c.sbCustId
    FROM sbCustomer AS c JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxType = 'buy'
    """


def defog_sql_text_broker_basic6() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the distinct list of ticker IDs that have daily price records on or
    after Apr 1, 2023.
    """
    return """
    SELECT DISTINCT tk.sbTickerId
    FROM sbTicker AS tk
    JOIN sbDailyPrice AS dp
    ON tk.sbTickerId = dp.sbDpTickerId
    WHERE dp.sbDpDate >= '2023-04-01'
    """


def defog_sql_text_broker_basic7() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 3 transaction statuses by number of transactions? Return
    the status and number of transactions.
    """
    return """
    SELECT sbTxStatus, COUNT(*) AS num_transactions
    FROM sbTransaction
    GROUP BY sbTxStatus
    ORDER BY CASE WHEN num_transactions IS NULL THEN 1 ELSE 0 END DESC, num_transactions DESC
    LIMIT 3
    """


def defog_sql_text_broker_basic8() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 5 countries by number of customers? Return the country
    name and number of customers.
    """
    return """
    SELECT sbCustCountry, COUNT(*) AS num_customers
    FROM sbCustomer
    GROUP BY sbCustCountry
    ORDER BY CASE WHEN num_customers IS NULL THEN 1 ELSE 0 END DESC, num_customers DESC
    LIMIT 5
    """


def defog_sql_text_broker_basic9() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the customer ID and name of customers who have not made any
    transactions.
    """
    return """
    SELECT c.sbCustId, c.sbCustName
    FROM sbCustomer AS c
    LEFT JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxCustId IS NULL
    """


def defog_sql_text_broker_basic10() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the ticker ID and symbol of tickers that do not have any daily
    price records.
    """
    return """
    SELECT tk.sbTickerId, tk.sbTickerSymbol
    FROM sbTicker AS tk
    LEFT JOIN sbDailyPrice AS dp
    ON tk.sbTickerId = dp.sbDpTickerId
    WHERE dp.sbDpTickerId IS NULL
    """


def defog_sql_text_broker_gen1() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the lowest daily closest price for symbol `VTI` in the past 7
    days.
    """
    return """
    SELECT MIN(sdp.sbDpClose) AS lowest_price
    FROM sbDailyPrice AS sdp
    JOIN sbTicker AS st ON sdp.sbDpTickerId = st.sbTickerId
    WHERE st.sbTickerSymbol = 'VTI' AND sdp.sbDpDate >= date('now', '-7 days');
    """


def defog_sql_text_broker_gen2() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the number of transactions by users who joined in the past 70
    days.
    """
    return """
    SELECT COUNT(t.sbTxCustId) AS transaction_count
    FROM sbTransaction AS t
    JOIN sbCustomer AS c ON t.sbTxCustId = c.sbCustId
    WHERE c.sbCustJoinDate >= date('now', '-70 days');
    """


def defog_sql_text_broker_gen3() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the customer id and the difference between their time from
    joining to their first transaction. Ignore customers who haven't made
    any transactions.
    """
    return """
    SELECT c.sbCustId,
    MIN(julianday(t.sbTxDateTime)) - julianday(c.sbCustJoinDate) AS DaysFromJoinToFirstTransaction
    FROM sbCustomer AS c
    JOIN sbTransaction AS t ON c.sbCustId = t.sbTxCustId
    GROUP BY c.sbCustId;
    """


def defog_sql_text_broker_gen4() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the customer who made the most sell transactions on 2023-04-01.
    Return the id, name and number of transactions.
    """
    return """
    WITH SellTransactions AS (
        SELECT sbTxCustId, COUNT(*) AS num_tx
        FROM sbTransaction
        WHERE DATE(sbTxDateTime) = '2023-04-01' AND sbTxType = 'sell'
        GROUP BY sbTxCustId
    )
    SELECT c.sbCustId, c.sbCustName, st.num_tx
    FROM sbCustomer AS c
    JOIN SellTransactions AS st ON c.sbCustId = st.sbTxCustId
    ORDER BY st.num_tx DESC NULLS FIRST
    LIMIT 1;
    """


def defog_sql_text_broker_gen5() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the monthly average transaction price for successful
    transactions in the 1st quarter of 2023?
    """
    return """
    SELECT strftime('%Y-%m-01', sbTxDateTime) AS datetime, AVG(sbTxPrice) AS avg_price FROM sbTransaction
    WHERE sbTxStatus = 'success' AND sbTxDateTime BETWEEN '2023-01-01' AND '2023-03-31'
    GROUP BY datetime
    ORDER BY datetime 
    """


def defog_sql_text_dealership_adv1() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    For sales with sale price over $30,000, how many payments were received in
    total and on weekends in each of the last 8 calendar weeks (excluding the
    current week)? Return the week (as a date), total payments received, and
    weekend payments received in ascending order.
    """
    return """
    SELECT date(p.payment_date, '-' || ((strftime('%w', p.payment_date) + 6) % 7) || ' days') AS week, 
        COUNT(p._id) AS total_payments, 
        COUNT(CASE WHEN strftime('%w', p.payment_date) IN ('0', '6') THEN 1 END) AS weekend_payments
    FROM payments_received AS p
    JOIN sales AS s ON p.sale_id = s._id
    WHERE s.sale_price > 30000
    AND p.payment_date >= date('now', '-' || ((strftime('%w', 'now') + 6) % 7) || ' days', '-56 days')
    AND p.payment_date < date('now', '-' || ((strftime('%w', 'now') + 6) % 7) || ' days')
    GROUP BY week
    ORDER BY week ASC;
    """


def defog_sql_text_dealership_adv2() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    How many sales did each salesperson make in the past 30 days, inclusive of
    today's date? Return their ID, first name, last name and number of sales
    made, ordered from most to least sales.
    """
    return """
    WITH recent_sales AS (
        SELECT sp._id, sp.first_name, sp.last_name, COUNT(s._id) AS num_sales
        FROM salespersons AS sp
        LEFT JOIN sales AS s ON sp._id = s.salesperson_id
        WHERE s.sale_date >= DATE('now', '-30 days')
        GROUP BY sp._id
    ) 
    SELECT _id, first_name, last_name, num_sales FROM recent_sales
    ORDER BY num_sales DESC;
    """


def defog_sql_text_dealership_adv3() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    How many sales were made for each car model that has 'M5' in its VIN
    number? Return the make, model and number of sales. When using car makes,
    model names, engine_type and vin_number, match case-insensitively and allow
    partial matches using LIKE with wildcards.
    """
    return """
    SELECT c.make, c.model, COUNT(s._id) AS num_sales 
    FROM cars AS c 
    LEFT JOIN sales AS s ON c._id = s.car_id 
    WHERE LOWER(c.vin_number) 
    LIKE '%m5%' 
    GROUP BY c.make, c.model;
    """


def defog_sql_text_dealership_adv4() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    How many Toyota cars were sold in the last 30 days inclusive of today?
    Return the number of sales and total revenue.
    """
    return """
    SELECT COUNT(s._id) AS num_sales, SUM(s.sale_price) AS total_revenue FROM sales AS s 
    JOIN cars AS c 
    ON s.car_id = c._id
    WHERE c.make = 'toyota' AND s.sale_date BETWEEN DATE('now', '-30 days') AND DATE('now');
    """


def defog_sql_text_dealership_adv5() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the highest sale price for each make and model of cars that have
    been sold and are no longer in inventory, ordered by the sale price from
    highest to lowest. Use the most recent date in the inventory_snapshots
    table to determine that car's inventory status. When getting a car's
    inventory status, always take the latest status from the
    inventory_snapshots table
    """
    return """
    WITH salesperson_sales AS (
        SELECT 
            salesperson_id, 
            SUM(sale_price) AS total_sales, 
            COUNT(*) AS num_sales 
        FROM sales 
        GROUP BY salesperson_id
    ) 
    SELECT 
        s.first_name, 
        s.last_name, 
        ss.total_sales, 
        ss.num_sales, 
        RANK() OVER (
            ORDER BY 
                CASE WHEN ss.total_sales IS NULL THEN 1 ELSE 0 END DESC, 
                ss.total_sales DESC
        ) AS sales_rank 
    FROM salesperson_sales AS ss 
    JOIN salespersons AS s ON ss.salesperson_id = s._id;
    """


def defog_sql_text_dealership_adv6() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the customer name, number of transactions, total transaction amount,
    and CR for all customers. CR = customer rank by total transaction amount,
    with rank 1 being the customer with the highest total transaction amount.
    """
    return """
    WITH latest_inventory_status AS (
        SELECT 
            car_id, 
            is_in_inventory, 
            ROW_NUMBER() OVER (
                PARTITION BY car_id 
                ORDER BY 
                    CASE WHEN snapshot_date IS NULL THEN 1 ELSE 0 END DESC, 
                    snapshot_date DESC
            ) AS rn
        FROM inventory_snapshots
    ) 
    SELECT 
        c.make, 
        c.model, 
        MAX(s.sale_price) AS highest_sale_price 
    FROM cars AS c 
    JOIN sales AS s ON c._id = s.car_id 
    JOIN latest_inventory_status AS lis ON c._id = lis.car_id 
    WHERE lis.is_in_inventory = FALSE 
    AND lis.rn = 1 
    GROUP BY c.make, c.model 
    ORDER BY 
        CASE WHEN highest_sale_price IS NULL THEN 1 ELSE 0 END DESC, 
        highest_sale_price DESC;
    """


def defog_sql_text_dealership_adv7() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What are the details and average sale price for cars that have 'Ford' in
    their make name or 'Mustang' in the model name? Return make, model, year,
    color, vin_number and avg_sale_price. When using car makes, model names,
    engine_type and vin_number, match case-insensitively and allow partial
    matches using LIKE with wildcards.
    """
    return """
    SELECT c.make, c.model, c.year, c.color, c.vin_number, AVG(s.sale_price) AS avg_sale_price 
    FROM cars AS c 
    JOIN sales AS s 
    ON c._id = s.car_id
    WHERE LOWER(c.make) LIKE '%ford%' OR LOWER(c.model) LIKE '%mustang%' 
    GROUP BY c.make, c.model, c.year, c.color, c.vin_number;
    """


def defog_sql_text_dealership_adv8() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What are the PMSPS and PMSR in the last 6 months excluding the current
    month, for salespersons hired between 2022 and 2023 (both inclusive)?
    Return all months in your answer, including those where metrics are 0.
    Order by month ascending. PMSPS = per month salesperson sales count. PMSR =
    per month sales revenue in dollars. Truncate date to month for aggregation.
    """
    return """
    WITH RECURSIVE date_range(month_start) AS (
        SELECT DATE('now', '-6 months', 'start of month') AS month_start
        UNION ALL
        SELECT DATE(month_start, '+1 month')
        FROM date_range
        WHERE month_start < DATE('now', '-1 month', 'start of month')
    ),
    sales_metrics AS (
        SELECT 
            strftime('%Y-%m', s.sale_date) AS sale_month,
            COUNT(s._id) AS PMSPS,
            SUM(s.sale_price) AS PMSR
        FROM sales AS s
        JOIN salespersons AS sp ON s.salesperson_id = sp._id
        WHERE 
            strftime('%Y', sp.hire_date) BETWEEN '2022' AND '2023'
            AND s.sale_date >= DATE('now', '-6 months', 'start of month')
            AND s.sale_date < DATE('now', 'start of month')
        GROUP BY sale_month
    )
    SELECT 
        dr.month_start,
        COALESCE(sm.PMSPS, 0) AS PMSPS,
        COALESCE(sm.PMSR, 0) AS PMSR
    FROM date_range AS dr
    LEFT JOIN sales_metrics AS sm 
        ON strftime('%Y-%m', dr.month_start) = sm.sale_month
    ORDER BY dr.month_start ASC;
    """


def defog_sql_text_dealership_adv9() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the ASP for sales made in the first quarter of 2023? ASP = Average
    Sale Price in the first quarter of 2023.
    """
    return """
    SELECT AVG(sale_price) AS ASP 
    FROM sales 
    WHERE sale_date >= '2023-01-01' AND sale_date <= '2023-03-31';
    """


def defog_sql_text_dealership_adv10() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the average number of days between the sale date and payment
    received date, rounded to 2 decimal places?
    """
    return """
    WITH sale_payments AS (SELECT s._id AS sale_id, s.sale_date, MAX(p.payment_date) AS latest_payment_date 
    FROM sales AS s 
    JOIN payments_received AS p 
    ON s._id = p.sale_id 
    GROUP BY s._id, s.sale_date) 
    SELECT ROUND(AVG(julianday(latest_payment_date) - julianday(sale_date)), 2) AS avg_days_to_payment
    FROM sale_payments;
    """


def defog_sql_text_dealership_adv11() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the GPM for all car sales in 2023? GPM (gross profit margin) =
    (total revenue - total cost) / total cost * 100
    """
    return """ 
    SELECT (SUM(sale_price) - SUM(cars.cost)) / SUM(cars.cost) * 100 AS gpm 
    FROM sales JOIN cars 
    ON sales.car_id = cars._id 
    WHERE strftime('%Y', sale_date) = '2023';
    """


def defog_sql_text_dealership_adv12() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the make, model and sale price of the car with the highest sale
    price that was sold on the same day it went out of inventory?
    """
    return """
    SELECT c.make, c.model, s.sale_price 
    FROM cars AS c 
    JOIN sales AS s ON c._id = s.car_id 
    JOIN inventory_snapshots AS i 
    ON c._id = i.car_id AND DATE(s.sale_date) = DATE(i.snapshot_date) 
    WHERE i.is_in_inventory = 0 ORDER BY s.sale_price DESC LIMIT 1;
    """


def defog_sql_text_dealership_adv13():
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the total payments received per month? Also calculate the MoM
    change for each month. MoM change = (current month value - prev month
    value). Return all months in your answer, including those where there were
    no payments.
    """
    return """
    WITH monthly_totals AS (
        SELECT 
            strftime('%Y-%m-01 00:00:00', payment_date) AS dt,
            SUM(payment_amount) AS total_payments
        FROM payments_received
        GROUP BY dt
    ),
    monthly_totals_with_zero AS (
        SELECT dt, total_payments FROM monthly_totals
        UNION ALL
        SELECT 
            strftime('%Y-%m-01 00:00:00', date(payment_date, 'start of month', '+' || (n || ' month'))) AS dt,
            0 AS total_payments
        FROM payments_received, 
        (
            SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL 
            SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL 
            SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL 
            SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11
        )
        WHERE strftime('%Y-%m-01 00:00:00', date(payment_date, 'start of month', '+' || (n || ' month'))) 
            <= strftime('%Y-%m-01 00:00:00', 'now')
        GROUP BY dt
    )
    SELECT 
        dt AS MONTH, 
        SUM(total_payments) AS total_payments,
        SUM(total_payments) - LAG(SUM(total_payments), 1) OVER (ORDER BY dt) AS mom_change
    FROM monthly_totals_with_zero
    GROUP BY dt
    ORDER BY dt;
    """


def defog_sql_text_dealership_adv14():
    """
    SQLite query text for the following question for the Car Dealership graph:

    What is the TSC in the past 7 days, inclusive of today? TSC = Total Sales
    Count.
    """
    return """
    SELECT COUNT(_id) AS TSC 
    FROM sales 
    WHERE sale_date >= DATE('now', '-7 days');
    """


def defog_sql_text_dealership_adv15() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Who are the top 3 salespersons by ASP? Return their first name, last name
    and ASP. ASP (average selling price) = total sales amount / number of sales
    """
    return """
    SELECT salespersons.first_name, salespersons.last_name, AVG(sales.sale_price) AS ASP 
    FROM sales JOIN salespersons ON sales.salesperson_id = salespersons._id 
    GROUP BY salespersons.first_name, salespersons.last_name 
    ORDER BY ASP DESC LIMIT 3;
    """


def defog_sql_text_dealership_adv16() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Who are the top 5 salespersons by total sales amount? Return their ID,
    first name, last name and total sales amount.
    """
    return """
    WITH salesperson_sales AS (SELECT s._id, s.first_name, s.last_name, SUM(sa.sale_price) AS total_sales 
    FROM salespersons AS s 
    LEFT JOIN sales AS sa 
    ON s._id = sa.salesperson_id 
    GROUP BY s._id) 
    SELECT _id, first_name, last_name, total_sales 
    FROM salesperson_sales
    ORDER BY total_sales DESC LIMIT 5;
    """


def defog_sql_text_dealership_basic1() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the car ID, make, model and year for cars that have no sales
    records, by doing a left join from the cars to sales table.
    """
    return """
    SELECT c._id AS car_id, c.make, c.model, c.year 
    FROM cars AS c 
    LEFT JOIN sales AS s 
    ON c._id = s.car_id 
    WHERE s.car_id IS NULL;
    """


def defog_sql_text_dealership_basic2() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the distinct list of customer IDs that have made a purchase, based
    on joining the customers and sales tables.
    """
    return """
    SELECT DISTINCT c._id AS customer_id 
    FROM customers  AS c 
    JOIN sales  AS s 
    ON c._id = s.customer_id;
    """


def defog_sql_text_dealership_basic3() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the distinct list of salesperson IDs that have received a cash
    payment, based on joining the salespersons, sales and payments_received
    tables.
    """
    return """
    SELECT DISTINCT s._id AS salesperson_id 
    FROM salespersons AS s 
    JOIN sales AS sa 
    ON s._id = sa.salesperson_id 
    JOIN payments_received AS p 
    ON sa._id = p.sale_id 
    WHERE p.payment_method = 'cash';
    """


def defog_sql_text_dealership_basic4() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the salesperson ID, first name and last name for salespersons that
    have no sales records, by doing a left join from the salespersons to sales
    table.
    """
    return """
    SELECT s._id AS salesperson_id, s.first_name, s.last_name 
    FROM salespersons AS s 
    LEFT JOIN sales AS sa 
    ON s._id = sa.salesperson_id 
    WHERE sa.salesperson_id IS NULL;
    """


def defog_sql_text_dealership_basic5() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the top 5 salespersons by number of sales in the past 30 days?
    Return their first and last name, total sales count and total revenue
    amount.
    """
    return """
    SELECT sp.first_name, sp.last_name, COUNT(s._id) AS total_sales, SUM(s.sale_price) AS total_revenue 
    FROM sales AS s 
    JOIN salespersons AS sp 
    ON s.salesperson_id = sp._id 
    WHERE s.sale_date >= DATE('now', '-30 days') 
    GROUP BY sp.first_name, sp.last_name, sp._id 
    ORDER BY total_sales DESC LIMIT 5;
    """


def defog_sql_text_dealership_basic6() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the top 5 states by total revenue, showing the number of unique
    customers and total revenue (based on sale price) for each state.
    """
    return """
    SELECT c.state, COUNT(DISTINCT s.customer_id) AS unique_customers, SUM(s.sale_price) AS total_revenue 
    FROM sales AS s 
    JOIN customers AS c 
    ON s.customer_id = c._id 
    GROUP BY c.state 
    ORDER BY CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END DESC, total_revenue DESC LIMIT 5;
    """


def defog_sql_text_dealership_basic7() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What are the top 3 payment methods by total payment amount received? Return
    the payment method, total number of payments and total amount.
    """
    return """
    SELECT payment_method, COUNT(*) AS total_payments, 
    SUM(payment_amount) AS total_amount 
    FROM payments_received 
    GROUP BY payment_method 
    ORDER BY CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END DESC, total_amount DESC LIMIT 3;
    """


def defog_sql_text_dealership_basic8() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What are the top 5 best selling car models by total revenue? Return the
    make, model, total number of sales and total revenue.
    """
    return """
    SELECT c.make, c.model, COUNT(s._id) AS total_sales, 
    SUM(s.sale_price) AS total_revenue 
    FROM sales AS s 
    JOIN cars AS c 
    ON s.car_id = c._id 
    GROUP BY c.make, c.model 
    ORDER BY CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END DESC, total_revenue DESC LIMIT 5;
    """


def defog_sql_text_dealership_basic9() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What are the total number of customer signups for the top 2 states? Return
    the state and total signups, starting from the top.
    """
    return """
    SELECT state, COUNT(*) AS total_signups 
    FROM customers 
    GROUP BY state 
    ORDER BY CASE WHEN total_signups IS NULL THEN 1 ELSE 0 END DESC, total_signups DESC LIMIT 2;
    """


def defog_sql_text_dealership_basic10() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Who were the top 3 sales representatives by total revenue in the past 3
    months, inclusive of today's date? Return their first name, last name,
    total number of sales and total revenue. Note that revenue refers to the
    sum of sale_price in the sales table.
    """
    return """
    SELECT c.first_name, c.last_name, COUNT(s._id) AS total_sales, 
    SUM(s.sale_price) AS total_revenue 
    FROM sales AS s 
    JOIN salespersons AS c ON s.salesperson_id = c._id 
    WHERE s.sale_date >= DATE('now', '-3 months') 
    GROUP BY c.first_name, c.last_name 
    ORDER BY total_revenue DESC LIMIT 3;
    """


def defog_sql_text_dealership_gen1() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the name and phone number of the salesperson with the shortest time
    from being hired to getting fired. Return the number of days he/she was
    employed for.
    """
    return """
    SELECT s.first_name, s.last_name, s.phone, julianday(s.termination_date) - julianday(s.hire_date) AS days_employed 
    FROM salespersons AS s 
    ORDER BY CASE WHEN days_employed IS NULL THEN 1 ELSE 0 END, days_employed ASC LIMIT 1;
    """


def defog_sql_text_dealership_gen2() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Return the number of payments made on weekends to the vendor named 'Utility
    Company'
    """
    return """
    SELECT COUNT(*) AS weekend_payments 
    FROM payments_made 
    WHERE vendor_name = 'Utility Company' 
    AND strftime('%w', payment_date) IN ('0', '6');
    """


def defog_sql_text_dealership_gen3() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Show me the daily total amount of payments received in the whole of the
    previous ISO week not including the current week, split by the
    payment_method.
    """
    return """
    SELECT payment_date, payment_method, SUM(payment_amount) AS total_amount 
    FROM payments_received 
    WHERE payment_date >= DATE('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days', '-7 days') 
    AND payment_date < DATE('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days') 
    GROUP BY payment_date, payment_method ORDER BY payment_date DESC, payment_method ASC;
    """


def defog_sql_text_dealership_gen4() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    What were the total quarterly sales in 2023 grouped by customer's state?
    Represent each quarter as the first date in the quarter.
    """
    return """
    SELECT CASE WHEN strftime('%m', s.sale_date) BETWEEN '01' AND '03' THEN '2023-01-01' 
    WHEN strftime('%m', s.sale_date) BETWEEN '04' AND '06' THEN '2023-04-01' 
    WHEN strftime('%m', s.sale_date) BETWEEN '07' AND '09' THEN '2023-07-01' ELSE '2023-10-01' END AS quarter, 
    c.state, SUM(s.sale_price) AS total_sales 
    FROM sales AS s 
    JOIN customers AS c 
    ON s.customer_id = c._id 
    WHERE strftime('%Y', s.sale_date) = '2023' 
    GROUP BY c.state, quarter 
    HAVING SUM(s.sale_price) > 0 
    ORDER BY quarter, c.state;
    """


def defog_sql_text_dealership_gen5() -> str:
    """
    SQLite query text for the following question for the Car Dealership graph:

    Which cars were in inventory in the latest snapshot for march 2023? Return
    the car id, make, model, and year. Cars are considered to be in inventory"
    if is_in_inventory is True."
    """
    return """
    WITH latest_snapshot AS (SELECT MAX(snapshot_date) AS snapshot_date 
    FROM inventory_snapshots 
    WHERE snapshot_date BETWEEN '2023-03-01' AND '2023-03-31'), latest_snapshot_data AS 
    (SELECT inv.car_id 
    FROM inventory_snapshots AS inv 
    JOIN latest_snapshot AS ls 
    ON inv.snapshot_date = ls.snapshot_date WHERE inv.is_in_inventory = TRUE) 
    SELECT c._id, c.make, c.model, c.year 
    FROM cars AS c 
    JOIN latest_snapshot_data AS lsd 
    ON c._id = lsd.car_id;
    """


def defog_sql_text_ewallet_adv1() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Calculate the CPUR for each merchant, considering only successful
    transactions. Return the merchant name and CPUR. CPUR (coupon usage
    rate) = number of distinct coupons used / number of distinct transactions
    """
    return """
    SELECT m.name, (COUNT(DISTINCT wtd.coupon_id) * 1.0 / NULLIF(COUNT(DISTINCT wtd.txid), 0)) AS CPUR 
    FROM wallet_transactions_daily AS wtd 
    JOIN merchants AS m 
    ON wtd.receiver_id = m.mid 
    WHERE wtd.status = 'success' 
    GROUP BY m.name;
    """


def defog_sql_text_ewallet_adv2() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    For users in the US and Canada, how many total notifications were sent in
    each of the last 3 weeks excluding the current week? How many of those
    were sent on weekends? Weekends are Saturdays and Sundays. Truncate
    created_at to week for aggregation.
    """
    return """
    SELECT date(n.created_at,  '-' || ((strftime('%w', n.created_at) + 6) % 7) || ' days') AS WEEK, 
    COUNT(*) AS total_notifications, 
    COUNT(CASE WHEN strftime('%w', n.created_at) IN ('0', '6') THEN 1 END) AS weekend_notifications 
    FROM notifications AS n JOIN users AS u ON n.user_id = u.uid 
    WHERE u.country IN ('US', 'CA') 
    AND n.created_at >= date('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days', '-21 days') 
    AND n.created_at < date('now',  '-' || ((strftime('%w', 'now') + 6) % 7) || ' days') 
    GROUP BY WEEK;
    """


def defog_sql_text_ewallet_adv3() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    How many active retail merchants have issued coupons? Return the merchant
    name and the total number of coupons issued. Merchant category should be
    matched case-insensitively.
    """
    return """
    SELECT m.name, COUNT(c.cid) AS total_coupons
    FROM merchants AS m
    JOIN coupons AS c ON m.mid = c.merchant_id
    WHERE m.status = 'active'
    AND LOWER(m.category) LIKE '%retail%'
    GROUP BY m.name;
    """


def defog_sql_text_ewallet_adv4() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    How many wallet transactions were made by users from the US in the last 7
    days inclusive of today? Return the number of transactions and total
    transaction amount. Last 7 days = DATE('now', -'7 days') to DATE('now').
    Always join wallet_transactions_daily with users before using the
    wallet_transactions_daily table.
    """
    return """
    SELECT COUNT(*) AS num_transactions, SUM(amount) AS total_amount 
    FROM wallet_transactions_daily AS t 
    JOIN users AS u ON t.sender_id = u.uid 
    WHERE u.country = 'US' AND t.created_at >= DATE('now', '-7 days') 
    AND t.created_at < DATE('now', '+1 day');
    """


def defog_sql_text_ewallet_adv5() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the average AMB for user wallets updated in the past week,
    inclusive of 7 days ago? Return the average balance. AMB = average balance
    per user (for the given time duration).
    """
    return """
    SELECT AVG(balance) AS AMB
    FROM wallet_user_balance_daily
    WHERE updated_at >= DATE('now', '-7 days');
    """


def defog_sql_text_ewallet_adv6() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the LUB for each user. LUB = Latest User Balance, which is the most
    recent balance for each user
    """
    return """
    WITH user_balances AS (
        SELECT user_id, balance, ROW_NUMBER() OVER (PARTITION BY user_id 
        ORDER BY CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END DESC, updated_at DESC) AS rn
        FROM wallet_user_balance_daily
    )
    SELECT user_id, balance
    FROM user_balances
    WHERE rn = 1;
    """


def defog_sql_text_ewallet_adv7() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the marketing opt-in preference for each user? Return the user ID
    and boolean opt-in value. To get any user's settings, only select the
    latest snapshot of user_setting_snapshot for each user.
    """
    return """
    WITH user_latest_setting AS (
    SELECT u.uid, s.marketing_opt_in, s.created_at, ROW_NUMBER() 
    OVER (PARTITION BY u.uid ORDER BY CASE WHEN s.created_at IS NULL THEN 1 ELSE 0 END DESC, s.created_at DESC) AS rn
    FROM users AS u
    JOIN user_setting_snapshot AS s ON u.uid = s.user_id
    )
    SELECT UID, marketing_opt_in 
    FROM user_latest_setting 
    WHERE rn = 1;
    """


def defog_sql_text_ewallet_adv8() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the MRR for each merchant? Return the merchant name, category,
    revenue amount, and revenue rank. MRR = Merchant Revenue Rank, which ranks
    merchants based on amounts from successfully received transactions only.
    """
    return """
    WITH merchant_revenue AS (
        SELECT m.mid, m.name, m.category AS merchant_category, SUM(w.amount) AS total_revenue
        FROM merchants AS m
        INNER JOIN wallet_transactions_daily AS w ON m.mid = w.receiver_id AND w.receiver_type = 1
        WHERE w.status = 'success'
        GROUP BY m.mid, m.name, m.category
    )
    SELECT *, RANK() OVER (ORDER BY CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END DESC, total_revenue DESC) AS mrr
    FROM merchant_revenue;
    """


def defog_sql_text_ewallet_adv9() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the PMDAU (Per Month Daily Active Users) for wallet transactions in
    the last 2 months excluding the current month? PMDAU (Per Month Daily
    Active Users) = COUNT(DISTINCT(sender_id) ... WHERE t.sender_type = 0.
    Truncate created_at to month for aggregation.
    """
    return """
    SELECT strftime('%Y-%m-01', t.created_at) AS month, COUNT(DISTINCT t.sender_id) AS active_users
    FROM wallet_transactions_daily AS t
    WHERE t.sender_type = 0
    AND t.created_at >= date('now', 'start of month', '-2 months')
    AND t.created_at < date('now', 'start of month')
    GROUP BY month;
    """


def defog_sql_text_ewallet_adv10() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the total number of wallet transactions sent by each user that is
    not a merchant? Return the user ID and total transaction count.
    """
    return """
    WITH user_transactions AS (
        SELECT u.uid, t.txid
        FROM users AS u
        JOIN wallet_transactions_daily AS t ON u.uid = t.sender_id
        WHERE t.sender_type = 0
    )
    SELECT UID, COUNT(txid) AS total_transactions
    FROM user_transactions
    GROUP BY UID;
    """


def defog_sql_text_ewallet_adv11() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the total session duration in seconds for each user between
    2023-06-01 inclusive and 2023-06-08 exclusive? Return the user ID and their
    total duration as an integer sorted by total duration with the longest
    duration first.
    """
    return """
    WITH user_session_duration AS (
        SELECT u.uid, s.session_start_ts, s.session_end_ts
        FROM users AS u
        JOIN user_sessions AS s ON u.uid = s.user_id
        WHERE s.session_start_ts >= '2023-06-01'
        AND s.session_end_ts < '2023-06-08'
    )
    SELECT uid, SUM(strftime('%s', session_end_ts) - strftime('%s', session_start_ts)) AS total_duration
    FROM user_session_duration
    GROUP BY uid
    ORDER BY total_duration DESC;
    """


def defog_sql_text_ewallet_adv12() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the total transaction amount for each coupon offered by merchant
    with ID 1? Return the coupon ID and total amount transacted with it.
    """
    return """
    WITH merchant_coupon_usage AS (
        SELECT c.cid, t.amount
        FROM coupons AS c
        JOIN wallet_transactions_daily AS t ON c.cid = t.coupon_id
        WHERE c.merchant_id = 1
    )
    SELECT cid, SUM(amount) AS total_discount
    FROM merchant_coupon_usage
    GROUP BY cid;
    """


def defog_sql_text_ewallet_adv13() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What is the TUC in the past month, inclusive of 1 month ago? Return the
    total count. TUC = Total number of user sessions in the past month
    """
    return """
    SELECT COUNT(*) AS TUC
    FROM user_sessions
    WHERE session_start_ts >= DATE('now', '-1 month')
    OR session_end_ts >= DATE('now', '-1 month');
    """


def defog_sql_text_ewallet_adv14() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What was the STR for wallet transactions in the previous month? STR
    (success transaction rate) = number of successful transactions / total
    number of transactions.
    """
    return """
    SELECT (SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) AS STR
    FROM wallet_transactions_daily
    WHERE strftime('%Y-%m', created_at) = strftime('%Y-%m', 'now', 'start of month', '-1 month');
    """


def defog_sql_text_ewallet_adv15() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Which merchant created the highest number of coupons within the same month
    that the merchant was created (coupon or merchant can be created earlier
    than the other)? Return the number of coupons along with the merchant's id
    and name.
    """
    return """
    WITH coupons_per_merchant AS (
        SELECT m.mid, COUNT(c.cid) AS num_coupons
        FROM coupons AS c
        JOIN merchants AS m ON m.mid = c.merchant_id AND strftime('%Y-%m', c.created_at) = strftime('%Y-%m', m.created_at)
        GROUP BY m.mid
    )
    SELECT coupons_per_merchant.mid, m.name, coupons_per_merchant.num_coupons
    FROM coupons_per_merchant
    JOIN merchants AS m USING (mid)
    ORDER BY coupons_per_merchant.num_coupons DESC
    LIMIT 1;
    """


def defog_sql_text_ewallet_adv16() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Which users from the US have unread promotional notifications? Return the
    username and the total number of unread promotional notifications. User
    country should be matched case-insensitively, e.g., LOWER(users.country) =
    'us'. Notification type and status should be matched exactly.
    """
    return """
    SELECT u.username, COUNT(n.id) AS total_notifications
    FROM users AS u
    JOIN notifications AS n ON u.uid = n.user_id
    WHERE n.type = 'promotion'
    AND n.status = 'unread'
    AND LOWER(u.country) = 'us'
    GROUP BY u.username;
    """


def defog_sql_text_ewallet_basic1() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    How many distinct active users sent money per month in 2023? Return the
    number of active users per month (as a date), starting from the earliest
    date. Do not include merchants in the query. Only include successful
    transactions.
    """
    return """
    SELECT strftime('%Y-%m-01', t.created_at) AS month, 
    COUNT(DISTINCT t.sender_id) AS active_users 
    FROM wallet_transactions_daily AS t 
    JOIN users AS u ON t.sender_id = u.uid 
    WHERE t.sender_type = 0 AND t.status = 'success' 
    AND u.status = 'active' AND t.created_at >= '2023-01-01' 
    AND t.created_at < '2024-01-01' 
    GROUP BY month ORDER BY month;
    """


def defog_sql_text_ewallet_basic10() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Who are the top 2 merchants (receiver type 1) by total transaction amount
    in the past 150 days (inclusive of 150 days ago)? Return the merchant name,
    total number of transactions, and total transaction amount.
    """
    return """
    SELECT m.name AS merchant_name, COUNT(t.txid) AS total_transactions, SUM(t.amount) AS total_amount 
    FROM merchants AS m 
    JOIN wallet_transactions_daily 
    AS t ON m.mid = t.receiver_id 
    WHERE t.receiver_type = 1 AND t.created_at >= DATE('now', '-150 days') 
    GROUP BY m.name ORDER BY total_amount DESC LIMIT 2;
    """


def defog_sql_text_ewallet_basic2() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return merchants (merchant ID and name) who have not issued any coupons.
    """
    return """
    SELECT m.mid AS merchant_id, m.name AS merchant_name 
    FROM merchants AS m 
    LEFT JOIN coupons AS c 
    ON m.mid = c.merchant_id 
    WHERE c.cid IS NULL;
    """


def defog_sql_text_ewallet_basic3() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the distinct list of merchant IDs that have received money from a
    transaction. Consider all transaction types in the results you return,
    but only include the merchant ids in your final answer.
    """
    return """
    SELECT DISTINCT m.mid AS merchant_id 
    FROM merchants AS m 
    JOIN wallet_transactions_daily AS t 
    ON m.mid = t.receiver_id 
    WHERE t.receiver_type = 1;
    """


def defog_sql_text_ewallet_basic4() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return the distinct list of user IDs who have received transaction
    notifications.
    """
    return """
    SELECT DISTINCT user_id 
    FROM notifications 
    WHERE type = 'transaction';
    """


def defog_sql_text_ewallet_basic5() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Return users (user ID and username) who have not received any
    notifications.
    """
    return """
    SELECT u.uid, u.username 
    FROM users AS u LEFT JOIN notifications AS n 
    ON u.uid = n.user_id 
    WHERE n.id IS NULL;
    """


def defog_sql_text_ewallet_basic6() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 2 most frequently used device types for user sessions
    and their respective counts?
    """
    return """
    SELECT device_type, COUNT(*) AS COUNT 
    FROM user_sessions 
    GROUP BY device_type 
    ORDER BY CASE WHEN COUNT IS NULL THEN 1 ELSE 0 END DESC, COUNT DESC LIMIT 2;
    """


def defog_sql_text_ewallet_basic7() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 3 most common transaction statuses and their respective
    counts?
    """
    return """
    SELECT status, COUNT(*) AS COUNT 
    FROM wallet_transactions_daily 
    GROUP BY status 
    ORDER BY CASE WHEN COUNT IS NULL THEN 1 ELSE 0 END DESC, COUNT DESC LIMIT 3;
    """


def defog_sql_text_ewallet_basic8() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What are the top 3 most frequently used coupon codes? Return the coupon
    code, total number of redemptions, and total amount redeemed.
    """
    return """
    SELECT c.code AS coupon_code, COUNT(t.txid) AS redemption_count, SUM(t.amount) AS total_discount 
    FROM coupons AS c 
    JOIN wallet_transactions_daily AS t ON c.cid = t.coupon_id 
    GROUP BY c.code 
    ORDER BY CASE WHEN redemption_count IS NULL THEN 1 ELSE 0 END DESC, redemption_count DESC LIMIT 3;
    """


def defog_sql_text_ewallet_basic9() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    Which are the top 5 countries by total transaction amount sent by users,
    sender_type = 0? Return the country, number of distinct users who sent,
    and total transaction amount.
    """
    return """
    SELECT u.country, COUNT(DISTINCT t.sender_id) AS user_count, SUM(t.amount) AS total_amount 
    FROM users AS u 
    JOIN wallet_transactions_daily AS t ON u.uid = t.sender_id 
    WHERE t.sender_type = 0 
    GROUP BY u.country 
    ORDER BY CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END DESC, total_amount DESC LIMIT 5;
    """


def defog_sql_text_ewallet_gen1() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Give me today's median merchant wallet balance for all active merchants
    whose category contains 'retail'.
    """
    return """
    WITH retail_merchants AS (
        SELECT mid
        FROM merchants
        WHERE LOWER(category) LIKE LOWER('%retail%')
        AND status = 'active'
    ), merchant_balances AS (
        SELECT balance
        FROM wallet_merchant_balance_daily AS wmbd
        JOIN retail_merchants AS rm ON wmbd.merchant_id = rm.mid
        WHERE DATE(wmbd.updated_at) = date('now')
    )
    SELECT AVG(balance) AS median_balance
    FROM (
        SELECT balance
        FROM merchant_balances
        ORDER BY balance
        LIMIT 2 - (
            SELECT COUNT(*)
            FROM merchant_balances
        ) % 2 OFFSET (
            SELECT (COUNT(*) - 1) / 2
            FROM merchant_balances
        )
    );
    """


def defog_sql_text_ewallet_gen2() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    What was the average transaction daily and monthly limit for the earliest
    setting snapshot in 2023?
    """
    return """
    SELECT AVG(tx_limit_daily) AS avg_daily_limit, AVG(tx_limit_monthly) AS avg_monthly_limit
    FROM user_setting_snapshot
    WHERE snapshot_date = (
        SELECT MIN(snapshot_date)
        FROM user_setting_snapshot
        WHERE snapshot_date >= '2023-01-01'
        AND snapshot_date < '2024-01-01'
    );
    """


def defog_sql_text_ewallet_gen3() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    what was the average user session duration in seconds split by device_type?
    """
    return """
    SELECT device_type, AVG(strftime('%s', session_end_ts) - strftime('%s', session_start_ts)) AS avg_session_duration_seconds
    FROM user_sessions
    WHERE session_end_ts IS NOT NULL
    GROUP BY device_type;
    """


def defog_sql_text_ewallet_gen4() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Which merchants earliest coupon start date was within a year of the
    merchant's registration? Return the merchant id, registration date, and
    earliest coupon id and start date
    """
    return """
    WITH earliest_coupons AS (
        SELECT c.merchant_id, MIN(c.start_date) AS earliest_coupon_start_date
        FROM coupons AS c
        GROUP BY c.merchant_id
    )
    SELECT m.mid AS merchant_id, m.created_at AS merchant_registration_date, ec.earliest_coupon_start_date, c.cid AS earliest_coupon_id
    FROM merchants AS m
    JOIN earliest_coupons AS ec ON m.mid = ec.merchant_id
    JOIN coupons AS c ON ec.merchant_id = c.merchant_id AND ec.earliest_coupon_start_date = c.start_date
    WHERE ec.earliest_coupon_start_date <= date(m.created_at, '+1 year');
    """


def defog_sql_text_ewallet_gen5() -> str:
    """
    SQLite query text for the following question for the eWallet graph:

    Which users did not get a notification within the first year of signing up?
    Return their usernames, emails and signup dates.
    """
    return """
    SELECT u.username, u.email, u.created_at 
    FROM users AS u LEFT JOIN notifications AS n 
    ON u.uid = n.user_id 
    AND n.created_at BETWEEN u.created_at 
    AND date(u.created_at, '+1 year') 
    WHERE n.user_id IS NULL;
    """
