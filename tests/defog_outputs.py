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

    NOTE: added an ORDER BY clause at the end to ensure the results were
    deterministic.
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
    ORDER BY sbTickerSymbol, month
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
    Customers who joined on or after January 1, 2023
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
    the country and AR. AR (Activity Ratio) = (Number of Active Customers with
    Transactions / Total Number of Customers with Transactions) * 100.
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

    NOTE: added an ORDER BY clause at the end to ensure the results were
    deterministic.
    """
    return """
    SELECT DISTINCT c.sbCustId
    FROM sbCustomer AS c JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxType = 'buy'
    ORDER BY sbCustId
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


def defog_sql_text_broker_gen1():
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


def defog_sql_text_broker_gen2():
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


def defog_sql_text_broker_gen3():
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


def defog_sql_text_broker_gen4():
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


def defog_sql_text_broker_gen5():
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
