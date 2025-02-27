"""
File that holds expected outputs for the defog queries.
"""

__all__ = [
    "defog_sql_text_broker_adv1",
    "defog_sql_text_broker_adv11",
    "defog_sql_text_broker_adv12",
    "defog_sql_text_broker_adv13",
    "defog_sql_text_broker_adv14",
    "defog_sql_text_broker_adv15",
    "defog_sql_text_broker_adv16",
    "defog_sql_text_broker_adv2",
    "defog_sql_text_broker_adv3",
    "defog_sql_text_broker_adv6",
    "defog_sql_text_broker_adv7",
    "defog_sql_text_broker_basic10",
    "defog_sql_text_broker_basic3",
    "defog_sql_text_broker_basic4",
    "defog_sql_text_broker_basic5",
    "defog_sql_text_broker_basic7",
    "defog_sql_text_broker_basic8",
    "defog_sql_text_broker_basic9",
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
