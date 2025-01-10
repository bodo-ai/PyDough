"""
File that holds expected outputs for the defog queries.
"""

__all__ = [
    "defog_sql_text_broker_adv1",
    "defog_sql_text_broker_adv11",
    "defog_sql_text_broker_adv12",
    "defog_sql_text_broker_adv15",
    "defog_sql_text_broker_adv3",
    "defog_sql_text_broker_adv6",
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
    ;
    """


def defog_sql_text_broker_adv3() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    For customers with at least 5 total transactions, what is their transaction success rate? Return the customer name and success rate, ordered from lowest to highest success rate.
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
    ;
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
    ON ct.sbTxCustId = c.sbCustId;
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
    ;
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
    ;
    """


def defog_sql_text_broker_adv15() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the AR for each country for customers who joined in 2022? Return the country and AR. AR (Activity Ratio) = (Number of Active Customers with Transactions / Total Number of Customers with Transactions) * 100.
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
    ;
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
    ;
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
    ;
    """


def defog_sql_text_broker_basic5() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the average transaction amount for each customer? Return the
    customer name and average transaction amount.

    NOTE: added an ORDER BY clause at the end to ensure the results were
    deterministic.
    """
    return """
    SELECT DISTINCT c.sbCustId
    FROM sbCustomer AS c JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxType = 'buy'
    ORDER BY sbCustId
    ;
    """


def defog_sql_text_broker_basic7() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the total transaction amount for each ticker symbol? Return the
    ticker symbol and total transaction amount.
    """
    return """
    SELECT sbTxStatus, COUNT(*) AS num_transactions
    FROM sbTransaction
    GROUP BY sbTxStatus
    ORDER BY CASE WHEN num_transactions IS NULL THEN 1 ELSE 0 END DESC, num_transactions DESC
    LIMIT 3
    ;
    """


def defog_sql_text_broker_basic8() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the number of transactions for each customer? Return the customer
    name and number of transactions.
    """
    return """
    SELECT sbCustCountry, COUNT(*) AS num_customers
    FROM sbCustomer
    GROUP BY sbCustCountry
    ORDER BY CASE WHEN num_customers IS NULL THEN 1 ELSE 0 END DESC, num_customers DESC
    LIMIT 5
    ;
    """


def defog_sql_text_broker_basic9() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the total transaction amount for each customer state? Return the
    customer state and total transaction amount.
    """
    return """
    SELECT c.sbCustId, c.sbCustName
    FROM sbCustomer AS c
    LEFT JOIN sbTransaction AS t
    ON c.sbCustId = t.sbTxCustId
    WHERE t.sbTxCustId IS NULL
    ;
    """


def defog_sql_text_broker_basic10() -> str:
    """
    SQLite query text for the following question for the Broker graph:

    What is the number of transactions for each ticker type? Return the ticker
    type and number of transactions.
    """
    return """
    SELECT tk.sbTickerId, tk.sbTickerSymbol
    FROM sbTicker AS tk
    LEFT JOIN sbDailyPrice AS dp
    ON tk.sbTickerId = dp.sbDpTickerId
    WHERE dp.sbDpTickerId IS NULL
    ;
    """
