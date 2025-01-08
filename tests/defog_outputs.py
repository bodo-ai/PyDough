"""
File that holds expected outputs for the defog queries.
"""

__all__ = [
    "defog_sql_text_broker_adv1",
    "defog_sql_text_broker_adv11",
    "defog_sql_text_broker_adv12",
    "defog_sql_text_broker_basic3",
    "defog_sql_text_broker_basic4",
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

    Question: What are the top 10 ticker symbols by total transaction amount?
    Return the ticker symbol, number of transactions and total transaction
    amount.
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
