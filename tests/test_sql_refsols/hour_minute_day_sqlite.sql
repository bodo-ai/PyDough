SELECT
  transaction_id,
  _expr0,
  _expr1,
  _expr2
FROM (
  SELECT
    transaction_id AS ordering_0,
    _expr0,
    _expr1,
    _expr2,
    transaction_id
  FROM (
    SELECT
      _expr0,
      _expr1,
      _expr2,
      symbol,
      transaction_id
    FROM (
      SELECT
        CAST(STRFTIME('%H', date_time) AS INTEGER) AS _expr0,
        CAST(STRFTIME('%M', date_time) AS INTEGER) AS _expr1,
        CAST(STRFTIME('%S', date_time) AS INTEGER) AS _expr2,
        ticker_id,
        transaction_id
      FROM (
        SELECT
          sbTxDateTime AS date_time,
          sbTxId AS transaction_id,
          sbTxTickerId AS ticker_id
        FROM main.sbTransaction
      ) AS _t2
    ) AS _table_alias_0
    LEFT JOIN (
      SELECT
        sbTickerId AS _id,
        sbTickerSymbol AS symbol
      FROM main.sbTicker
    ) AS _table_alias_1
      ON ticker_id = _id
  ) AS _t1
  WHERE
    symbol IN ('AAPL', 'GOOGL', 'NFLX')
) AS _t0
ORDER BY
  ordering_0
