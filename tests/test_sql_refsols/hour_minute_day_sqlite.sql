SELECT
  transaction_id,
  _expr0,
  _expr1,
  _expr2
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
    )
  )
  LEFT JOIN (
    SELECT
      sbTickerId AS _id,
      sbTickerSymbol AS symbol
    FROM main.sbTicker
  )
    ON ticker_id = _id
)
WHERE
  symbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  transaction_id
