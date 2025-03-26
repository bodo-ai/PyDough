WITH _table_alias_0 AS (
  SELECT
    CAST(STRFTIME('%H', sbtransaction.sbtxdatetime) AS INTEGER) AS _expr0,
    CAST(STRFTIME('%M', sbtransaction.sbtxdatetime) AS INTEGER) AS _expr1,
    CAST(STRFTIME('%S', sbtransaction.sbtxdatetime) AS INTEGER) AS _expr2,
    sbtransaction.sbtxtickerid AS ticker_id,
    sbtransaction.sbtxid AS transaction_id
  FROM main.sbtransaction AS sbtransaction
), _table_alias_1 AS (
  SELECT
    sbticker.sbtickerid AS _id,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
)
SELECT
  _table_alias_0.transaction_id AS transaction_id,
  _table_alias_0._expr0 AS _expr0,
  _table_alias_0._expr1 AS _expr1,
  _table_alias_0._expr2 AS _expr2
FROM _table_alias_0 AS _table_alias_0
LEFT JOIN _table_alias_1 AS _table_alias_1
  ON _table_alias_0.ticker_id = _table_alias_1._id
WHERE
  _table_alias_1.symbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  _table_alias_0.transaction_id
