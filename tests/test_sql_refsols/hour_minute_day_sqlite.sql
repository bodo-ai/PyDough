SELECT
  _s0.sbtxid AS transaction_id,
  CAST(STRFTIME('%H', _s0.sbtxdatetime) AS INTEGER) AS _expr0,
  CAST(STRFTIME('%M', _s0.sbtxdatetime) AS INTEGER) AS _expr1,
  CAST(STRFTIME('%S', _s0.sbtxdatetime) AS INTEGER) AS _expr2
FROM main.sbtransaction AS _s0
JOIN main.sbticker AS _s1
  ON _s0.sbtxtickerid = _s1.sbtickerid
  AND _s1.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  transaction_id
