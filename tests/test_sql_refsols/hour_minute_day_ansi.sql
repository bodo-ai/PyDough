SELECT
  _s0.sbtxid AS transaction_id,
  EXTRACT(HOUR FROM _s0.sbtxdatetime) AS _expr0,
  EXTRACT(MINUTE FROM _s0.sbtxdatetime) AS _expr1,
  EXTRACT(SECOND FROM _s0.sbtxdatetime) AS _expr2
FROM main.sbtransaction AS _s0
JOIN main.sbticker AS _s1
  ON _s0.sbtxtickerid = _s1.sbtickerid
  AND _s1.sbtickersymbol IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  transaction_id
