SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer AS _s0
JOIN main.sbtransaction AS _s1
  ON _s0.sbcustid = _s1.sbtxcustid
JOIN main.sbticker AS _s2
  ON _s1.sbtxtickerid = _s2.sbtickerid
  AND _s2.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
WHERE
  _s0.sbcustemail LIKE '%.com'
