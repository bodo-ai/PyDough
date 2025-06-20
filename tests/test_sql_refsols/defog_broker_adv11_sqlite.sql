SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer AS _s0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction AS _s1
    JOIN main.sbticker AS _s2
      ON _s1.sbtxtickerid = _s2.sbtickerid
      AND _s2.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
    WHERE
      _s0.sbcustid = _s1.sbtxcustid
  )
  AND _s0.sbcustemail LIKE '%.com'
