SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbtransaction AS sbtransaction
    JOIN main.sbticker AS sbticker
      ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
      AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
    WHERE
      sbcustomer.sbcustid = sbtransaction.sbtxcustid
  )
  AND sbcustemail LIKE '%.com'
