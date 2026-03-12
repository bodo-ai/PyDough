SELECT
  COUNT(*) AS n_customers
FROM broker.sbcustomer
WHERE
  ENDSWITH(sbcustemail, '.com')
  AND EXISTS(
    SELECT
      1 AS "1"
    FROM broker.sbtransaction AS sbtransaction
    JOIN broker.sbticker AS sbticker
      ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
      AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
    WHERE
      sbcustomer.sbcustid = sbtransaction.sbtxcustid
  )
