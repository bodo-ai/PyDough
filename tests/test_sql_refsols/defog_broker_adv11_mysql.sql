SELECT
  COUNT(*) AS n_customers
FROM broker.sbCustomer
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM broker.sbTransaction AS sbTransaction
    JOIN broker.sbTicker AS sbTicker
      ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
      AND sbTicker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
    WHERE
      sbCustomer.sbcustid = sbTransaction.sbtxcustid
  )
  AND sbcustemail LIKE '%.com'
