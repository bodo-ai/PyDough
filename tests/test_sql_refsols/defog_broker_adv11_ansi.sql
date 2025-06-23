SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer AS sbcustomer
JOIN main.sbtransaction AS sbtransaction
  ON sbcustomer.sbcustid = sbtransaction.sbtxcustid
JOIN main.sbticker AS sbticker
  ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
  AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
WHERE
  sbcustomer.sbcustemail LIKE '%.com'
