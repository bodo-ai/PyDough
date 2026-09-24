WITH _s3 AS (
  SELECT
    sbtransaction.sbtxcustid
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
)
SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer AS sbcustomer
SEMI JOIN _s3 AS _s3
  ON _s3.sbtxcustid = sbcustomer.sbcustid
WHERE
  sbcustomer.sbcustemail LIKE '%.com'
