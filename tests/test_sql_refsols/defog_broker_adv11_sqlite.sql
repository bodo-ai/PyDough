WITH _u_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS _u_1
  FROM main.sbtransaction AS sbtransaction
  JOIN main.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  GROUP BY
    sbtransaction.sbtxcustid
)
SELECT
  COUNT(*) AS n_customers
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL AND sbcustomer.sbcustemail LIKE '%.com'
