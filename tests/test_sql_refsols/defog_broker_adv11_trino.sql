WITH _u_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS _u_1
  FROM mysql.broker.sbtransaction AS sbtransaction
  JOIN mysql.broker.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n_customers
FROM postgres.main.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL AND sbcustomer.sbcustemail LIKE '%.com'
