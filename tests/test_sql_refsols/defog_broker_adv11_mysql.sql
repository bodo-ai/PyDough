WITH _u_0 AS (
  SELECT
    sbTransaction.sbtxcustid AS _u_1
  FROM main.sbTransaction AS sbTransaction
  JOIN main.sbTicker AS sbTicker
    ON sbTicker.sbtickerid = sbTransaction.sbtxtickerid
    AND sbTicker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n_customers
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbCustomer.sbcustid
WHERE
  NOT _u_0._u_1 IS NULL AND sbCustomer.sbcustemail LIKE '%.com'
