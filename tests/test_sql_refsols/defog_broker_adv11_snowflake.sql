WITH _u_0 AS (
  SELECT
    sbtransaction.sbtxcustid AS _u_1
  FROM broker.sbtransaction AS sbtransaction
  JOIN broker.sbticker AS sbticker
    ON sbticker.sbtickerid = sbtransaction.sbtxtickerid
    AND sbticker.sbtickersymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n_customers
FROM broker.sbcustomer AS sbcustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbcustomer.sbcustid
WHERE
  ENDSWITH(sbcustomer.sbcustemail, '.com') AND NOT _u_0._u_1 IS NULL
