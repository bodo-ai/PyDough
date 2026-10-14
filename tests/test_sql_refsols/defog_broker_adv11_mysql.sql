WITH _u_0 AS (
  SELECT
    sbTransaction.sbTxCustId AS _u_1
  FROM broker.sbTransaction AS sbTransaction
  JOIN broker.sbTicker AS sbTicker
    ON sbTicker.sbTickerId = sbTransaction.sbTxTickerId
    AND sbTicker.sbTickerSymbol IN ('AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX')
  GROUP BY
    1
)
SELECT
  COUNT(*) AS n_customers
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _u_0 AS _u_0
  ON _u_0._u_1 = sbCustomer.sbCustId
WHERE
  NOT _u_0._u_1 IS NULL AND sbCustomer.sbCustEmail LIKE '%.com'
