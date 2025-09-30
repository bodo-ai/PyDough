WITH _s1 AS (
  SELECT
    sbtxcustid AS sbTxCustId,
    SUM(sbtxamount) AS sum_sbTxAmount
  FROM main.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  2 DESC
LIMIT 5
