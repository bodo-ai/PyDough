WITH _s1 AS (
  SELECT
    SUM(sbtxamount) AS sum_sbTxAmount,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbCustomer.sbcustname AS name,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  total_amount DESC
LIMIT 5
