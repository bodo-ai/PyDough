WITH _s1 AS (
  SELECT
    sbTxCustId,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustName AS name,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbCustId
ORDER BY
  2 DESC
LIMIT 5
