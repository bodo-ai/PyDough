WITH _s1 AS (
  SELECT
    EXTRACT(MONTH FROM CAST(sbTxDateTime AS DATETIME)) AS month_sbTxDateTime,
    EXTRACT(YEAR FROM CAST(sbTxDateTime AS DATETIME)) AS year_sbTxDateTime,
    sbTxCustId,
    COUNT(*) AS n_rows
  FROM broker.sbTransaction
  GROUP BY
    1,
    2,
    3
)
SELECT
  sbCustomer.sbCustId AS _id,
  sbCustomer.sbCustName AS name,
  COALESCE(_s1.n_rows, 0) AS num_transactions
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.month_sbTxDateTime = EXTRACT(MONTH FROM CAST(sbCustomer.sbCustJoinDate AS DATETIME))
  AND _s1.sbTxCustId = sbCustomer.sbCustId
  AND _s1.year_sbTxDateTime = EXTRACT(YEAR FROM CAST(sbCustomer.sbCustJoinDate AS DATETIME))
ORDER BY
  3 DESC
LIMIT 1
