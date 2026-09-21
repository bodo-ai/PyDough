WITH _s1 AS (
  SELECT
    sbTxCustId,
    COUNT(*) AS n_rows
  FROM broker.sbTransaction
  WHERE
    CAST(CAST(sbTxDateTime AS DATETIME) AS DATE) = CAST('2023-04-01' AS DATE)
    AND sbTxType = 'sell'
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustId AS _id,
  sbCustomer.sbCustName AS name,
  COALESCE(_s1.n_rows, 0) AS num_tx
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbCustId
ORDER BY
  3 DESC
LIMIT 1
