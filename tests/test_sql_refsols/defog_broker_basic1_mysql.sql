WITH _s1 AS (
  SELECT
    sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  WHERE
    sbTxDateTime >= CAST(DATE_SUB(CURRENT_TIMESTAMP(), INTERVAL '30' DAY) AS DATE)
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustCountry AS country,
  COALESCE(SUM(_s1.n_rows), 0) AS num_transactions,
  COALESCE(SUM(_s1.sum_sbTxAmount), 0) AS total_amount
FROM broker.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbCustId
GROUP BY
  1
