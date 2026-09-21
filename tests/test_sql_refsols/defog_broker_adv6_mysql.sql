WITH _s1 AS (
  SELECT
    sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbTxAmount) AS sum_sbTxAmount
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustName AS name,
  _s1.n_rows AS num_tx,
  COALESCE(_s1.sum_sbTxAmount, 0) AS total_amount,
  RANK() OVER (
    ORDER BY CASE WHEN COALESCE(_s1.sum_sbTxAmount, 0) IS NULL THEN 1 ELSE 0 END DESC, COALESCE(_s1.sum_sbTxAmount, 0) DESC
  ) AS cust_rank
FROM broker.sbCustomer AS sbCustomer
JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbCustId
