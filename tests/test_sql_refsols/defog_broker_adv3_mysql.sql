WITH _t1 AS (
  SELECT
    sbTxCustId,
    COUNT(*) AS n_rows,
    SUM(sbTxStatus = 'success') AS sum_expr
  FROM broker.sbTransaction
  GROUP BY
    1
)
SELECT
  sbCustomer.sbCustName AS name,
  (
    100.0 * COALESCE(_t1.sum_expr, 0)
  ) / _t1.n_rows AS success_rate
FROM broker.sbCustomer AS sbCustomer
JOIN _t1 AS _t1
  ON _t1.n_rows >= 5 AND _t1.sbTxCustId = sbCustomer.sbCustId
ORDER BY
  2
