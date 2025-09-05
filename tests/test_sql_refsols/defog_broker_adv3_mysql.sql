WITH _t1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxstatus = 'success') AS sum_expr,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    3
)
SELECT
  sbCustomer.sbcustname AS name,
  (
    100.0 * COALESCE(_t1.sum_expr, 0)
  ) / _t1.n_rows AS success_rate
FROM main.sbCustomer AS sbCustomer
JOIN _t1 AS _t1
  ON _t1.n_rows >= 5 AND _t1.sbTxCustId = sbCustomer.sbcustid
ORDER BY
  2
