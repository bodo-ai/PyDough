WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxstatus = 'success') AS sum_expr_2,
    sbtxcustid AS sbTxCustId
  FROM main.sbTransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbCustomer.sbcustname AS name,
  (
    100.0 * COALESCE(_s1.sum_expr_2, 0)
  ) / COALESCE(_s1.n_rows, 0) AS success_rate
FROM main.sbCustomer AS sbCustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbTxCustId = sbCustomer.sbcustid
WHERE
  NOT _s1.n_rows IS NULL AND _s1.n_rows >= 5
ORDER BY
  success_rate
