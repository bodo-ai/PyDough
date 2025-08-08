WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(CASE WHEN sbtxstatus = 'success' THEN 1 ELSE 0 END) AS sum_expr_2,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  (
    100.0 * COALESCE(_s1.sum_expr_2, 0)
  ) / COALESCE(_s1.n_rows, 0) AS success_rate
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
WHERE
  NOT _s1.n_rows IS NULL AND _s1.n_rows >= 5
ORDER BY
  (
    100.0 * COALESCE(_s1.sum_expr_2, 0)
  ) / COALESCE(_s1.n_rows, 0) NULLS FIRST
