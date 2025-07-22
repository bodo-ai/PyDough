WITH _s1 AS (
  SELECT
    COUNT(*) AS n_rows,
    SUM(sbtxstatus = 'success') AS sum_expr_2,
    sbtxcustid
  FROM main.sbtransaction
  GROUP BY
    sbtxcustid
)
SELECT
  sbcustomer.sbcustname AS name,
  CAST((
    100.0 * COALESCE(_s1.sum_expr_2, 0)
  ) AS REAL) / COALESCE(_s1.n_rows, 0) AS success_rate
FROM main.sbcustomer AS sbcustomer
LEFT JOIN _s1 AS _s1
  ON _s1.sbtxcustid = sbcustomer.sbcustid
WHERE
  NOT _s1.n_rows IS NULL AND _s1.n_rows >= 5
ORDER BY
  CAST((
    100.0 * COALESCE(_s1.sum_expr_2, 0)
  ) AS REAL) / COALESCE(_s1.n_rows, 0)
