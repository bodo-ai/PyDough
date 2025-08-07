WITH _t1 AS (
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
    100.0 * COALESCE(_t1.sum_expr_2, 0)
  ) AS REAL) / _t1.n_rows AS success_rate
FROM main.sbcustomer AS sbcustomer
JOIN _t1 AS _t1
  ON _t1.n_rows >= 5 AND _t1.sbtxcustid = sbcustomer.sbcustid
ORDER BY
  CAST((
    100.0 * COALESCE(_t1.sum_expr_2, 0)
  ) AS REAL) / _t1.n_rows
