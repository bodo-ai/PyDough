WITH _t0 AS (
  SELECT
    sbcustcountry AS country,
    COUNT(*) AS n_rows,
    SUM(sbcuststatus = 'active') AS sum_expr_2
  FROM main.sbcustomer
  WHERE
    sbcustjoindate <= '2022-12-31' AND sbcustjoindate >= '2022-01-01'
  GROUP BY
    sbcustcountry
)
SELECT
  country,
  100 * COALESCE(COALESCE(sum_expr_2, 0) / n_rows, 0.0) AS ar
FROM _t0
