WITH _s0 AS (
  SELECT
    comid,
    dos,
    COUNT(*) AS n_rows
  FROM shpmnts
  WHERE
    dos <= CAST('2025-07-04' AS DATE) AND dos >= CAST('2025-07-01' AS DATE)
  GROUP BY
    1,
    2
), _t0 AS (
  SELECT
    _s0.dos,
    supls.supname,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN supls AS supls
    ON _s0.comid = supls.supid
  GROUP BY
    1,
    2
)
SELECT
  supname AS company_name,
  dos AS day,
  sum_n_rows AS n_orders,
  sum_n_rows - LAG(sum_n_rows, 1) OVER (PARTITION BY supname ORDER BY dos) AS delta
FROM _t0
ORDER BY
  1 NULLS FIRST,
  2 NULLS FIRST
