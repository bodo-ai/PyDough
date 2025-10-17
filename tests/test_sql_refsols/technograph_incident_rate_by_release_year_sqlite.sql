WITH _s0 AS (
  SELECT
    de_product_id,
    COUNT(*) AS n_rows
  FROM main.devices
  GROUP BY
    1
), _s1 AS (
  SELECT
    pr_id,
    pr_release
  FROM main.products
), _s6 AS (
  SELECT
    CAST(STRFTIME('%Y', _s1.pr_release) AS INTEGER) AS year_pr_release,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.de_product_id = _s1.pr_id
  GROUP BY
    1
), _s7 AS (
  SELECT
    CAST(STRFTIME('%Y', _s3.pr_release) AS INTEGER) AS year_pr_release,
    COUNT(*) AS n_rows
  FROM main.devices AS devices
  JOIN _s1 AS _s3
    ON _s3.pr_id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    1
)
SELECT
  _s6.year_pr_release AS year,
  ROUND(CAST(COALESCE(_s7.n_rows, 0) AS REAL) / _s6.sum_n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year_pr_release = _s7.year_pr_release
ORDER BY
  1
