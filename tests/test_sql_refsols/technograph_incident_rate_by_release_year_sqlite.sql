WITH _s1 AS (
  SELECT
    pr_id,
    pr_release
  FROM main.products
), _t1 AS (
  SELECT
    COUNT(*) AS n_rows_1,
    MAX(_s1.pr_release) AS pr_release
  FROM main.devices AS devices
  JOIN _s1 AS _s1
    ON _s1.pr_id = devices.de_product_id
  GROUP BY
    devices.de_product_id
), _s6 AS (
  SELECT
    CAST(STRFTIME('%Y', pr_release) AS INTEGER) AS release_year,
    SUM(n_rows_1) AS sum_n_rows
  FROM _t1
  GROUP BY
    CAST(STRFTIME('%Y', pr_release) AS INTEGER)
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    CAST(STRFTIME('%Y', _s3.pr_release) AS INTEGER) AS release_year
  FROM main.devices AS devices
  JOIN _s1 AS _s3
    ON _s3.pr_id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    CAST(STRFTIME('%Y', _s3.pr_release) AS INTEGER)
)
SELECT
  _s6.release_year AS year,
  ROUND(CAST(COALESCE(_s7.n_rows, 0) AS REAL) / _s6.sum_n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  _s6.release_year
