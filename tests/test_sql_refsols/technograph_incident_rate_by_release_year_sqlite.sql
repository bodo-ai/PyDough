WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    de_product_id
  FROM main.devices
  GROUP BY
    2
), _s6 AS (
  SELECT
    CAST(STRFTIME('%Y', release_date) AS INTEGER) AS release_year,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN main.products AS products
    ON _s0.de_product_id = products.pr_id
  GROUP BY
    1
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    CAST(STRFTIME('%Y', products.pr_release) AS INTEGER) AS release_year
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    2
)
SELECT
  _s6.release_year AS year,
  ROUND(CAST(COALESCE(_s7.n_rows, 0) AS REAL) / _s6.sum_n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  1
