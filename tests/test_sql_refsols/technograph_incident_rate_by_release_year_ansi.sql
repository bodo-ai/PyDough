WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    de_product_id
  FROM main.devices
  GROUP BY
    2
), _s1 AS (
  SELECT
    pr_id,
    pr_release
  FROM main.products
), _s6 AS (
  SELECT
    SUM(_s0.n_rows) AS n_rows,
    EXTRACT(YEAR FROM CAST(_s1.pr_release AS DATETIME)) AS release_year
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.de_product_id = _s1.pr_id
  GROUP BY
    2
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    EXTRACT(YEAR FROM CAST(_s3.pr_release AS DATETIME)) AS release_year
  FROM main.devices AS devices
  JOIN _s1 AS _s3
    ON _s3.pr_id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    2
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.n_rows, 0) / _s6.n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  1
