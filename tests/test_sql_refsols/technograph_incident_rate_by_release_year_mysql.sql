WITH _s0 AS (
  SELECT
    de_product_id,
    COUNT(*) AS n_rows
  FROM main.DEVICES
  GROUP BY
    1
), _s1 AS (
  SELECT
    pr_id,
    pr_release
  FROM main.PRODUCTS
), _s6 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s1.pr_release AS DATETIME)) AS year_pr_release,
    SUM(_s0.n_rows * 1) AS n_rows
  FROM _s0 AS _s0
  JOIN _s1 AS _s1
    ON _s0.de_product_id = _s1.pr_id
  GROUP BY
    1
), _s7 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(_s3.pr_release AS DATETIME)) AS year_pr_release,
    COUNT(*) AS n_rows
  FROM main.DEVICES AS DEVICES
  JOIN _s1 AS _s3
    ON DEVICES.de_product_id = _s3.pr_id
  JOIN main.INCIDENTS AS INCIDENTS
    ON DEVICES.de_id = INCIDENTS.in_device_id
  GROUP BY
    1
)
SELECT
  _s6.year_pr_release AS year,
  ROUND(COALESCE(_s7.n_rows, 0) / _s6.n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year_pr_release = _s7.year_pr_release
ORDER BY
  1
