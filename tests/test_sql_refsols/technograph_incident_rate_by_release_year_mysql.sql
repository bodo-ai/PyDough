WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    de_product_id
  FROM main.DEVICES
  GROUP BY
    2
), _s6 AS (
  SELECT
    EXTRACT(YEAR FROM CAST(release_date AS DATETIME)) AS release_year,
    SUM(_s0.n_rows) AS sum_n_rows
  FROM _s0 AS _s0
  JOIN main.PRODUCTS AS PRODUCTS
    ON PRODUCTS.pr_id = _s0.de_product_id
  GROUP BY
    1
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    EXTRACT(YEAR FROM CAST(PRODUCTS.pr_release AS DATETIME)) AS release_year
  FROM main.DEVICES AS DEVICES
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id
  JOIN main.INCIDENTS AS INCIDENTS
    ON DEVICES.de_id = INCIDENTS.in_device_id
  GROUP BY
    2
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.n_rows, 0) / _s6.sum_n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  1
