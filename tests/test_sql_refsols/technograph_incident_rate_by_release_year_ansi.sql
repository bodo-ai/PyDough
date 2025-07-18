WITH _s0 AS (
  SELECT
    COUNT(*) AS n_rows,
    de_product_id
  FROM main.devices
  GROUP BY
    de_product_id
), _t4 AS (
  SELECT
    pr_id,
    pr_release
  FROM main.products
), _s6 AS (
  SELECT
    SUM(_s0.n_rows) AS sum_n_rows,
    EXTRACT(YEAR FROM CAST(_t4.pr_release AS DATETIME)) AS release_year
  FROM _s0 AS _s0
  JOIN _t4 AS _t4
    ON _s0.de_product_id = _t4.pr_id
  GROUP BY
    EXTRACT(YEAR FROM CAST(_t4.pr_release AS DATETIME))
), _s7 AS (
  SELECT
    COUNT(*) AS n_rows,
    EXTRACT(YEAR FROM CAST(_t6.pr_release AS DATETIME)) AS release_year
  FROM main.devices AS devices
  JOIN _t4 AS _t6
    ON _t6.pr_id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM CAST(_t6.pr_release AS DATETIME))
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.n_rows, 0) / _s6.sum_n_rows, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  _s6.release_year
