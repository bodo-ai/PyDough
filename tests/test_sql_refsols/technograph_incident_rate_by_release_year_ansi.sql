WITH _s0 AS (
  SELECT
    de_product_id AS product_id
  FROM main.devices
), _s1 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT DISTINCT
    EXTRACT(YEAR FROM _s1.release_date) AS year
  FROM _s0 AS _s0
  LEFT JOIN _s1 AS _s1
    ON _s0.product_id = _s1._id
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM _s3.release_date) AS year_10
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s3
    ON _s3._id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM _s3.release_date)
), _s11 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _s9.release_date) AS year_19
  FROM _s0 AS _s8
  LEFT JOIN _s1 AS _s9
    ON _s8.product_id = _s9._id
  GROUP BY
    EXTRACT(YEAR FROM _s9.release_date)
)
SELECT
  _s6.year,
  ROUND(COALESCE(_s7.agg_0, 0) / COALESCE(_s11.agg_1, 0), 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year = _s7.year_10
LEFT JOIN _s11 AS _s11
  ON _s11.year_19 = _s6.year
ORDER BY
  year
