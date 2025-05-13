WITH _t0 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s8 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', _t0.release_date) AS INTEGER) AS year
  FROM main.devices AS devices
  JOIN _t0 AS _t0
    ON _t0._id = devices.de_product_id
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(COALESCE(_s7.agg_0, 0)) AS agg_0,
    CAST(STRFTIME('%Y', _s5.release_date) AS INTEGER) AS year_13
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  JOIN _t0 AS _s5
    ON _s5._id = devices.de_product_id
  LEFT JOIN _s7 AS _s7
    ON _s7.device_id = devices.de_id
  GROUP BY
    CAST(STRFTIME('%Y', _s5.release_date) AS INTEGER)
)
SELECT
  _s8.year,
  ROUND(CAST(COALESCE(_t1.agg_0, 0) AS REAL) / _t1.agg_1, 2) AS ir
FROM _s8 AS _s8
JOIN _t1 AS _t1
  ON _s8.year = _t1.year_13
ORDER BY
  year
