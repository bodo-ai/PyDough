WITH _t0 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', _t0.release_date) AS INTEGER) AS year
  FROM main.devices AS devices
  JOIN _t0 AS _t0
    ON _t0._id = devices.de_product_id
), _s5 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t1 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(COALESCE(_s5.agg_0, 0)) AS agg_0,
    CAST(STRFTIME('%Y', _s3.release_date) AS INTEGER) AS year_13
  FROM main.devices AS devices
  JOIN _t0 AS _s3
    ON _s3._id = devices.de_product_id
  LEFT JOIN _s5 AS _s5
    ON _s5.device_id = devices.de_id
  GROUP BY
    CAST(STRFTIME('%Y', _s3.release_date) AS INTEGER)
)
SELECT
  _s6.year,
  ROUND(CAST(COALESCE(_t1.agg_0, 0) AS REAL) / _t1.agg_1, 2) AS ir
FROM _s6 AS _s6
JOIN _t1 AS _t1
  ON _s6.year = _t1.year_13
ORDER BY
  year
