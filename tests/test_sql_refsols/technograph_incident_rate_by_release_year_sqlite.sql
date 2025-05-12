WITH _s1 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT DISTINCT
    CAST(STRFTIME('%Y', _s1.release_date) AS INTEGER) AS year
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1._id = devices.de_product_id
), _s5 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s7 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(COALESCE(_s5.agg_0, 0)) AS agg_0,
    CAST(STRFTIME('%Y', _s3.release_date) AS INTEGER) AS year_13
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s3
    ON _s3._id = devices.de_product_id
  LEFT JOIN _s5 AS _s5
    ON _s5.device_id = devices.de_id
  GROUP BY
    CAST(STRFTIME('%Y', _s3.release_date) AS INTEGER)
)
SELECT
  _s6.year,
  ROUND(CAST(COALESCE(_s7.agg_0, 0) AS REAL) / COALESCE(_s7.agg_1, 0), 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year = _s7.year_13
ORDER BY
  year
