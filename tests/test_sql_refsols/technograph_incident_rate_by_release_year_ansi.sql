WITH _s0 AS (
  SELECT
    COUNT(*) AS agg_1,
    de_product_id AS product_id
  FROM main.devices
  GROUP BY
    de_product_id
), _t4 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT
    SUM(_s0.agg_1) AS agg_1,
    EXTRACT(YEAR FROM _t4.release_date) AS release_year
  FROM _s0 AS _s0
  JOIN _t4 AS _t4
    ON _s0.product_id = _t4._id
  GROUP BY
    EXTRACT(YEAR FROM _t4.release_date)
), _s7 AS (
  SELECT
    COUNT(*) AS agg_0,
    EXTRACT(YEAR FROM _t6.release_date) AS release_year
  FROM main.devices AS devices
  JOIN _t4 AS _t6
    ON _t6._id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM _t6.release_date)
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.agg_0, 0) / _s6.agg_1, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  year
