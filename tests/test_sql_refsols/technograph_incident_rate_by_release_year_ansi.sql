WITH _t3 AS (
  SELECT
    de_product_id AS product_id
  FROM main.devices
), _s0 AS (
  SELECT
    COUNT() AS agg_1,
    product_id
  FROM _t3
  GROUP BY
    product_id
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
), _s2 AS (
  SELECT
    COUNT() AS agg_0,
    product_id
  FROM _t3
  GROUP BY
    product_id
), _s5 AS (
  SELECT DISTINCT
    in_device_id AS device_id
  FROM main.incidents
), _s7 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0,
    EXTRACT(YEAR FROM _t7.release_date) AS release_year
  FROM _s2 AS _s2
  JOIN _t4 AS _t7
    ON _s2.product_id = _t7._id
  JOIN _s5 AS _s5
    ON _s5.device_id = _t7._id
  GROUP BY
    EXTRACT(YEAR FROM _t7.release_date)
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.agg_0, 0) / _s6.agg_1, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  year
