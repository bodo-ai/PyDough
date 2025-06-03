WITH _t2 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _t2.release_date) AS year
  FROM main.devices AS devices
  JOIN _t2 AS _t2
    ON _t2._id = devices.de_product_id
  GROUP BY
    EXTRACT(YEAR FROM _t2.release_date)
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM _t4.release_date) AS year
  FROM main.devices AS devices
  JOIN _t2 AS _t4
    ON _t4._id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM _t4.release_date)
)
SELECT
  _s6.year,
  ROUND(COALESCE(_s7.agg_0, 0) / _s6.agg_1, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.year = _s7.year
ORDER BY
  year
