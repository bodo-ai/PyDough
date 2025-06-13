WITH _t3 AS (
  SELECT
    pr_id AS _id,
    pr_release AS release_date
  FROM main.products
), _s6 AS (
  SELECT
    COUNT() AS agg_1,
    EXTRACT(YEAR FROM _t3.release_date) AS release_year
  FROM main.devices AS devices
  JOIN _t3 AS _t3
    ON _t3._id = devices.de_product_id
  GROUP BY
    EXTRACT(YEAR FROM _t3.release_date)
), _s7 AS (
  SELECT
    COUNT() AS agg_0,
    EXTRACT(YEAR FROM _t5.release_date) AS release_year
  FROM main.devices AS devices
  JOIN _t3 AS _t5
    ON _t5._id = devices.de_product_id
  JOIN main.incidents AS incidents
    ON devices.de_id = incidents.in_device_id
  GROUP BY
    EXTRACT(YEAR FROM _t5.release_date)
)
SELECT
  _s6.release_year AS year,
  ROUND(COALESCE(_s7.agg_0, 0) / _s6.agg_1, 2) AS ir
FROM _s6 AS _s6
LEFT JOIN _s7 AS _s7
  ON _s6.release_year = _s7.release_year
ORDER BY
  year
