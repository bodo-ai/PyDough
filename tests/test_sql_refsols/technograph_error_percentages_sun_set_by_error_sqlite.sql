WITH _s2 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id,
    in_error_id AS error_id
  FROM main.incidents
  GROUP BY
    in_device_id,
    in_error_id
), _s5 AS (
  SELECT
    SUM(_s2.agg_0) AS agg_0,
    _s2.error_id
  FROM _s2 AS _s2
  JOIN main.devices AS devices
    ON _s2.device_id = devices.de_id
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  GROUP BY
    _s2.error_id
), _t0 AS (
  SELECT
    errors.er_name AS error,
    ROUND(
      CAST((
        100.0 * COALESCE(_s5.agg_0, 0)
      ) AS REAL) / SUM(COALESCE(_s5.agg_0, 0)) OVER (),
      2
    ) AS pct
  FROM main.errors AS errors
  LEFT JOIN _s5 AS _s5
    ON _s5.error_id = errors.er_id
)
SELECT
  error,
  pct
FROM _t0
ORDER BY
  pct DESC
