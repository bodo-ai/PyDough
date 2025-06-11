WITH _s5 AS (
  SELECT
    COUNT() AS agg_0,
    incidents.in_error_id AS error_id
  FROM main.incidents AS incidents
  JOIN main.devices AS devices
    ON devices.de_id = incidents.in_device_id
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id AND products.pr_name = 'Sun-Set'
  GROUP BY
    incidents.in_error_id
), _t0 AS (
  SELECT
    errors.er_name AS error,
    ROUND((
      100.0 * COALESCE(_s5.agg_0, 0)
    ) / SUM(COALESCE(_s5.agg_0, 0)) OVER (), 2) AS pct
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
