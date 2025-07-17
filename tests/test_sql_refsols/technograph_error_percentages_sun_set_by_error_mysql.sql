WITH _s5 AS (
  SELECT
    COUNT(*) AS n_rows,
    INCIDENTS.in_error_id
  FROM main.INCIDENTS AS INCIDENTS
  JOIN main.DEVICES AS DEVICES
    ON DEVICES.de_id = INCIDENTS.in_device_id
  JOIN main.PRODUCTS AS PRODUCTS
    ON DEVICES.de_product_id = PRODUCTS.pr_id AND PRODUCTS.pr_name = 'Sun-Set'
  GROUP BY
    INCIDENTS.in_error_id
), _t0 AS (
  SELECT
    ROUND((
      100.0 * COALESCE(_s5.n_rows, 0)
    ) / SUM(COALESCE(_s5.n_rows, 0)) OVER (), 2) AS pct,
    ERRORS.er_name
  FROM main.ERRORS AS ERRORS
  LEFT JOIN _s5 AS _s5
    ON ERRORS.er_id = _s5.in_error_id
)
SELECT
  er_name AS error,
  pct
FROM _t0
ORDER BY
  pct DESC
