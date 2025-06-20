WITH _s9 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s1.in_error_id AS error_id
  FROM main.incidents AS _s1
  JOIN main.devices AS _s2
    ON _s1.in_device_id = _s2.de_id
  JOIN main.products AS _s3
    ON _s2.de_product_id = _s3.pr_id AND _s3.pr_name = 'Sun-Set'
  GROUP BY
    _s1.in_error_id
), _t0 AS (
  SELECT
    _s0.er_name AS error,
    ROUND((
      100.0 * COALESCE(_s9.agg_0, 0)
    ) / SUM(COALESCE(_s9.agg_0, 0)) OVER (), 2) AS pct
  FROM main.errors AS _s0
  LEFT JOIN _s9 AS _s9
    ON _s0.er_id = _s9.error_id
)
SELECT
  error,
  pct
FROM _t0
ORDER BY
  pct DESC
