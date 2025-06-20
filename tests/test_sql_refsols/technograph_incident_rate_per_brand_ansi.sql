WITH _s6 AS (
  SELECT
    COUNT(*) AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    SUM(COALESCE(_s6.agg_0, 0)) AS agg_0,
    COUNT(*) AS agg_1,
    _s1.pr_brand AS brand
  FROM main.devices AS _s0
  JOIN main.products AS _s1
    ON _s0.de_product_id = _s1.pr_id
  LEFT JOIN _s6 AS _s6
    ON _s0.de_id = _s6.device_id
  GROUP BY
    _s1.pr_brand
)
SELECT
  brand,
  ROUND(COALESCE(agg_0, 0) / agg_1, 2) AS ir
FROM _t0
ORDER BY
  brand
