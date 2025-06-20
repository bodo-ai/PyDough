WITH _s7 AS (
  SELECT
    COUNT(*) AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    SUM(COALESCE(_s7.agg_0, 0)) AS agg_0,
    COUNT(*) AS agg_1,
    _s1.de_product_id AS product_id
  FROM main.devices AS _s1
  JOIN main.products AS _s2
    ON _s1.de_product_id = _s2.pr_id
  LEFT JOIN _s7 AS _s7
    ON _s1.de_id = _s7.device_id
  GROUP BY
    _s1.de_product_id
)
SELECT
  _s0.pr_name AS product,
  _s0.pr_brand AS product_brand,
  _s0.pr_type AS product_type,
  ROUND(COALESCE(_t0.agg_0, 0) / _t0.agg_1, 2) AS ir
FROM main.products AS _s0
JOIN _t0 AS _t0
  ON _s0.pr_id = _t0.product_id
ORDER BY
  ir DESC
LIMIT 5
