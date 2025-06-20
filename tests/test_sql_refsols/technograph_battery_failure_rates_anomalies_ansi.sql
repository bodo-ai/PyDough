WITH _s12 AS (
  SELECT
    COUNT(*) AS agg_0,
    _s7.in_device_id AS device_id
  FROM main.incidents AS _s7
  JOIN main.errors AS _s8
    ON _s7.in_error_id = _s8.er_id AND _s8.er_name = 'Battery Failure'
  GROUP BY
    _s7.in_device_id
), _t1 AS (
  SELECT
    SUM(COALESCE(_s12.agg_0, 0)) AS agg_0,
    COUNT(*) AS agg_1,
    _s0.co_name AS country_name,
    _s4.pr_name AS product_name
  FROM main.countries AS _s0
  JOIN main.devices AS _s1
    ON _s0.co_id = _s1.de_production_country_id
  JOIN main.products AS _s4
    ON _s1.de_product_id = _s4.pr_id
  LEFT JOIN _s12 AS _s12
    ON _s1.de_id = _s12.device_id
  GROUP BY
    _s0.co_name,
    _s4.pr_name
)
SELECT
  country_name,
  product_name,
  ROUND(COALESCE(agg_0, 0) / agg_1, 2) AS ir
FROM _t1
ORDER BY
  ir DESC,
  product_name,
  country_name
LIMIT 5
