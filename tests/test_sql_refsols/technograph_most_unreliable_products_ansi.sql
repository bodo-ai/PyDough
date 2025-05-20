WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    COUNT() AS agg_1,
    SUM(COALESCE(_s1.agg_0, 0)) AS agg_0,
    devices.de_product_id AS product_id
  FROM main.devices AS devices
  LEFT JOIN _s1 AS _s1
    ON _s1.device_id = devices.de_id
  GROUP BY
    devices.de_product_id
)
SELECT
  products.pr_name AS product,
  products.pr_brand AS product_brand,
  products.pr_type AS product_type,
  ROUND(COALESCE(_t0.agg_0, 0) / _t0.agg_1, 2) AS ir
FROM main.products AS products
JOIN _t0 AS _t0
  ON _t0.product_id = products.pr_id
ORDER BY
  ir DESC
LIMIT 5
