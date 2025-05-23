WITH _s1 AS (
  SELECT
    COUNT() AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s3 AS (
  SELECT
    SUM(COALESCE(_s1.agg_0, 0)) AS agg_0,
    COUNT() AS agg_1,
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
  ROUND(COALESCE(_s3.agg_0, 0) / COALESCE(_s3.agg_1, 0), 2) AS ir
FROM main.products AS products
LEFT JOIN _s3 AS _s3
  ON _s3.product_id = products.pr_id
ORDER BY
  ir DESC
LIMIT 5
