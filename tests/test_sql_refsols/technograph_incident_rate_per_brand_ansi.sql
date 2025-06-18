WITH _s3 AS (
  SELECT
    COUNT(*) AS agg_0,
    in_device_id AS device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _t0 AS (
  SELECT
    SUM(COALESCE(_s3.agg_0, 0)) AS agg_0,
    COUNT(*) AS agg_1,
    products.pr_brand AS brand
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s3 AS _s3
    ON _s3.device_id = devices.de_id
  GROUP BY
    products.pr_brand
)
SELECT
  brand,
  ROUND(COALESCE(agg_0, 0) / agg_1, 2) AS ir
FROM _t0
ORDER BY
  brand
