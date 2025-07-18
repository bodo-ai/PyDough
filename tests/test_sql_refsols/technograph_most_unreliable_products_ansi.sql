WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.incidents
  GROUP BY
    in_device_id
), _s5 AS (
  SELECT
    ROUND(COALESCE(SUM(COALESCE(_s3.n_rows, 0)), 0) / COUNT(*), 2) AS ir,
    devices.de_product_id
  FROM main.devices AS devices
  JOIN main.products AS products
    ON devices.de_product_id = products.pr_id
  LEFT JOIN _s3 AS _s3
    ON _s3.in_device_id = devices.de_id
  GROUP BY
    devices.de_product_id
)
SELECT
  products.pr_name AS product,
  products.pr_brand AS product_brand,
  products.pr_type AS product_type,
  _s5.ir
FROM main.products AS products
JOIN _s5 AS _s5
  ON _s5.de_product_id = products.pr_id
ORDER BY
  ir DESC
LIMIT 5
