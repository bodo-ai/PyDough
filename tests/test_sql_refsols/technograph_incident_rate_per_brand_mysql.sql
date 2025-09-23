WITH _s3 AS (
  SELECT
    in_device_id,
    COUNT(*) AS n_rows
  FROM main.INCIDENTS
  GROUP BY
    1
)
SELECT
  PRODUCTS.pr_brand COLLATE utf8mb4_bin AS brand,
  ROUND(SUM(_s3.n_rows) / COUNT(*), 2) AS ir
FROM main.DEVICES AS DEVICES
JOIN main.PRODUCTS AS PRODUCTS
  ON DEVICES.de_product_id = PRODUCTS.pr_id
JOIN _s3 AS _s3
  ON DEVICES.de_id = _s3.in_device_id
GROUP BY
  1
ORDER BY
  1
