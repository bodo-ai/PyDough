WITH _s3 AS (
  SELECT
    COUNT(*) AS n_rows,
    in_device_id
  FROM main.INCIDENTS
  GROUP BY
    in_device_id
)
SELECT
  PRODUCTS.pr_brand AS brand,
  ROUND(COALESCE(SUM(_s3.n_rows), 0) / COUNT(*), 2) AS ir
FROM main.DEVICES AS DEVICES
JOIN main.PRODUCTS AS PRODUCTS
  ON DEVICES.de_product_id = PRODUCTS.pr_id
LEFT JOIN _s3 AS _s3
  ON DEVICES.de_id = _s3.in_device_id
GROUP BY
  1
ORDER BY
  PRODUCTS.pr_brand COLLATE utf8mb4_bin
